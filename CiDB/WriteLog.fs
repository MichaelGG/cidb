namespace CiDB

(*
Write ahead storage for 64-bit keys and 64-bit values.

Inserts are done in value-order. Log files cannot be larger than 2GB (at the moment).
Format is 128-bits of zero, 64-bit value, 64-bit record count, followed by that many 64-bit keys.
Zero bits are markers, as that many would never appear in a legitimate file. Can be used to detect/repair corruption.

Open logs are available for searching (linear through the value chunks, binary search on the keys for a specific value).

When 128MB limit is reached, log is written to indexed format and a new log is opened. 
*)

#nowarn "9"

open System
open System.IO
open Microsoft.FSharp.NativeInterop

type private DV = DefaultValueAttribute
module NP = NativePtr

/// Represents an open transaction log
/// Writes and flushes are not threadsafe! (Searches are.)
type WriteLog(filename, size) =
    static let log = log4net.LogManager.GetLogger("CiDB.WriteLog")

    static let recoverLog (p:nativeptr<uint64>) fileSize =
        let maxcount = fileSize / 8
        let read offset = NP.get p offset
        let rec seekEnd offset = 
            if (read offset, read (offset + 1)) <> (0UL, 0UL) then
                failwith "Corruption detected (missing header) in bulk log file at position 0x%x." (8 * offset)
            else
            let value = read (offset + 2)
            if value = 0UL then offset else
            let count = read (offset + 3) |> Checked.int32
            let nextOffset = offset + 4 + count
            // Just in case the last item is damaged somehow, and points beyond the file
            if (nextOffset + 4) > maxcount then offset else seekEnd nextOffset
        seekEnd 0    

    let exists = IO.File.Exists(filename)
    let filesize = if exists then IO.FileInfo(filename).Length |> Checked.int32 else size
    do IO.Directory.CreateDirectory(IO.Path.GetDirectoryName filename) |> ignore

    let file = MemoryMappedFiles.MemoryMappedFile.CreateFromFile(filename, FileMode.OpenOrCreate, IO.Path.GetFileName(filename), int64 filesize)
    let view = file.CreateViewAccessor()
    let ptr = view.SafeMemoryMappedViewHandle.DangerousGetHandle() |> NP.ofNativeInt<uint64>
    let mutable nextPos = if exists then recoverLog ptr filesize |> int else 0

    do if exists then log.InfoFormat(sprintf "Log file '%s' recovered to position 0x%x." filename (nextPos * 8))

    let mutable lastFlushPos = nextPos
    let mutable lastFlushDate = DateTime.UtcNow

    let mutable disposed = false

    /// Object to lock just for Write/Flush. Used to make it easier to periodically call "FlushIfNeeded".
    let updateLock = Object() 

    let iterLog f = 
        // Offset is still a 32-bit value, as arrays in .NET can't be more than that anyways.
        // Corrupting checking is less useful here, as we won't _write_ bad data, 
        // and recoverLog will have made sure each record count lines up properly.
        let total = lock updateLock (fun () -> nextPos - 4)
        let mutable pos = 0
        let mutable lastValue = 0UL
        let mutable minValue = UInt64.MaxValue
        let mutable maxValue = UInt64.MinValue
        while pos < total do
            lastValue <- NP.get ptr (2 + pos) 
            minValue <- min minValue lastValue 
            maxValue <- max maxValue lastValue
            let maxCount = (total - pos)
            let rcount = NP.get ptr (pos + 3) |> int
            let rcount = min rcount maxCount
            for x = 1 to rcount do
                let key = NP.get ptr (pos + 3 + x)
                f key lastValue 
            pos <- pos + 4 + rcount
        minValue, maxValue

    member x.NextPosition = lock updateLock <| fun () -> nextPos
    member x.Pointer = ptr
    member x.FileName = filename

    member private x.flushIfNeeded() =
        // How many items are we willing to lose? 1 page is 1024 items, so larger is faster, but less safe
        // 64K chunks are probably most efficient, so don't flush until we have 16 pages
        lock updateLock <| fun () ->
        let safetyCount = 15 * 1024 + 1
        let safetyTime = TimeSpan.FromSeconds(5.) // 5 is a long enough delay, if tx rate is slow
        if (lastFlushPos < nextPos - safetyCount) || 
            (DateTime.UtcNow - lastFlushDate > safetyTime && lastFlushPos <> nextPos) then 
            view.Flush()
            lastFlushPos <- nextPos

    member x.WriteRecords (value:uint64) (keys:ResizeArray<uint64>) =
        lock updateLock <| fun () ->
        if keys.Count + 5 + nextPos > (filesize / 8) then invalidArg "keys" "Number of keys would exceed file size."
        if keys.Count = 0 then () else
        // Keys come in unsorted with duplicates
        keys.Sort()
        let p = NP.add ptr nextPos
        // Two zeros is header. We overwrite because it's possible that a log got really corrupted
        // where a later page was written before an earlier page. So the end of records could
        // be detected by recovery, but 4K+ down the line, there's more data.
        // Since that data is unrecoverable, we're just going to overwrite it.
        if NP.read p <> 0UL then
            log.Fatal(sprintf "Log corruption detected (missing header). File: '%s' Position: 0x%x." filename (nextPos * 8))
        else
        NP.set p 0 0UL 
        NP.set p 1 0UL
        let p = NP.add p 2
        let mutable off = 2 // Skip the first two ints, as they are the value and the record count
        NP.set p off keys.[0]
        for i = 1 to keys.Count - 1 do
            if keys.[i] <> keys.[i - 1] then
                off <- off + 1
                NP.set p off keys.[i]
        let count = uint64(off - 1)
        NP.set p 1 count
        NP.set p 0 value 
        nextPos <- nextPos + (off + 3) // Offset points to last key, so +1 to get past that, and +2 to get past the zeros
        x.flushIfNeeded()

    /// Returns true if there's less than 8MB left
    member x.IsFull = (nextPos * 8) > (filesize - (8 * 1024 * 1024))

    /// Converts a transaction log into a full array of sorted 64-bit key-values.
    member x.ExpandLog() = 
        // Use resize array, as this makes it easier to sort and deal with results
        // Avoids reallocation, since we won't fill the estimated size
        if nextPos = 0 then None else
        let arr = ResizeArray<_>(nextPos) // Very generous, to avoid re-allocation
        let minVal, maxVal = iterLog(fun key value -> arr.Add(UInt64Pair(key, value)))
        arr.Sort()
        Some (arr, minVal, maxVal)

    // Wow, this was easy
    // Yea I should probably redesign it to enumerate by value, so I can binarysearch, but oh well
    member x.Search (startKey: uint64) (endKey: uint64) = 
        let res = ResizeArray<_>()
        iterLog (fun key value -> 
                    if key >= startKey && key <= endKey && (res.Count = 0 || res.[res.Count - 1] <> value) then res.Add value) |> ignore
        res

    member x.Dispose() =
        if disposed then () else
        disposed <- true
        view.Flush()
        view.Dispose()
        file.Dispose()

    interface IDisposable with
        member x.Dispose() = x.Dispose()
