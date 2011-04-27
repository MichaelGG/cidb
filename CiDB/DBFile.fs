namespace CiDB

#nowarn "9"

open System
open CiDB
open Microsoft.FSharp.NativeInterop

module NP = NativePtr

(*
CiDB file format is a header, followed by index, followed by page data.
CiDB file can be embedded into any stream.

Header (56 bytes):
    Cookie  "CiDB0001"B 3544385890568923459UL
    Flags           : uint64 // Reserved
    RecordCount     : int64
    PageSize        : int64 
    GlobalMinValue  : uint64
    PageDataStart   : int64 // This is the relative offset from the start of the header
    PageDataLength  : int64 // Bytes in page data
*)  

module DBFile = 
    // WARNING: This returns the same array, so be sure 
    // code that uses it never uses more than 1 result at a time
    let private valuesToArrays pageSize (values: UInt64Pair seq) = 
        if pageSize < 1 then invalidArg "pageSize" "Page size must be greater than zero."
        let arr = Array.zeroCreate pageSize
        match values with
        | :? ResizeArray<UInt64Pair> as values ->
            seq {
                // Silly imperative loop to avoid F#'s unoptimized sequence stuff
                let i = ref -pageSize
                while !i < (values.Count - pageSize) do 
                    let ix = !i + pageSize
                    i := ix
                    let arrSize = min pageSize (values.Count - ix)
                    let arr = if arrSize = arr.Length then arr else Array.zeroCreate arrSize
                    values.CopyTo(ix, arr, 0, arrSize)
                    yield arr
            }
        | _ -> 
            seq {
            use e = values.GetEnumerator()
            let more = ref true
            while !more do
                let i = ref 0
                while !i < arr.Length && e.MoveNext() do
                    arr.[!i] <- e.Current
                    incr i
                let arr = if arr.Length = !i then arr else
                          more := false
                          let arr' = Array.zeroCreate !i
                          Array.Copy(arr, arr', !i)
                          arr'
                if arr.Length <> 0 then yield arr
            }
    
    /// Writes a complete CiDB file to stream. (Stream, pageSize, minValue, values, valuesCount)
    let writeFile (s: IO.Stream) pageSize (minValue: uint64) (values: UInt64Pair seq) (valuesCount: int64) =
        if pageSize < 1 || pageSize > 65535 then invalidArg "pageSize" "Page size must be between 1 and 65535."
        let values = valuesToArrays pageSize values
        let indexSize = Index.calcIndexSize valuesCount pageSize
        let bw = new IO.BinaryWriter(s)

        // Write header
        let fileStartPos = s.Position
        let pageDataStartPos = s.Position + 56L + int64 indexSize
        let pageDataStartPos = ((pageDataStartPos / 16L) + 1L) * 16L
        bw.Write 3544385890568923459UL
        bw.Write 0UL // Flags
        bw.Write valuesCount
        bw.Write (int64 pageSize)
        bw.Write minValue
        bw.Write (pageDataStartPos - fileStartPos) // Relative start pos

        let indexPos = s.Position
        s.Seek(pageDataStartPos, IO.SeekOrigin.Begin) |> ignore
        let pageInfo = Page.writeValuesAsPages s values minValue valuesCount pageSize pageDataStartPos
        let dataLen = s.Position - pageDataStartPos
        let eof = s.Position
        s.Seek(indexPos, IO.SeekOrigin.Begin) |> ignore
        bw.Write dataLen // indexPos is 8 bytes short, to allow dataLen to be written
        Index.writeIndex s pageInfo
        s.Seek(eof, IO.SeekOrigin.Begin) |> ignore // After writing, position should be at end

type DBStreamFile(s: IO.Stream) =
    let br = new IO.BinaryReader(s)
    let fileStartPos = s.Position
    do match br.ReadUInt64() with 
        | 3544385890568923459UL -> ()
        | x                     -> failwithf "DB cookie not found. Read: 0x%x at position 0x%x." x fileStartPos
    do match br.ReadUInt64() with
        | 0UL   -> ()
        | x     -> failwithf "Unknown flags value: 0x%x." x
    let valuesCount = br.ReadInt64()
    let pageSize    = br.ReadInt64()
    let minValue    = br.ReadUInt64()
    let pageDataOff = br.ReadInt64()
    let dataLen     = br.ReadInt64()

    let pageDataAbs = pageDataOff + fileStartPos
    let indexAbs    = s.Position

    let slock = Object()

    member x.ValuesCount     = valuesCount
    member x.PageSize        = pageSize
    member x.GlobalMinValue  = minValue
    member x.PageDataOffset  = pageDataOff
    member x.DataLength      = dataLen
    member x.TotalSize       = 56L + pageDataOff + dataLen


    member x.Search (xStart, xEnd) = 
        lock slock <| fun () ->
        if s.Position <> indexAbs then s.Seek(indexAbs, IO.SeekOrigin.Begin) |> ignore
        let pageOffs = Index.searchIndexStream s (xStart, xEnd)
        let res = ResizeArray<_>(if pageOffs.Count = 1 then 32 else pageOffs.Count * int pageSize)
        let searchVals = (xStart, xEnd)
        for po in pageOffs do
            let poabs = int64 po + pageDataAbs
            if s.Position <> poabs then s.Seek(poabs, IO.SeekOrigin.Begin) |> ignore
            Page.searchSinglePageStream s minValue searchVals res
        res

type DBMappedFile(filename: string) =
    let filestream = IO.File.Open(filename, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read)
    let file = IO.MemoryMappedFiles.MemoryMappedFile.CreateFromFile(
                    filestream, null, 0L,
                    IO.MemoryMappedFiles.MemoryMappedFileAccess.Read,
                    IO.MemoryMappedFiles.MemoryMappedFileSecurity(),
                    IO.HandleInheritability.Inheritable,
                    false)
    let view = file.CreateViewAccessor(0L, 0L, IO.MemoryMappedFiles.MemoryMappedFileAccess.Read)
    let ptr = view.SafeMemoryMappedViewHandle.DangerousGetHandle()

    let memStream = new IO.UnmanagedMemoryStream(ptr |> NP.ofNativeInt, filestream.Length, filestream.Length, IO.FileAccess.Read)
    let db = DBStreamFile(memStream)
    let pageDataOffset = db.PageDataOffset
    let minValue = db.GlobalMinValue
    let pageSize = db.PageSize

    let rwlock = new Threading.ReaderWriterLockSlim()
    let mutable disposed = false

    member x.Search (xStart, xEnd) =
        rwlock.EnterReadLock()
        try
            if disposed then raise (ObjectDisposedException("DBMappedFile"))
            use ums = new IO.UnmanagedMemoryStream(ptr |> NP.ofNativeInt, filestream.Length, filestream.Length, IO.FileAccess.Read)
            ums.Seek(56L, IO.SeekOrigin.Begin) |> ignore
            let pageOffs = Index.searchIndexStream ums (xStart, xEnd)
            let res = ResizeArray<_>(if pageOffs.Count = 1 then 32 else pageOffs.Count * int pageSize)
            let searchVals = (xStart, xEnd)
            for po in pageOffs do
                let poabs = int64 po + pageDataOffset
                let ptr = NP.ofNativeInt(ptr + nativeint poabs)
                Page.searchSinglePagePtr ptr minValue searchVals res 
            res
        finally
            rwlock.ExitReadLock()

    member x.Dispose() =
        if disposed then () else
        rwlock.EnterWriteLock()
        try
            disposed <- true
            if (memStream <> null) then memStream.Dispose()
            if (view <> null) then view.Dispose()
            if (file <> null) then file.Dispose()
            if (filestream <> null) then filestream.Dispose()
        finally
            rwlock.ExitWriteLock()     
            rwlock.Dispose()   
    
    interface IDisposable with
        member x.Dispose() = x.Dispose()