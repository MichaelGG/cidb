module private CiDB.Page

#nowarn "9"

open System
open Microsoft.FSharp.NativeInterop
open System.Runtime.InteropServices

open Extensions

module NP = NativePtr

(*
Pages are 16-byte aligned.
Page header (16 bytes):
    Cookie "\000\000Page\000\000"B (zero records wouldn't sort like that in data stream)
        - This is 111494688669696UL
    UInt32 itemCount
    UInt32 itemBytes
Items:
    Assumes they are completely sorted
    Base128 Key, Base128 value
    Key is delta encoded from last key (start at zero)
    When key changes, value is delta encoded from minValue (global setting)
    When key is same as last, value is delta encoded from last value
    Since values are sorted, negative values are impossible, so encoding is as efficient as possible.
        Run length encoding complicates things. We'd have to steal a bit from the first key byte to indicate a range.
        Sounds complicated; unsure of benefit.
*)
[<AllowNullLiteral>]
type private WriteBuffer(maxItems) = 
    do if maxItems < 1 then invalidArg "maxItems" "Max items must be positive."
    let arr = Array.zeroCreate<byte> (maxItems * 20 + 128) // Make sure it's large enough to hold 2x10byte ints plus a header
    let parr = PinnedArray.of_array arr

    member x.Array = arr
    member x.Ptr = parr.Ptr

    interface IDisposable with
        member x.Dispose() = parr.Free()

/// Writes a page into a writebuffer, returns length written
let private writePageToBuffer (values: UInt64Pair array) (minValue: uint64) (wbuffer: WriteBuffer) = 
    if wbuffer.Array.Length < (values.Length * 20 + 128) then invalidArg "wbuffer" "WriteBuffer too small." 

    let mutable ptr = NP.addBytes wbuffer.Ptr 16  // Skip past header
    let mutable lastValue = minValue
    let mutable lastKey = 0UL
    for i = 0 to values.Length - 1 do
        let k = values.[i].Key
        let v = values.[i].Value
        if k = lastKey then
            // Just write a zero for the key, and delta encode value from lastValue
            // Must write the zero, because buffer is not clean
            NP.write ptr 0uy
            ptr <- NP.addBytes ptr 1n
            ptr <- Base128.writeB128Fast ptr (v - lastValue)
        else
            // Delta encode key, then encode lastValue from min
            ptr <- Base128.writeB128Fast ptr (k - lastKey)
            ptr <- Base128.writeB128Fast ptr (v - minValue)
            lastKey <- k
        lastValue <- v
        ()

    // Write header
    let itemBytes = NP.toNativeInt ptr - NP.toNativeInt wbuffer.Ptr - 16n |> uint32
    NP.write (NP.cast wbuffer.Ptr) 111494688669696UL
    NP.write (NP.cast (NP.addBytes (wbuffer.Ptr) 8)) (uint32 values.Length)
    NP.write (NP.cast (NP.addBytes (wbuffer.Ptr) 12)) itemBytes
    
    // Bytes written are the item bytes plus the header
    int32 itemBytes + 16

let inline private calcPadding x amount = 
    if x % amount = 0L then 0L else amount - (x % amount)

/// Writes all values to the stream, using async IO. 
/// Returns a memory stream containing 8 byte values and 8 byte positions.
/// Positions returned are relative to posOffset. 
/// s values minValue recordCount pageSize posOffset
let writeValuesAsPages (s: IO.Stream) (values: UInt64Pair array seq) minValue (valuesCount: int64) pageSize posOffset = 
    if pageSize < 1 || pageSize > 16777216 then invalidArg "pageSize" "Page size out of range (1 to 16777216)."
    if (posOffset < 0L) then invalidArg "posOffset" "Position offset must not be negative."
    if valuesCount = 0L then [||] else 
    let pageInfo = Array.zeroCreate ((decimal valuesCount / decimal pageSize |> ceil |> Checked.int) * 16)
    let ms = new IO.MemoryStream(pageInfo)
    let bw = new IO.BinaryWriter(ms)

    // Use two write buffers, so we can pend a write to stream, while computing the next page
    use wbuf1 = new WriteBuffer(pageSize)
    use wbuf2 = new WriteBuffer(pageSize)
    let mutable flip = false

    // Allocate all these here to do manual alloc lifting for the closure
    // TODO: Switch this to a queue to allow more than 2 buffers
    let paddingArray = Array.zeroCreate 32
    use writeFinished  = new Threading.ManualResetEventSlim(true, 10)
    use writeAvailable = new Threading.ManualResetEventSlim(false, 10)
    let currentKey = ref 0UL 
    let currentWBuf = ref wbuf1
    let currentCount = ref 0
    Threading.ThreadPool.UnsafeQueueUserWorkItem((fun _ ->
        let ct = Threading.CancellationToken()
        writeAvailable.Wait(-1, ct) |> ignore
        writeAvailable.Reset()
        while !currentWBuf <> null do
            let padding = calcPadding s.Position (16L) |> int32
            if padding > 0 then
                s.Write(paddingArray, 0, padding)
            bw.Write(!currentKey)
            bw.Write(uint64 (s.Position - posOffset))
            s.Write((!currentWBuf).Array, 0, !currentCount)
            writeFinished.Set() |> ignore
            writeAvailable.Wait(-1, ct) |> ignore
            writeAvailable.Reset()
        writeFinished.Set() 
        ), null) |> ignore
    let ct = Threading.CancellationToken()
    for vals in values do 
        flip <- not flip
        let wbuf = if flip then wbuf1 else wbuf2
        let count = writePageToBuffer vals minValue wbuf
        writeFinished.Wait(-1, ct) |> ignore
        writeFinished.Reset() 
        currentKey := vals.[0].Key // Vals is not threadsafe, so cache key
        currentCount := count
        currentWBuf := wbuf
        writeAvailable.Set() 

    if !currentWBuf <> null then // Only wait if we know it'll be set
        writeFinished.Wait(-1, ct) |> ignore 
        writeFinished.Reset()
    currentWBuf := null
    writeAvailable.Set()
    writeFinished.Wait(-1, ct) |> ignore
    pageInfo

/// Finds and returns the first match tuple or None, updates the pointer to the next record
let private seekMatchStart (ptr: byref<nativeptr<byte>>) (endPtr: nativeint) xstart xend = 
    let mutable p = ptr
    let mutable lastKey = 0UL
    let mutable notFound = true
    let mutable v = 0UL
    while notFound && NP.toNativeInt p < endPtr do 
        lastKey <- lastKey + Base128.readB128Fast &p 
        // For value, no delta decode, because we only match on key change, and caller must add minValue
        v <- Base128.readB128Fast &p    
        if lastKey >= xstart then notFound <- false
    ptr <- p
    if not notFound && lastKey <= xend then Some (lastKey, v) else None

let private collectToMatchEnd (ptr: nativeptr<byte>) (endPtr: nativeint) firstKey firstValue globalMinValue xend (res: ResizeArray<_>) =
    res.Add(UInt64Pair(firstKey, firstValue))
    let mutable run = true
    let mutable p = ptr
    let mutable lastKey = firstKey
    let mutable lastValue = firstValue
    while run && NP.toNativeInt p < endPtr do
        let k' = Base128.readB128Fast &p 
        let v = Base128.readB128Fast &p 
        if k' = 0UL then
            // We're still in a matching key, delta the value
            lastValue <- lastValue + v
            res.Add(UInt64Pair(lastKey, lastValue))
        else
            // New key
            lastKey <- lastKey + k'
            if lastKey <= xend then
                lastValue <- globalMinValue + v
                res.Add(UInt64Pair(lastKey, lastValue))
            else
                // No more matches
                run <- false

/// Takes a pointer to the start of the page, the minimum value, and search range.
let searchSinglePagePtr (ptr: nativeptr<byte>) (globalMinValue: uint64) (xstart, xend) (res: ResizeArray<_>) =
    if (xstart > xend) then invalidArg "xstart" "Start value cannot be greater than end value."

    // Read header
    if NP.read (NP.cast ptr) <> 111494688669696UL then failwith "Page header incorrect." 
    let itemCount : uint32 = NP.read (NP.cast (NP.addBytes (ptr) 8)) 
    let itemBytes : uint32 = NP.read (NP.cast (NP.addBytes (ptr) 12))
    if itemCount = 0u || itemBytes = 0u then () else
    if itemBytes < itemCount * 2u then failwithf "Page has %d items, but only %d bytes." itemCount itemBytes
    
    let mutable p = NP.addBytes ptr 16 // Skip header
    let endPtr = NP.addBytes p itemBytes |> NP.toNativeInt
    match seekMatchStart &p endPtr xstart xend with
    | None                      -> ()
    | Some (lastKey, lastValue) -> collectToMatchEnd p endPtr lastKey (globalMinValue + lastValue) globalMinValue xend res

/// Searches via a stream. For now, this is implemented via reading the 
/// entire page into a buffer and using the pointer functions on it.
let searchSinglePageStream (s: IO.Stream) (globalMinValue: uint64) (xstart, xend) (res: ResizeArray<_>) =
    if (xstart > xend) then invalidArg "xstart" "Start value cannot be greater than end value."

    let br = new IO.BinaryReader(s)
    // Read header
    if br.ReadUInt64() <> 111494688669696UL then failwith "Page header incorrect."
    let itemCount = br.ReadUInt32()
    let itemBytes = br.ReadUInt32()
    if itemCount = 0u || itemBytes = 0u then () else
    if itemBytes < itemCount * 2u then failwithf "Page has %d items, but only %d bytes." itemCount itemBytes

    s.Seek(-16L, IO.SeekOrigin.Current) |> ignore

    let buf = Array.zeroCreate(16 + Checked.int itemBytes)
    s.Read(buf, 0, buf.Length) |> ignore
    use pa = PinnedArray.of_array buf
    searchSinglePagePtr pa.Ptr globalMinValue (xstart, xend) res
