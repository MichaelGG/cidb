module CiDB.Tests

open System

let genValues (count: int32) minValue range mask = 
    let mask = if mask = 64 then (uint64 -1L) else (1UL <<< mask) - 1UL
    let res = ResizeArray<CiDB.UInt64Pair>(count)
    let perVal = min count 3
    let seed = DateTime.UtcNow.GetHashCode()
    //printfn "Seed: %d" seed
    let r = Random(seed)
    let buf = Array.zeroCreate 8
    for i = 1 to (count / perVal) do
        let valNums = if i = (count / perVal) then count - res.Count else perVal
        r.NextBytes(buf)
        let key = BitConverter.ToUInt64(buf, 0)
        let key = key &&& mask
        for x = 1 to valNums do
            let v = r.Next(minValue, minValue + range) |> uint64
            res.Add(CiDB.UInt64Pair(key, v))
    let res = ResizeArray<_>(res |> Seq.distinct)
    res.Sort()
    res

let perfTest() =
    let someVals = genValues 2000000 0 3000 40
    let sw = Diagnostics.Stopwatch.StartNew()
    let min = someVals |> Seq.minBy(fun x -> x.Value) 
    printf "Start now"
    Console.ReadLine() |> ignore
    let sw = Diagnostics.Stopwatch.StartNew()
    for i = 1 to 2 do
        use fs = IO.File.Create("C:\\cidb.dat", 1)
        //use bs = new IO.BufferedStream(fs, 4096 * 16)
        CiDB.DBFile.writeFile fs 1024 min.Value someVals (int64 someVals.Count)
        //bs.Flush()
        fs.Flush(true)
    let elapsed = sw.Elapsed
    let fileLen = ((new IO.FileInfo("C:\\cidb.dat")).Length)
    printfn "Elapsed: %A Bytes %d Per Record: %f" elapsed fileLen (float fileLen / float someVals.Count)

let fullDataInOut size baseOffset =
    let someVals = genValues size 12345 10000000 40
    let count = someVals.Count |> int64
    let minValue = (someVals |> Seq.minBy(fun x -> x.Value)).Value - 64UL
    use ms = new IO.MemoryStream(someVals.Count * 4)
    ms.Seek(baseOffset, IO.SeekOrigin.Begin) |> ignore
    CiDB.DBFile.writeFile ms 4096 minValue someVals count
    ms.Seek(baseOffset, IO.SeekOrigin.Begin) |> ignore
    let db = CiDB.DBStreamFile(ms)
    let sres = db.Search(0UL, UInt64.MaxValue)
    if sres.Count <> someVals.Count then failwith "Search has %d items, originally %d." sres.Count someVals.Count
    for i = 0 to sres.Count - 1 do
        if sres.[i] <> someVals.[i] then failwith "Items don't match."
    printfn "Full data (size %d, offset %d) returned." size baseOffset

let fullDataInOutMapped size = 
    let filename = IO.Path.GetTempFileName()
    try 
        let someVals = genValues size 12345 10000000 40
        let count = someVals.Count |> int64
        let minValue = (someVals |> Seq.minBy(fun x -> x.Value)).Value - 64UL
        do  use fs = IO.File.OpenWrite filename
            CiDB.DBFile.writeFile fs 4096 minValue someVals count
        use db = new CiDB.DBMappedFile(filename)
        let sres = db.Search(0UL, UInt64.MaxValue)
        if sres.Count <> someVals.Count then failwith "Search has %d items, originally %d." sres.Count someVals.Count
        for i = 0 to sres.Count - 1 do
            if sres.[i] <> someVals.[i] then failwith "Items don't match."
        printfn "Full mapped data (%d) returned." size
    finally
        IO.File.Delete filename

let writeLogTestConvert() =
    let filename = IO.Path.GetTempFileName()
    IO.File.Delete filename // WriteLog's gonna expect no file
    try
        let someVals = genValues 100000 12345 100000 40
        use wl = new CiDB.WriteLog(filename, 1024 * 1024 * 16)
        do someVals 
            |> Seq.groupBy(fun x -> x.Value) 
            |> Seq.sortBy (fun (x,_) -> x)
            |> Seq.iter (fun (v, xs) -> wl.WriteRecords v (ResizeArray<_>(xs |> Seq.map(fun x -> x.Key))))
        wl.Dispose()
        use wl = new CiDB.WriteLog(filename, 0) // Test recovery
        match wl.ExpandLog() with
        | None -> failwith "ExpandLog failed."
        | Some  (res,minValue,maxValue) ->
        if res.Count <> someVals.Count then failwith "Expanded count doesn't match."
        for i = 0 to res.Count - 1 do
            if res.[i] <> someVals.[i] then failwithf "Expanded items don't match, index %d." i
        let realMin = (someVals |> Seq.minBy(fun x -> x.Value)).Value
        let realMax = (someVals |> Seq.maxBy(fun x -> x.Value)).Value
        if realMin <> minValue then failwith "Expanded min value incorrect."
        if realMax <> maxValue then failwith "Expanded max value incorrect."
        printfn "WriteLog converted fully."

        use ms = new IO.MemoryStream(res.Count * 4)
        ms.Seek(3L, IO.SeekOrigin.Begin) |> ignore
        CiDB.DBFile.writeFile ms 4096 minValue res (int64 res.Count)
        ms.Seek(3L, IO.SeekOrigin.Begin) |> ignore
        let db = CiDB.DBStreamFile(ms)
        let sres = db.Search(0UL, UInt64.MaxValue)
        if sres.Count <> someVals.Count then failwith "Search has %d items, originally %d." sres.Count someVals.Count
        for i = 0 to sres.Count - 1 do
            if sres.[i] <> someVals.[i] then failwith "Items don't match."   
        printfn "Converted WriteLog fully returned via DBStreamFile."                     
    finally
        IO.File.Delete filename

let specificSearches() = 
    printfn "Starting specific searches"
    let someVals = genValues 250000 12345 10000000 64
    do 
        let kv = UInt64Pair(0UL, 0UL)
        if someVals.Contains kv |> not then someVals.Add kv
        let kv = UInt64Pair(UInt64.MaxValue, UInt64.MaxValue)
        if someVals.Contains kv |> not then someVals.Add kv
        someVals.Sort()
    let count = someVals.Count |> int64
    let minValue = (someVals |> Seq.minBy(fun x -> x.Value)).Value - 64UL
    use ms = new IO.MemoryStream(someVals.Count * 4)
    ms.Seek(123L, IO.SeekOrigin.Begin) |> ignore
    printfn "    - Writing file"
    CiDB.DBFile.writeFile ms 512 minValue someVals count
    ms.Seek(123L, IO.SeekOrigin.Begin) |> ignore
    printfn "    - Reading file"
    let db = CiDB.DBStreamFile(ms)

    let searchOrig xStart xEnd =
        someVals |> ResizeArray.filter (fun x -> x.Key >= xStart && x.Key <= xEnd)
    let searchAndCompare xStart xEnd =
        let someVals = searchOrig xStart xEnd
        let sres = db.Search(xStart, xEnd) 
        if sres.Count <> someVals.Count then failwithf "Search has %d items, original search %d." sres.Count someVals.Count
        for i = 0 to sres.Count - 1 do
            let a = sres.[i]
            let b = sres.[i]
            if a.Key <> b.Key || a.Value <> b.Value then failwithf "Items don't match, index %d." i
                      
    let r = Random()
    let getRand =
        let buf = Array.zeroCreate 8
        let q = Collections.Generic.Queue()
        for i = 1 to 50000 do
            r.NextBytes(buf)
            q.Enqueue(BitConverter.ToUInt64(buf, 0))
        q.Dequeue

    let sw = Diagnostics.Stopwatch.StartNew()    

    searchAndCompare 0UL 0UL
    searchAndCompare UInt64.MaxValue UInt64.MaxValue
    printfn "    - Simple queries passed"
    
    let randSearch() = 
        let a,b = getRand(), getRand()
        let a = min a b
        let b = max a b
        searchAndCompare a b
    for i = 0 to 100 do randSearch()
    printfn "    - Random range passed"
    for i = 0 to 100 do
        let a = getRand()
        searchAndCompare a a
    printfn "    - Random single passed"
    for i = 0 to 100 do
        let a = someVals.[r.Next(someVals.Count)].Key
        searchAndCompare a a
    printfn "    - Sampled key single passed"
    for i = 0 to 100 do 
        let a = someVals.[r.Next(someVals.Count)].Key
        let b = someVals.[r.Next(someVals.Count)].Key
        let a = min a b
        let b = max a b
        searchAndCompare a b
    printfn "    - Sampled key range passed"
    for i = 0 to 100 do
        let a = someVals.[r.Next(someVals.Count)].Key
        let b = someVals.[r.Next(someVals.Count)].Key
        let a = (min a b) - 1UL
        let b = (max a b) + 1UL
        let a = min a b
        let b = max a b
        searchAndCompare a b  
    printfn "    - Sampled key range off +1 passed"              
    for i = 0 to 100 do
        let a = someVals.[r.Next(someVals.Count)].Key
        let b = someVals.[r.Next(someVals.Count)].Key
        let a = (min a b) + 1UL
        let b = (max a b) - 1UL
        let a = min a b
        let b = max a b
        searchAndCompare a b        
    printfn "    - Sampled key range off -1 passed"              
    printfn "All specific searches finished (%A)." sw.Elapsed

let noDataOK() = 
    use ms = new IO.MemoryStream()
    CiDB.DBFile.writeFile ms 512 0UL (seq[]) 0L
    ms.Seek(0L, IO.SeekOrigin.Begin) |> ignore
    let db = CiDB.DBStreamFile(ms)
    let sres = db.Search(0UL, UInt64.MaxValue)
    if sres.Count <> 0 then failwith "Expected no search results."
    printfn "No Data OK"

let fullBattery i = 
    if i % 2 = 0 then writeLogTestConvert()
    fullDataInOut 1 0L
    fullDataInOut 511 0L
    fullDataInOut 512 0L
    fullDataInOut 513 0L
    if i % 2 <> 0 then specificSearches()
    fullDataInOut 4095 0L
    fullDataInOut 4096 0L
    fullDataInOut 4097 0L
    fullDataInOut 500 0L
    fullDataInOut 500 12345L
    fullDataInOut 100000 3L
    if i % 2 = 0 then specificSearches()
    fullDataInOutMapped 500 
    fullDataInOutMapped 100000 
    noDataOK()
    if i % 2 <> 0 then writeLogTestConvert()

do
    log4net.Config.BasicConfigurator.Configure()
    printfn "Setting threads: %b" <| Threading.ThreadPool.SetMinThreads(20, 10)
    specificSearches()
    for i = 1 to 100 do
        let tasks = [|1..10|] 
                    |> Array.map (fun i -> Threading.Tasks.Task.Factory.StartNew(fun () -> fullBattery i))
        Threading.Tasks.Task.WaitAll(tasks)
        GC.Collect()
        GC.WaitForPendingFinalizers()
        GC.Collect()
        GC.WaitForPendingFinalizers()
        printfn "Loop done."
    printfn "Tests done."
     