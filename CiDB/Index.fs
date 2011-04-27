module private CiDB.Index

#nowarn "9"

open System
open Microsoft.FSharp.NativeInterop
open System.Runtime.InteropServices

open Extensions

module NP = NativePtr

(*
Index is not compressed. Estimated savings not worth complexity.

Creates a two-level index. 
Second level contains full value (uint64) and relative page position (uint64).
First level contains list of full values from second level, at fanout interval

Header (16 bytes) is:
  - Cookie "\000\000iNDX\000\000"B (97050396524544UL)
  - Page count (int32): number of entries in second level
  - Fanout (int32): number of second level entries per first level
*)

let calcIndexSize (recordCount: int64) pageSize = 
    let pageCount = decimal recordCount / decimal pageSize |> ceil |> int
    let fanout = sqrt (float pageCount) |> round |> int |> max 16
    let l1Entries = decimal pageCount / decimal fanout |> ceil |> int
    16 + (l1Entries * 8) + (pageCount * 16)

/// Writes the pageData from the MemoryStream (provided by Page.writeValuesAsPages) in the index format
let writeIndex (s: IO.Stream) (pageData: byte array) =
    let pageCount = pageData.Length / 16 // Each page is a 64-bit key,64-value
    // Since we're just two-level, fanout should be the sqrt of the pages
    // lower bound of 16 just for fun
    let fanout = sqrt (float pageCount) |> round |> int |> max 16
    
    // Header
    s.Write("\000\000iNDX\000\000"B, 0, 8)
    s.Write(BitConverter.GetBytes(pageCount), 0, 4)
    s.Write(BitConverter.GetBytes(fanout), 0, 4)
    
    if pageCount = 0 then () // Empty indexes are important
    else
        let l1Entries = decimal pageCount / decimal fanout |> ceil |> int
        for i = 0 to l1Entries - 1 do
            s.Write(pageData, i * 16 * fanout, 8)
        s.Write(pageData, 0, int pageData.Length)

/// Searches an index, returning all matching page offset positions 
let searchIndexStream (s: IO.Stream) (xStart, xEnd) =
    let br = new IO.BinaryReader(s)
    if br.ReadUInt64() <> 97050396524544UL then failwith "Index is invalid. Cookie not found." else
    let pageCount = br.ReadInt32()
    let fanout = br.ReadInt32() 
    let l1Pos = s.Position
    let l1Entries = decimal pageCount / decimal fanout |> ceil |> int

    match pageCount with
    | 0 -> ResizeArray<_>()
    | 1 -> s.Seek(l1Pos + (int64 l1Entries * 8L), IO.SeekOrigin.Begin) |> ignore
           let k = br.ReadUInt64()
           if k > xEnd then ResizeArray<_>() else ResizeArray<_>(seq[br.ReadUInt64()])
    | _ ->

    // Find L2 position via L1 index
    br.ReadUInt64() |> ignore // BUGBUG: Assume 0 start
    let rec searchL1 i =
        if i = l1Entries then l1Entries - 1 else
        let x = br.ReadUInt64()
        if x >= xStart then (i - 1) else searchL1 (i + 1)
    let l2Start = searchL1 1

    // Seek to L2 read position
    s.Seek(l1Pos + (int64 l1Entries * 8L) + (int64 l2Start * int64 fanout * 16L), IO.SeekOrigin.Begin) |> ignore

    // At this point, we are at position l2Start in the L2 index.
    // This location is at or before the first matching page. 
    // So, find the first matching page. Then collect until a non-matching page.
    let pagePoses = ResizeArray()
    let rec findFirst i lastPos = 
        // When we find a match (key >= start) then return the last page
        // Because the values may have started in the previous page
        if i = pageCount then (i - 1, lastPos) else 
        let key = br.ReadUInt64()
        let pos = br.ReadUInt64()
        if key >= xStart then (i - 1, lastPos) else findFirst (i + 1) pos

    let rec collectToEnd i = 
        if i = pageCount then () else
        let key = br.ReadUInt64()
        let pos = br.ReadUInt64()
        if key > xEnd then () else pagePoses.Add pos; collectToEnd (i + 1)
    
    br.ReadUInt64() |> ignore // BUGBUG: Don't care about the current key
    let firstPos = br.ReadUInt64() 
    let firstI, firstPos = findFirst (l2Start * fanout + 1) firstPos
    pagePoses.Add firstPos
    s.Seek(-16L, IO.SeekOrigin.Current) |> ignore
    collectToEnd (firstI + 1)
    pagePoses