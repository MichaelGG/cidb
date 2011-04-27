namespace CiDB

#nowarn "9"

open System
open System.Runtime.InteropServices

[<Struct>]
[<StructLayout(LayoutKind.Explicit, Pack = 1)>]
[<CustomComparison>]
[<CustomEquality>]
type UInt64Pair = 
    new (k,v) = { Key = k; Value = v }
    [<FieldOffset(0)>]
    val Key : uint64
    [<FieldOffset(8)>]
    val Value : uint64
    
    override x.Equals(y) = 
        match y with 
        | :? UInt64Pair as y ->
            x.Key = y.Key && x.Value = y.Value
        | _ -> false
    
    override x.GetHashCode() = 
        x.Key.GetHashCode() ^^^ x.Value.GetHashCode()

    member inline x.CompareTo (y: UInt64Pair) = 
        let m = compare x.Key y.Key
        if m <> 0 then m else compare x.Value y.Value

    interface IComparable with
        member x.CompareTo y =
            match y with
            | :? UInt64Pair as y -> x.CompareTo(y)
            | _ -> invalidArg "y" "Comparison value was not a UInt64Pair."

    interface IComparable<UInt64Pair> with 
        member x.CompareTo y = x.CompareTo y
