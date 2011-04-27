module private CiDB.Extensions

#nowarn "9"

open System
open Microsoft.FSharp.NativeInterop

module NativePtr = 
    let inline cast p = p |> NativePtr.toNativeInt |> NativePtr.ofNativeInt
    let inline addBytes (p: nativeptr<byte>) n = 
        (p |> NativePtr.toNativeInt) + nativeint n |> NativePtr.ofNativeInt<byte> 

module Base128 = 
    module NP = NativePtr

    (* The inlining and returning the pointer was heavily profiled 
       and seems optimal by a good 30%, versus modifying the pointer
       or not using inline *)
    /// Writes a uint64 in base128, returns updated pointer
    let inline writeB128Fast (ptr: nativeptr<byte>) (x: uint64) =
        let mutable p = ptr 
        let mutable x = x
        while x > 127UL do
            let b = (byte (x ||| 0x80UL))
            NP.write p b
            p <- NP.addBytes p 1
            x <- x >>> 7
        NP.write p (byte x)
        NP.addBytes p 1 
        
    /// Reads a base128 uint64, and returns the value. Pointer updated to after value.
    let inline readB128Fast (ptr: byref<nativeptr<byte>>) =
        let mutable p = ptr
        let mutable res = 0UL
        let mutable i = 0
        let mutable x = 0uy
        while i < 11 do // Max 10 bytes for b128 
            x <- NP.read p 
            p <- NP.addBytes p 1
            res <- res ||| (uint64 (x &&& 0x7Fuy) <<< (7 * i))
            i <- i + 1
            if x &&& 0x80uy = 0uy then i <- 11 
        ptr <- p
        res

    