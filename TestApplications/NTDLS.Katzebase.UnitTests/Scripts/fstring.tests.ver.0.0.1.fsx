//module Shared
(*

Please remeber to add --define:GENERIC_TDATA to F# fsi when doing generic version engine core debugging

*)

#if INTERACTIVE
#r @"nuget: Newtonsoft.Json, 13.0.3"
#r @"nuget: NTDLS.DelegateThreadPooling, 1.4.8"
#r @"nuget: NTDLS.FastMemoryCache, 1.7.5"
#r @"nuget: NTDLS.Helpers, 1.3.3"
#r "nuget: NCalc"
#r @"nuget: Serilog, 4.0.1"
#r @"nuget: NTDLS.ReliableMessaging, 1.10.9.0"
#r @"nuget: protobuf-net"
#r @"../../../NTDLS.Katzebase.Shared/bin/Debug/net8.0/NTDLS.Katzebase.Shared.dll"
#r @"../../../NTDLS.Katzebase.Engine/bin/Debug/net8.0/NTDLS.Katzebase.Engine.dll"
#r @"../../../NTDLS.Katzebase.Engine/bin/Debug/net8.0/NTDLS.Katzebase.Client.dll"
//#if GENERIC_TDATA
#r @"../../../NTDLS.Katzebase.Engine/bin/Debug/net8.0/NTDLS.Katzebase.Parsers.Generic.dll"
//#if CELL_STRUCT
//#r @"../../../NTDLS.Katzebase.Engine/bin/Debug/net8.0/fstring.dll"
#r @"G:\coldfar_py\sharftrade9\Libs5\KServer\fstring\bin\Debug\net9.0\fstring.dll"
#r @"G:\coldfar_py\sharftrade9\Libs5\KServer\protobuf-net-fsharp\src\ProtoBuf.FSharp\bin\Debug\netstandard2.0\protobuf-net-fsharp.dll"
//#endif
//#else
//#r @"../../../NTDLS.Katzebase.Engine/bin/Debug/net8.0/NTDLS.Katzebase.Parsers.dll"
//#endif
#endif
open Newtonsoft.Json
open NTDLS.Katzebase.Shared
open NTDLS.Katzebase.Engine
open NTDLS.Katzebase.Parsers.Query.Fields
open System
open System.IO
open System.Reflection
open ProtoBuf
open NTDLS.Katzebase.Parsers.Interfaces

open fs
open PCSL


let oFun (opResult: OpResult<'Key, 'Value>) = 
    match opResult with
    | CUnit -> PCSLTaskTyp<'Key>.Idx CUnit
    | CAdded added -> PCSLTaskTyp<'Key>.Idx (CAdded added)
    | CUpdated -> PCSLTaskTyp<'Key>.Idx CUpdated
    | CBool b -> PCSLTaskTyp<'Key>.Idx (CBool b)
    | COptionValue (exists, value) -> PCSLTaskTyp<'Key>.Idx (COptionValue (exists, value))
    | CInt i -> PCSLTaskTyp<'Key>.Idx (CInt i)
    | CKeyList keys -> PCSLTaskTyp<'Key>.Idx (CKeyList keys)
    | CValueList values -> PCSLTaskTyp<'Key>.Idx (CValueList values)

let eFun (taskResult: PCSLTaskTyp<'Key>) =
    match taskResult with
    | PCSLTaskTyp<'Key>.Idx opResult -> opResult
    | PCSLTaskTyp<'Key>.PS _ -> failwith "Not implemented"
    | PCSLTaskTyp<'Key>.IdxR _ -> failwith "Not implemented"


let pcsl = PersistedConcurrentSortedList<fstring, fstring>(20, @"c:\pcsl", "test")