namespace oo
module PTEST = 
 
    open fs
    open PB2
    open CSL
    open PCSL
    open System.Collections.Generic


    type PCSLFunHelper<'Key, 'Value when 'Key: comparison and 'Value: comparison> =
        //static let kvkt = typeof<'Key>
        //static let kvvt = typeof<'Value>
        //static let kht = typeof<KeyHash>
        //static let pst = typeof<SortedListPersistenceStatus>



        static member oFun (id: SLTyp) (opResult: OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>):PCSLTaskTyp<'Key, 'Value> = 
#if DEBUG1
            printfn $"SLId: {id}: oFun"
#endif
            match opResult with
            | CUnit -> 
                match id with
                | TSLIdxR -> IdxR CUnit
                | TSLIdx  -> Idx  CUnit
                | TSLPSts -> PS   CUnit
                | TSL     -> KV   CUnit
                | _ -> failwith "Unsupported type in CUnit"

            | CAdded added -> 
                match id with
                | TSLIdxR -> IdxR (CAdded added)
                | TSLIdx  -> Idx  (CAdded added)
                | TSLPSts  ->PS   (CAdded added)
                | TSL     -> KV   (CAdded added)
                | _ -> failwith "Unsupported type in CAdded"

            | CUpdated -> 
                match id with
                | TSLIdxR -> IdxR CUpdated
                | TSLIdx  -> Idx  CUpdated
                | TSLPSts -> PS   CUpdated
                | TSL     -> KV   CUpdated
                | _ -> failwith "Unsupported type in CUpdated"

            | CBool b -> 
                match id with
                | TSLIdxR -> IdxR (CBool b)
                | TSLIdx  -> Idx  (CBool b)
                | TSLPSts -> PS   (CBool b)
                | TSL     -> KV   (CBool b)
                | _ -> failwith "Unsupported type in CBool"

            | COptionValue (exists, value) -> 
                let vt = typeof<'Value>
                printfn "vvvvvvvv: %s %A %A" vt.Name value id
                match id with
                | TSLIdxR -> 
                    let vOpt = value |> Option.map (fun (SLK v)  -> v)
                    IdxR (COptionValue (exists, vOpt))
                | TSL -> 
                    let vOpt = value |> Option.map (fun (SLV v)  -> v)
                    KV   (COptionValue (exists, vOpt))
                | TSLIdx -> 
                    let vOpt = value |> Option.map (fun (SLKH v) -> v)
                    Idx  (COptionValue (exists, vOpt))
                | TSLPSts -> 
                    let vOpt = value |> Option.map (fun (SLPS v) -> v)
                    PS   (COptionValue (exists, vOpt))
                | _ -> failwith "Unsupported type in COptionValue"

            | CInt i -> 
                match id with
                | TSLIdxR -> IdxR (CInt i)
                | TSLIdx  -> Idx  (CInt i)
                | TSLPSts -> PS   (CInt i)
                | TSL     -> KV   (CInt i)
                | _ -> failwith "Unsupported type in CInt"

            | CKeyList keys -> 
                //match typeof<'Key>, typeof<'Value> with
                //| tk, tv when tk = kvkt && tv = kvvt -> 
                //    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                //    KV (CKeyList ks)
                //| tk, _ when tk = kht -> 
                //    let ks = keys |> Seq.map (fun (SLKH k) -> k) |> (fun s -> List<_>(s)) :> IList<KeyHash>
                //    IdxR (CKeyList ks)
                //| tk, tv when tk = kvkt && tv = pst -> 
                //    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                //    PS (CKeyList ks)
                //| tk, tv when tk = kvvt && tv = kht -> 
                //    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                //    Idx (CKeyList ks)
                //| _ -> failwith "Unsupported type in CKeyList"
                match id with
                | TSL -> 
                    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                    KV (CKeyList ks)
                | TSLIdxR -> 
                    let ks = keys |> Seq.map (fun (SLKH k) -> k)|> (fun s -> List<_>(s)) :> IList<KeyHash>
                    IdxR (CKeyList ks)
                | TSLPSts -> 
                    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                    PS (CKeyList ks)
                | TSLIdx -> 
                    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                    Idx (CKeyList ks)
                | _ -> failwith "Unsupported type in CKeyList"

            | CValueList values -> 
                match id with
                | TSLIdxR -> 
                    let vs = values |> Seq.map (fun (SLK v) -> v) |> (fun s -> List<_>(s)) :> IList<'Key>
                    IdxR (CValueList vs)
                | TSL -> 
                    let vs = values |> Seq.map (fun (SLV v) -> v) |> (fun s -> List<_>(s)) :> IList<'Value>
                    KV (CValueList vs)
                | TSLIdx -> 
                    let vs = values |> Seq.map (fun (SLKH v) -> v)|> (fun s -> List<_>(s)) :> IList<KeyHash>
                    Idx (CValueList vs)
                | TSLPSts -> 
                    let vs = values |> Seq.map (fun (SLPS v) -> v)|> (fun s -> List<_>(s)) :> IList<SortedListPersistenceStatus>
                    PS (CValueList vs)
                | _ -> failwith "Unsupported type in CValueList"




        static member eFun (id: SLTyp) (taskResult: PCSLTaskTyp<'Key, 'Value>): OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>> =
            printfn $"SLId: {id}: eFun"
            match taskResult with
            | KV (o:OpResult<'Key, 'Value>) ->
                match o with
                | CUnit -> 
                    CUnit
                | CAdded added -> 
                    CAdded added
                | CUpdated -> 
                    CUpdated
                | CBool b -> 
                    CBool b
                | COptionValue (exists, value) -> 
                    let vOpt = value |> Option.map (fun v -> SLV v)
                    COptionValue (exists, vOpt)
                | CInt i -> 
                    CInt i
                | CKeyList keys -> 
                    let ks = keys |> Seq.map (fun (k:'Key) -> SLK k) |> fun l -> List<_> l :> IList<_>
                    CKeyList ks
                | CValueList values -> 
                    let vs = values |> Seq.map (fun v -> SLV v) |> fun l -> List<_> l :> IList<_>
                    CValueList vs
            | Idx (o:OpResult<'Key, KeyHash>) ->
                match o with
                | CUnit -> 
                    CUnit
                | CAdded added -> 
                    CAdded added
                | CUpdated -> 
                    CUpdated
                | CBool b -> 
                    CBool b
                | COptionValue (exists, value) -> 
                    let vOpt = value |> Option.map (fun v -> SLKH v)
                    COptionValue (exists, vOpt)
                | CInt i -> 
                    CInt i
                | CKeyList keys -> 
                    let ks = keys |> Seq.map (fun (k:'Key) -> SLK k) |> fun l -> List<_> l :> IList<_>
                    CKeyList ks
                | CValueList values -> 
                    let vs = values |> Seq.map (fun v -> SLKH v) |> fun l -> List<_> l :> IList<_>
                    CValueList vs
            | IdxR (o:OpResult<KeyHash, 'Key>) ->
                match o with
                | CUnit -> 
                    CUnit
                | CAdded added -> 
                    CAdded added
                | CUpdated -> 
                    CUpdated
                | CBool b -> 
                    CBool b
                | COptionValue (exists, value) -> 
                    let vOpt = value |> Option.map (fun v -> SLK v)
                    COptionValue (exists, vOpt)
                | CInt i -> 
                    CInt i
                | CKeyList keys -> 
                    let ks = keys |> Seq.map (fun (k:KeyHash) -> SLKH k) |> fun l -> List<_> l :> IList<_>
                    CKeyList ks
                | CValueList values -> 
                    let vs = values |> Seq.map (fun v -> SLK v) |> fun l -> List<_> l :> IList<_>
                    CValueList vs
            | PS (o:OpResult<'Key, SortedListPersistenceStatus>) ->
                match o with
                | CUnit -> 
                    CUnit
                | CAdded added -> 
                    CAdded added
                | CUpdated -> 
                    CUpdated
                | CBool b -> 
                    CBool b
                | COptionValue (exists, value) -> 
                    let vOpt = value |> Option.map (fun v -> SLPS v)
                    COptionValue (exists, vOpt)
                | CInt i -> 
                    CInt i
                | CKeyList keys -> 
                    let ks = keys |> Seq.map (fun (k:'Key) -> SLK k) |> fun l -> List<_> l :> IList<_>
                    CKeyList ks
                | CValueList values -> 
                    let vs = values |> Seq.map (fun v -> SLPS v) |> fun l -> List<_> l :> IList<_>
                    CValueList vs

    
    //type fsk = fstring
    //type fsv = Option<fstring>

    let pcsl = PersistedConcurrentSortedList<string, fsv>(
        20, @"c:\pfcfsl", "test"
        , PCSLFunHelper<string, fsv>.oFun
        , PCSLFunHelper<string, fsv>.eFun)

    printfn "%A" pcsl._idx._base.Keys.Count
    //let success = pcsl.Add ("ORZ", (A [| S "GG"|]), 3000)

    printfn "%A" (pcsl.TryGetValue ("ORZ"))
    printfn "%A" (pcsl.TryGetValue ("ORZ2"))

    0