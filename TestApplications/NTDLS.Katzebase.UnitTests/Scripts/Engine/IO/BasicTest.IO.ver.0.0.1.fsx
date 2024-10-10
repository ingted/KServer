#if INTERACTIVE
#load "../../UnitTestsSharedResource.ver.0.0.2.fsx"
#load "../../Tests.ver.0.0.1.fsx"
#endif

open Shared
open Tests
open Xunit
open Xunit.Abstractions

#if GENERIC_TDATA
#if CELL_STRUCT
open fs
#endif
#endif
module IOTests =

    open NTDLS.Katzebase.Client.Types
    open NTDLS.Katzebase.Client.Payloads
    open NTDLS.Katzebase.Client.Payloads.RoundTrip
    
    let insert001 () =
        use transactionReference = _core.Transactions.Acquire(preLogin)
#if GENERIC_TDATA
        let d = new KbInsensitiveDictionary<fstring>()
#if CELL_STRUCT
        let insert100Resp =
            [1..100]
            |> List.iter (fun i ->
                d.Add($"guy{i}", T ("ttc", A [| S "OGC"; D (double i) |]))
            )
            let result = new KbQueryDocumentStoreReply()
            let docPointer =
                _core.Documents.InsertDocument(
                    transactionReference.Transaction,
                    testSchemaIO,
                    (new KbDocument(d)).Content
                    )
            result.Value <- docPointer.DocumentId
            transactionReference.CommitAndApplyMetricsThenReturnResults(result, d.Count)
        transactionReference.Commit()
        equals 100 insert100Resp.RowCount
#endif
#endif

open PCSL

let persistedList = PersistedConcurrentSortedList<string, fstring>("C:/data", "mySchema")

// 添加数据
let data = A [| S "OGC"; D (double 5) |]
persistedList.Add("myKey", data)

// 获取数据
let found, valueOption = persistedList.TryGetValue("myKey")


PB2.ModelContainer<fstring>.readFromFile @"C:\Data\mySchema\gMks3PgO6AF7ffuXzDfdZW46tjK52v5Af_2ljqU4zpU.val"

PB2.ModelContainer<fstring>.pbModel

open System.IO

let ms = new MemoryStream()
let data1 = S "OGC"

PB2.ModelContainer<fstring>.serializeF (ms, data)

ms.Position <- 0
let dataDeserialized = PB2.ModelContainer<fstring>.deserializeF ms

PB2.ModelContainer<fstring>.readFromFile 