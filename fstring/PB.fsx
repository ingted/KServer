namespace fs

open System
open System.Collections
open System.Collections.Generic
open NTDLS.Katzebase.Parsers.Interfaces



[<ProtoBuf.ProtoContract>]
type fstring =
| S of string
| D of double
| A of fstring []
| T of string * fstring
    member this.toJsonString() =
        let rec toJson (f: fstring) =
            match f with
            | S s -> sprintf "%s" s
            | D d -> sprintf "%f" d
            | A arr -> 
                let elements = arr |> Array.map toJson |> String.concat ", "
                sprintf "[%s]" elements
            | T (key, value) ->
                let keyStr = sprintf "%s" key
                let valueStr = toJson value
                sprintf "%s: %s" keyStr valueStr
        toJson this

    static member compareArrays (arr1: fstring array) (arr2: fstring array): int =
        Seq.zip arr1 arr2
        |> Seq.tryPick (fun (x, y) ->
            let res = compare x y
            if res = 0 then None else Some res)
        |> Option.defaultValue 0

    static member compareLength (arr1: fstring array) (arr2: fstring array): int =
        let a1l = if arr1 = null then 0 else arr1.Length
        let a2l = if arr2 = null then 0 else arr2.Length
        compare a1l a2l

    static member Compare (x: fstring, y: fstring): int =
        match box x, box y with
        | null, null -> 0
        | _, null -> 
            if x = S null then 0 else 1
        | null, _ ->
            if y = S null then 0 else -1
        | _ ->
            match (x, y) with
            | (D d1, D d2) -> Decimal.Compare(decimal d1, decimal d2)
            | (S s1, S s2) -> String.Compare(s1, s2, StringComparison.OrdinalIgnoreCase)
            | (A arr1, A arr2) ->
                let lenComp = fstring.compareLength arr1 arr2
                if lenComp <> 0 then lenComp
                else fstring.compareArrays arr1 arr2
            | (T (tag1, f1), T (tag2, f2)) ->
                let tagComp = String.Compare(tag1, tag2, StringComparison.OrdinalIgnoreCase)
                if tagComp <> 0 then tagComp
                else fstring.Compare(f1, f2)
            | (D _, _) -> -1
            | (_, D _) -> 1
            | (S _, (A _ | T _)) -> -1
            | ((A _ | T _), S _) -> 1
            | (A _, T _) -> -1
            | (T _, A _) -> 1

    interface IComparer<fstring> with
        override this.Compare(x: fstring, y: fstring): int =
            fstring.Compare(x, y)

    member this.s =
        match this with
        | S s -> s
        | _ -> failwith "Not fstring.S."

    member this.d =
        match this with
        | D d -> d
        | _ -> failwith "Not fstring.D."

    member this.ToLowerInvariant () =
        match this with
        | S "" -> fstring.SNull
        | S s -> s.ToLowerInvariant() |> S
        | _ -> failwith "Not fstring.S."

    static member val CompareFunc : Func<fstring, fstring, bool> = 
        FuncConvert.FromFunc(fun (x: fstring) (y: fstring) -> fstring.Compare(x, y) = 0)

    static member aFromStringArr (sArr) =
        sArr
        |> Array.map S
        |> A

    static member fromStringArr (sArr) =
        sArr
        |> Array.map S

    static member val SEmpty        = S ""                          with get
    static member val AEmpty        = A [||]                        with get
    static member val SNull         = S null                        with get
    static member val ANull         = A null                        with get
    static member val Unassigned    = Unchecked.defaultof<fstring>  with get

    static member SIsNullOrEmpty (o:fstring) = if box o = null || o = fstring.SEmpty || o = fstring.SNull then true else false
    static member AIsNullOrEmpty (o:fstring) = if box o = null || o = fstring.AEmpty || o = fstring.ANull then true else false
    static member IsNull (o:fstring) = 
        if box o = null || o = fstring.ANull || o = fstring.SNull then true else false

    // 加入 IStringable 接口的實作
    interface IStringable with
        override this.GetKey () = this.s
        override this.IsNullOrEmpty () = this.s = null || this.s = ""
        override this.ToLowerInvariant () = this.ToLowerInvariant()
        override this.ToT<'T> () =
            match typeof<'T> with
            | t when t = typeof<string> -> box this.s :?> 'T
            | t when t = typeof<double> -> box (Double.Parse this.s) :?> 'T
            | t when t = typeof<int> -> box (Int32.Parse this.s) :?> 'T
            | t when t = typeof<bool> -> box (Boolean.Parse this.s) :?> 'T
            | t -> failwithf "type %s not supported" t.Name

        override this.ToT (t: Type) =
            match t with
            | t when t = typeof<string> -> box this.s
            | t when t = typeof<double> -> box (Double.Parse this.s)
            | t when t = typeof<int> -> box (Int32.Parse this.s)
            | t when t = typeof<bool> -> box (Boolean.Parse this.s)
            | _ -> failwithf "type %s not supported" t.Name

        override this.ToNullableT<'T> () =
            match typeof<'T> with
            | t when t = typeof<string> -> box this.s :?> 'T
            | t when t = typeof<double> -> box (Double.Parse this.s) :?> 'T
            | t when t = typeof<int> -> box (Int32.Parse this.s) :?> 'T
            | t when t = typeof<bool> -> box (Boolean.Parse this.s) :?> 'T
            | _ -> failwithf "type %s not supported" typeof<'T>.Name


    member this.me =
        match this with
        | S s -> s
        | D d -> d.ToString()
        | A arr -> arr |> Array.map (fun f -> f.me) |> String.concat ", "
        | T (key, value) -> sprintf "%s: %s" key value.me

    member this.Value = this.me

    //new () =
    //    S null
open System.Runtime.CompilerServices

[<Extension>]
module FS =
    let mapper = new Dictionary<Type, fstring -> obj>()

    let _ =
        mapper.Add(
            typeof<string>, (fun (S s) -> box s)
        )
    let _ =
        mapper.Add(
            typeof<double>, (fun (D d) -> box d)
        )

    let _ =
        mapper.Add(
            typeof<fstring []>, (fun (A a) -> box a)
        )

    let _ =
        mapper.Add(
            typeof<string * fstring>, (fun (T (k, v)) -> box (KeyValuePair.Create(k, v)))
        )

    [<Extension>]
    let toType (this: fstring, t:Type) = mapper[t] this



[<Extension>]
module ExtensionsString =
    [<Extension>]
    let toF(str : string) = S str
[<Extension>]
module ExtensionsDecimal =
    [<Extension>]
    let toF(d : decimal) = D (double d)
[<Extension>]
module ExtensionsDouble =
    [<Extension>]
    let toF(d : double) = D d

[<Extension>]
module ExtensionsInt =
    [<Extension>]
    let toF(d : int) = D (double d)

    
module PB =
    open ProtoBuf.Meta
    open ProtoBuf.FSharp
    let mutable pbModel = //lazy (
        printfn "???????????????????????????????????????????????????????????????"
        RuntimeTypeModel.Create("???")
        |> Serialiser.registerUnionIntoModel<fstring> 
        //

    let serializeFBase m (ms, o) =
        printfn "[serializeF] type: %s, %A" (o.GetType().Name) o
        Serialiser.serialise m ms o

    let deserializeFBase<'T> m ms =
        printfn "[deserializeF] 'T: %s" typeof<'T>.Name
        Serialiser.deserialise<'T> m ms

    let serializeF (ms, o) = serializeFBase pbModel (ms, o)

    let deserializeF<'T> ms = 
        deserializeFBase<'T> pbModel ms

module PB2 =
    open System.IO
    open ProtoBuf.Meta
    open ProtoBuf.FSharp
    open System.Security.Cryptography
    open System.Text

    let generateStrHash (input: string) : string =
        use sha256 = SHA256.Create()
        let bytes = Encoding.UTF8.GetBytes(input)
        let hashBytes = sha256.ComputeHash(bytes)

        // 使用 Base64 编码并替换非法字符
        let base64Hash = Convert.ToBase64String(hashBytes)
        let safeHash = base64Hash.Replace("/", "-").Replace("+", "_").Replace("=", "")

        // 返回 Windows 安全的文件名
        safeHash

    let generateBArrHash (bytes: byte[]) : string =
        use sha256 = SHA256.Create()
        let hashBytes = sha256.ComputeHash(bytes)

        // 使用 Base64 编码并替换非法字符
        let base64Hash = Convert.ToBase64String(hashBytes)
        let safeHash = base64Hash.Replace("/", "-").Replace("+", "_").Replace("=", "")

        // 返回 Windows 安全的文件名
        safeHash

    type ModelContainer<'T> () =
        static let lockObj = obj()
        static member val pbModel =
            lock lockObj (fun () ->
                printfn $"?????????????????????????? {typeof<'T>.Name} ??????????????????????????"
                RuntimeTypeModel.Create(typeof<'T>.Name)
                |> Serialiser.registerUnionIntoModel<'T> 
            )
            with get, set

        static member serializeFBase (m, (ms, o)) =
            printfn "[serializeF] type: %s, %A" (o.GetType().Name) o
            Serialiser.serialise m ms o

        static member deserializeFBase (m, ms) =
            printfn "[deserializeF] 'T: %s" typeof<'T>.Name
            Serialiser.deserialiseConcreteType<'T> m ms

        static member serializeF (ms, o) = 
            ModelContainer<'T>.serializeFBase (ModelContainer<'T>.pbModel, (ms, o))

        static member serializeF2BArr o = 
            let ms = new MemoryStream()
            ModelContainer<'T>.serializeFBase (ModelContainer<'T>.pbModel, (ms, o))
            ms.ToArray()
            |> Deflate.compress

        static member getHashStr o = 
            ModelContainer<'T>.serializeF2BArr o
            |> generateBArrHash

        static member deserializeF ms = 
            ModelContainer<'T>.deserializeFBase (ModelContainer<'T>.pbModel, ms)

        static member write2File = fun filePath (o:'T) ->            
            File.WriteAllBytes(filePath, ModelContainer<'T>.serializeF2BArr o)

        static member readFromFileBase = fun filePath ->
            let serializedData = File.ReadAllBytes(filePath) |> Deflate.decompress
            // 使用反序列化来读取值
            use input = new MemoryStream(serializedData)
            input.Position <- 0
            ModelContainer<'T>.deserializeF input
        
        static member readFromFile = fun filePath ->
            if File.Exists(filePath) then
                ModelContainer<'T>.readFromFileBase filePath |> Some
            else
                None

    let registerType<'T> (rtmOpt:RuntimeTypeModel option) =
        if rtmOpt.IsNone then
            ModelContainer<'T>.pbModel
        else
            let updated = 
                //rtmOpt.Value.GetTypes() => 如果後續有需要，看是不是把所有的 type 的 ModelContainer<'T>.pbModel 都更新
                rtmOpt.Value
                |> Serialiser.registerUnionIntoModel<'T>
            ModelContainer<'T>.pbModel <- updated
            ModelContainer<'T>.pbModel

    

module CSL_ORZ =
    open System
    open System.Collections.Generic
    open System.Collections.Concurrent
    open System.Threading.Tasks

    let createTask (fn: unit -> 'T) : Task<'T> =
        new Task<'T>(fun () -> fn())
    
    type Op<'Key, 'Value when 'Key : comparison> =
    | CAdd of 'Key * 'Value

    type OpResult<'Key, 'Value when 'Key : comparison> =
    | CUnit

    type ConcurrentSortedList<'Key, 'Value when 'Key : comparison>() =
        let sortedList = SortedList<'Key, 'Value>()
        let lockObj = obj()

        let opQueue = ConcurrentQueue<Task<OpResult<'Key, 'Value>>> ()


        member this.Run () =
            lock lockObj (fun () ->
                while (opQueue.Count > 0) do
                    match opQueue.TryDequeue () with
                    | true, t -> 
                        t.RunSynchronously()
                    | false, _ -> ()
            )

        member this.AddBase(key: 'Key, value: 'Value) =
            sortedList.Add(key, value)



        member this.TryGetValue(key: 'Key) : bool * 'Value option =
            lock lockObj (fun () ->
                if sortedList.ContainsKey(key) then
                    true, Some(sortedList.[key])
                else
                    false, None
            )

        member this.Item
            with get(key: 'Key) =
                lock lockObj (fun () -> sortedList.[key])
            and set(key: 'Key) (value: 'Value) =
                lock lockObj (fun () -> sortedList.[key] <- value)

        member this.Count =
            lock lockObj (fun () -> sortedList.Count)

        member this.Values =
            lock lockObj (fun () -> sortedList.Values)

        member this.Keys =
            lock lockObj (fun () -> sortedList.Keys)

        member this.Remove(key: 'Key) =
            lock lockObj (fun () -> sortedList.Remove(key) |> ignore)

        // 新增的 TryUpdate 方法
        member this.TryUpdate(key: 'Key, newValue: 'Value) : bool =
            lock lockObj (fun () ->
                if sortedList.ContainsKey(key) then
                    sortedList.[key] <- newValue
                    true
                else
                    false
            )

        member this.Clean () =
            lock lockObj (fun () ->
                sortedList.Clear ()
            )

        //member this.ContainsKey k =
            

        member this.LockableOp (op: Op<'Key, 'Value>) =
            let rtnTask = 
                match op with
                | CAdd (k, v) ->
                    fun () ->
                        this.AddBase (k, v)
                        CUnit
                |> createTask
            
            opQueue.Enqueue rtnTask
            this.Run ()

            rtnTask


        member this.Add (k, v) =
            this.LockableOp (CAdd (k, v))


module CSL =
    open System
    open System.Collections.Generic
    open System.Collections.Concurrent
    open System.Threading.Tasks

    let createTask (fn: unit -> 'T) : Task<'T> =
        new Task<'T>(fun () -> fn())

    let t = createTask (fun () -> 123)

    (*
    async {
        do! Async.Sleep 5000
        t.Start ()
    } |> Async.Start
    
    t.Result
    *)

    type Op<'Key, 'Value when 'Key : comparison> =
    | CAdd of 'Key * 'Value
    | CRemove of 'Key
    | CUpdate of 'Key * 'Value
    | CGet of 'Key
    | CCount
    | CValues
    | CKeys
    | CClean

    type OpResult<'Key, 'Value when 'Key : comparison> =
    | CUnit
    | CBool of bool
    | COptionValue of (bool * 'Value option)
    | CInt of int
    | CKeyList of IList<'Key>
    | CValueList of IList<'Value>


    type TResult<'Key, 'Value when 'Key : comparison> (t:Task<OpResult<'Key, 'Value>>) =
        member this.Result = t.Result
        member this.WaitIgnore = t.Result |> ignore
        member this.IsCompleted = t.IsCompleted
        member this.IsCanceled = t.IsCanceled
        member this.fIsCompletedSuccessfully = t.IsCompletedSuccessfully
        member this.fIsFaulted = t.IsFaulted

    type ConcurrentSortedList<'Key, 'Value when 'Key : comparison>() =
        let sortedList = SortedList<'Key, 'Value>()
        let lockObj = obj()
        let opQueue = ConcurrentQueue<Task<OpResult<'Key, 'Value>>>() // 操作任务队列

        /// 运行队列中的任务
        let rec run () =
            async {
                match opQueue.TryDequeue() with
                | true, t -> 
                    t.RunSynchronously()  // 同步执行任务
                    do! run ()
                | false, _ -> ()
            }

        member this.Run () =
            lock lockObj (fun () ->
                run () |> Async.Start
            )
        /// 基础的 Add 操作（直接修改内部数据结构）
        member this.AddBase(key: 'Key, value: 'Value) =
            sortedList.Add(key, value)

        /// 基础的 Remove 操作
        member this.RemoveBase(key: 'Key) =
            sortedList.Remove(key)

        /// 尝试更新，如果键存在则更新值
        member this.TryUpdateBase(key: 'Key, newValue: 'Value) : bool =
            if sortedList.ContainsKey(key) then
                sortedList.[key] <- newValue
                true
            else
                false

        member this.TryGetValueBase(key: 'Key) : bool * 'Value option =
            if sortedList.ContainsKey(key) then
                true, Some(sortedList.[key])
            else
                false, None


        /// 封装操作并添加到任务队列，执行后返回 Task
        member this.LockableOp (op: Op<'Key, 'Value>) : TResult<'Key, 'Value> =
            let taskToEnqueue = 
                match op with
                | CAdd (k, v) ->
                    fun () ->
                        this.AddBase(k, v)
                        CUnit
                | CRemove k ->
                    fun () ->
                        this.RemoveBase(k) |> CBool

                | CUpdate (k, v) ->
                    fun () ->
                        this.TryUpdateBase(k, v) |> CBool

                | CGet k ->
                    fun () ->
                        this.TryGetValueBase(k) |> COptionValue
                | CCount ->
                    fun () -> 
                        sortedList.Count |> CInt
                | CValues ->
                    fun () -> 
                        sortedList.Values |> CValueList
                | CKeys ->
                    fun () -> 
                        sortedList.Keys |> CKeyList
                | CClean ->
                    fun () ->
                        sortedList.Clear()
                        CUnit
                |> createTask
            
            // 将操作添加到队列并运行队列
            opQueue.Enqueue taskToEnqueue
            this.Run ()

            TResult<_, _>(taskToEnqueue)

        /// Add 方法：将添加操作封装为任务并执行
        member this.Add(k, v) =
            this.LockableOp(CAdd(k, v))

        /// Remove 方法：将移除操作封装为任务并执行
        member this.Remove(k) =
            this.LockableOp(CRemove(k))

        /// TryUpdate 方法：将更新操作封装为任务并执行
        member this.TryUpdate(k, v) =
            this.LockableOp(CUpdate(k, v))

        /// TryGetValue 同步获取值，不需要队列
        member this.TryGetValue(key: 'Key) : bool * 'Value option =
            lock lockObj (fun () ->
                if sortedList.ContainsKey(key) then
                    true, Some(sortedList.[key])
                else
                    false, None
            )

        member this.TryGetValueSafe(key: 'Key) : bool * 'Value option =
            let (COptionValue r) = this.LockableOp(CGet key).Result
            r

        /// 访问 Item：带线程锁的读写访问
        member this.Item
            with get(key: 'Key) =
                (this.TryGetValueSafe key |> snd).Value
            and set(k: 'Key) (v: 'Value) =
                //lock lockObj (fun () -> sortedList.[key] <- value)
                this.TryUpdate(k, v).Result |> ignore

        /// 获取 Count
        member this.Count =
            lock lockObj (fun () -> sortedList.Count)

        /// 获取所有 Values
        member this.Values =
            lock lockObj (fun () -> sortedList.Values)

        /// 获取所有 Keys
        member this.Keys =
            lock lockObj (fun () -> sortedList.Keys)

        /// 清空列表
        member this.Clean () =
            lock lockObj (fun () -> sortedList.Clear())
        
        /// 获取 Count - Safe 版本
        member this.CountSafe : int =
            let (CInt count) = this.LockableOp(CCount).Result
            count

        /// 获取所有 Values - Safe 版本
        member this.ValuesSafe : IList<'Value> =
            let (CValueList values) = this.LockableOp(CValues).Result
            values

        /// 获取所有 Keys - Safe 版本
        member this.KeysSafe : IList<'Key> =
            let (CKeyList keys) = this.LockableOp(CKeys).Result
            keys

        /// 清空列表 - Safe 版本
        member this.CleanSafe() =
            this.LockableOp(CClean).Result |> ignore


module PCSL =

    open System
    open System.IO
    open System.Security.Cryptography
    open System.Text
    open CSL
    open PB2
    open System.Threading.Tasks




    type KeyHash = string

    type SortedListLogicalStatus =
    | Inserted
    | Updated
    | Deleted

    type SortedListPersistenceStatus =
    | Buffered //already read from disk and stored in ConcurrentSortedList<'Key, 'Value>, but not changed 
               //or just updated/persisted
    | Changed  //already read from disk and stored in ConcurrentSortedList<'Key, 'Value>, but     changed
    | NonBuffered //just updated/persisted and removed from buffer

    type PersistedConcurrentSortedList<'Key, 'Value when 'Key : comparison>(basePath: string, schemaName: string) =
        let sortedList = ConcurrentSortedList<'Key, 'Value>()
        let sortedListStatus = ConcurrentSortedList<'Key, SortedListLogicalStatus>()
        let sortedListPersistenceStatus = ConcurrentSortedList<'Key, SortedListPersistenceStatus>()
        let sortedListIndexReversed = ConcurrentSortedList<KeyHash, 'Key>()
        let sortedListIndex         = ConcurrentSortedList<'Key, KeyHash>()
        let schemaPath = Path.Combine(basePath, schemaName)
        let mutable initialized = false

        let sch () =
            // 创建 schema 文件夹和索引文件夹
            if not (Directory.Exists(schemaPath)) then
                Directory.CreateDirectory(schemaPath) |> ignore


        let indexInitialization () =
            sortedListIndex.Clean ()
            sortedListIndexReversed.Clean ()
            sortedListPersistenceStatus.Clean ()
            let di = DirectoryInfo schemaPath
            di.GetFiles()
            |> Array.iter (fun fi ->
                let key = ModelContainer<'Key>.readFromFileBase fi.FullName
                let baseName = fi.Name.Replace(".index", "")
                sortedListIndex.Add(key, baseName).WaitIgnore
                sortedListIndexReversed.Add(baseName, key).WaitIgnore
                sortedListPersistenceStatus.Add(key, NonBuffered).WaitIgnore
            )



        do 
            sch ()
            indexInitialization ()
            initialized <- true

        // 生成 SHA-256 哈希
        let generateHash (key: 'Key) : string =
            ModelContainer<'Key>.getHashStr key


        // 存储 value 到文件
        let writeToFile (key: 'Key) (value: 'Value) ifRemoveFromBuffer =
            if sortedListIndex
            let hashKey = generateHash key
            let filePath = Path.Combine(schemaPath, hashKey + ".val")
            ModelContainer<'Value>.write2File filePath value

            if ifRemoveFromBuffer then
                sortedList.Remove key
                sortedListPersistenceStatus.TryUpdate (key, NonBuffered)
            else
                sortedListPersistenceStatus.TryUpdate (key, Buffered)

        // 从文件中反序列化
        let readFromFile (key: 'Key) : 'Value option =
            let hashKey = sortedListIndex[key]
            let filePath = Path.Combine(schemaPath, hashKey + ".val")
            ModelContainer<'Value>.readFromFile filePath

        member this.Initialized = initialized
        // 添加 key-value 对
        member this.Add(key: 'Key, value: 'Value) =
            lock sortedList (fun () ->
                sortedList.Add(key, value)
                writeToFile key value
            )
        member this.AddAsync(key: 'Key, value: 'Value) =
            task {
                this.Add(key, value)
            }

        // 获取 value，如果 ConcurrentSortedList 中不存在则从文件系统中读取
        member this.TryGetValue(key: 'Key) : bool * 'Value option =
            lock sortedList (fun () ->
                let exists, value = sortedList.TryGetValue(key)
                if exists then
                    true, value
                else
                    sch ()
                    match readFromFile key with
                    | Some v ->
                        // 文件存在，反序列化并存入 ConcurrentSortedList
                        sortedList.Add(key, v)
                        true, Some v
                    | None -> false, None
            )

        member this.TryGetValueAsync(key: 'Key) : Task<bool * 'Value option> =
            task {
                return this.TryGetValue(key)
            }
        // 其他成员可以根据需要进行扩展，比如 Count、Remove 等
