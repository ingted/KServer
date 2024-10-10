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

        static member deserializeF ms = 
            ModelContainer<'T>.deserializeFBase (ModelContainer<'T>.pbModel, ms)

        static member write2File = fun filePath (o:'T) ->
            let output = new MemoryStream()
            ModelContainer<'T>.serializeF(output, o)
            let compressedBytes = Deflate.compress (output.ToArray())
            File.WriteAllBytes(filePath, compressedBytes)

        static member readFromFile = fun filePath ->
            if File.Exists(filePath) then
                // 从文件读取字节数据
                let serializedData = File.ReadAllBytes(filePath)

                // 使用反序列化来读取值
                use input = new MemoryStream(serializedData)
                input.Position <- 0
                let deserializedObject = ModelContainer<'T>.deserializeF input
                Some deserializedObject
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

    

module CSL =
    open System
    open System.Collections.Generic

    open System.Security.Cryptography
    open System.Text

    let generateHash (input: string) : string =
        use sha256 = SHA256.Create()
        let bytes = Encoding.UTF8.GetBytes(input)
        let hashBytes = sha256.ComputeHash(bytes)

        // 使用 Base64 编码并替换非法字符
        let base64Hash = Convert.ToBase64String(hashBytes)
        let safeHash = base64Hash.Replace("/", "-").Replace("+", "_").Replace("=", "")

        // 返回 Windows 安全的文件名
        safeHash

    type ConcurrentSortedList<'Key, 'Value when 'Key : comparison>() =
        let sortedList = SortedList<'Key, 'Value>()
        let lockObj = obj()

        member this.Add(key: 'Key, value: 'Value) =
            lock lockObj (fun () ->
                sortedList.Add(key, value)
            )

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



module PCSL =

    open System
    open System.IO
    open System.Security.Cryptography
    open System.Text
    open CSL
    open PB2

    type PersistedConcurrentSortedList<'Key, 'Value when 'Key : comparison>(basePath: string, schemaName: string) =
        let sortedList = ConcurrentSortedList<'Key, 'Value>()
        let schemaPath = Path.Combine(basePath, schemaName)

        let sch () =
            // 创建 schema 文件夹
            if not (Directory.Exists(schemaPath)) then
                Directory.CreateDirectory(schemaPath) |> ignore

        do sch ()

        // 生成 SHA-256 哈希
        let generateHash (key: 'Key) : string =
            let keyStr = key.ToString()
            CSL.generateHash keyStr

        // 存储 value 到文件
        let writeToFile (key: 'Key) (value: 'Value) =
            let hashKey = generateHash key
            let filePath = Path.Combine(schemaPath, hashKey + ".val")
            ModelContainer<'Value>.write2File filePath value

        // 从文件中反序列化
        let readFromFile (key: 'Key) : 'Value option =
            let hashKey = generateHash key
            let filePath = Path.Combine(schemaPath, hashKey + ".val")
            ModelContainer<'Value>.readFromFile filePath

        // 添加 key-value 对
        member this.Add(key: 'Key, value: 'Value) =
            lock sortedList (fun () ->
                sortedList.Add(key, value)
                sch ()
                writeToFile key value
            )

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

        // 其他成员可以根据需要进行扩展，比如 Count、Remove 等
