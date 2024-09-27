namespace NTDLS.Katzebase.UnitTests

open Shared
open BasicTest.Parser.ver.``0``.``0``.``1``
open BasicTest.IO.ver.``0``.``0``.``1``.IOTests
open BasicTest.DDLExecution.ver.``0``.``0``.``1``

module KatzebaseTests =
    //ParserBasicTests.``Parse "SELECT * FROM MASTER:ACCOUNT"`` None 
    ParserBasicTests.``[Condition] Parse "SELECT * FROM MASTER:ACCOUNT WHERE Username = ?Username AND PasswordHash = ?PasswordHash"`` None
    //DDLExecutionBasicTests.``Execute "CREATE SCHEMA testSch"`` None
    printfn "Done!"        