﻿using Katzebase.Library;
using Katzebase.Library.Payloads;
using Microsoft.AspNetCore.Mvc;

namespace Katzebase.Service.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class TransactionController
    {
        [HttpGet]
        [Route("{sessionId}/Begin")]
        public KbActionResponse Begin(Guid sessionId)
        {
            ulong processId = Program.Core.Sessions.UpsertSessionId(sessionId);
            Thread.CurrentThread.Name = Thread.CurrentThread.Name = $"API:{processId}:{Utility.GetCurrentMethod()}";
            Program.Core.Log.Trace(Thread.CurrentThread.Name);

            KbActionResponse result = new KbActionResponse();

            try
            {
                Program.Core.Transactions.Begin(processId, true);
                result.Success = true;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
            }

            return result;
        }

        [HttpGet]
        [Route("{sessionId}/Commit")]
        public KbActionResponse Commit(Guid sessionId)
        {
            ulong processId = Program.Core.Sessions.UpsertSessionId(sessionId);
            Thread.CurrentThread.Name = Thread.CurrentThread.Name = $"API:{processId}:{Utility.GetCurrentMethod()}";
            Program.Core.Log.Trace(Thread.CurrentThread.Name);

            KbActionResponse result = new KbActionResponse();

            try
            {
                Program.Core.Transactions.Commit(processId);
                result.Success = true;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
            }

            return result;
        }

        [HttpGet]
        [Route("{sessionId}/Rollback")]
        public KbActionResponse Rollback(Guid sessionId)
        {
            ulong processId = Program.Core.Sessions.UpsertSessionId(sessionId);
            Thread.CurrentThread.Name = Thread.CurrentThread.Name = $"API:{processId}:{Utility.GetCurrentMethod()}";
            Program.Core.Log.Trace(Thread.CurrentThread.Name);

            KbActionResponse result = new KbActionResponse();

            try
            {
                Program.Core.Transactions.Rollback(processId);
                result.Success = true;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
            }

            return result;
        }
    }
}
