﻿using NTDLS.Katzebase.Client.Payloads;
using NTDLS.Katzebase.Engine.Atomicity;

namespace NTDLS.Katzebase.Engine.Functions.System.Implementations
{
    internal static class SystemShowWaitingLocks
    {
        public static KbQueryResultCollection<TData> Execute<TData>(EngineCore<TData> core, Transaction<TData> transaction, SystemFunctionParameterValueCollection<TData> function) where TData : IStringable
        {
            var collection = new KbQueryResultCollection<TData>();
            var result = collection.AddNew();

            result.AddField("ProcessId");
            result.AddField("Granularity");
            result.AddField("Operation");
            result.AddField("Object Name");

            var waitingTxSnapshots = core.Locking.SnapshotWaitingTransactions().ToList();

            var processId = function.GetNullable<ulong?>("processId");
            if (processId != null)
            {
                waitingTxSnapshots = waitingTxSnapshots.Where(o => o.Key.ProcessId == processId).ToList();
            }

            foreach (var waitingForLock in waitingTxSnapshots)
            {
                var values = new List<TData>(new[]
                {
                    waitingForLock.Key.ProcessId.ToString(),
                    waitingForLock.Value.Granularity.ToString(),
                    waitingForLock.Value.Operation.ToString(),
                    waitingForLock.Value.ObjectName.ToString(),
                }.Select(s => s.CastToT<TData>(EngineCore<TData>.StrCast)));
                result.AddRow(values);
            }

            return collection;
        }
    }
}
