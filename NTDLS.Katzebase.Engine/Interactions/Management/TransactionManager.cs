﻿using Newtonsoft.Json;
using NTDLS.Helpers;
using NTDLS.Katzebase.Engine.Atomicity;
using NTDLS.Katzebase.Engine.Instrumentation;
using NTDLS.Katzebase.Engine.Interactions.APIHandlers;
using NTDLS.Katzebase.Engine.Interactions.QueryHandlers;
using NTDLS.Katzebase.Engine.Sessions;
using NTDLS.Semaphore;
using System.Diagnostics;
using static NTDLS.Katzebase.Engine.Instrumentation.InstrumentationTracker;
using static NTDLS.Katzebase.Engine.Library.EngineConstants;

namespace NTDLS.Katzebase.Engine.Interactions.Management
{
    /// <summary>
    /// Public core class methods for locking, reading, writing and managing tasks related to transactions.
    /// </summary>
    public class TransactionManager<TData> where TData : IStringable
    {
        private readonly EngineCore<TData> _core;
        private readonly OptimisticCriticalResource<List<Transaction<TData>>> _collection = new();

        internal TransactionQueryHandlers<TData> QueryHandlers { get; private set; }
        public TransactionAPIHandlers<TData> APIHandlers { get; private set; }

        internal TransactionReference<TData> Acquire(SessionState session)
        {
            var transactionReference = Acquire(session, false);

            var stackFrames = (new StackTrace()).GetFrames();
            if (stackFrames.Length >= 2)
            {
                transactionReference.Transaction.TopLevelOperation = stackFrames[1].GetMethod()?.Name ?? string.Empty;
            }

            return transactionReference;
        }

        internal List<TransactionSnapshot> Snapshot()
        {
            var collectionClone = new List<Transaction<TData>>();

            _collection.Read((obj) => collectionClone.AddRange(obj));

            var clones = new List<TransactionSnapshot>();

            foreach (var item in collectionClone)
            {
                clones.Add(item.Snapshot());
            }

            return clones;
        }

        internal TransactionManager(EngineCore<TData> core)
        {
            _core = core;
            try
            {
                QueryHandlers = new TransactionQueryHandlers<TData>(core);
                APIHandlers = new TransactionAPIHandlers<TData>(core);
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to instantiate transaction manager.", ex);
                throw;
            }
        }

        internal Transaction<TData>? GetByProcessId(ulong processId)
        {
            try
            {
                return _collection.Read((obj) => obj.FirstOrDefault(o => o.ProcessId == processId));
            }
            catch (Exception ex)
            {
                LogManager.Error($"Failed to get transaction by process id for process id {processId}.", ex);
                throw;
            }
        }

        internal void RemoveByProcessId(ulong processId)
        {
            try
            {
                _collection.Write((obj) =>
                {
                    var transaction = GetByProcessId(processId);
                    if (transaction != null)
                    {
                        obj.Remove(transaction);
                    }
                });
            }
            catch (Exception ex)
            {
                LogManager.Error($"Failed to remove transaction by process id for process {processId}.", ex);
                throw;
            }
        }

        /// <summary>
        /// Kills all transactions associated with the given processID.
        /// This is typically called from the session manager and probably should not be called otherwise.
        /// </summary>
        /// <param name="processIDs"></param>
        internal void CloseByProcessID(ulong processId)
        {
            try
            {
                _collection.Write((obj) =>
                {
                    var transaction = GetByProcessId(processId);
                    if (transaction != null)
                    {
                        transaction.Rollback();

                        obj.Remove(transaction);
                    }
                });

            }
            catch (Exception ex)
            {
                LogManager.Error($"Failed to remove transactions by processID.", ex);
                throw;
            }
        }

        internal void Recover()
        {
            try
            {
                Directory.CreateDirectory(_core.Settings.TransactionDataPath);

                var transactionFiles = Directory.EnumerateFiles(
                    _core.Settings.TransactionDataPath, TransactionActionsFile, SearchOption.AllDirectories).ToList();

                if (transactionFiles.Count != 0)
                {
                    LogManager.Warning($"Found {transactionFiles.Count} open transactions.");
                }

                foreach (string transactionFile in transactionFiles)
                {
                    var processIdString = Path.GetFileNameWithoutExtension(Path.GetDirectoryName(transactionFile));
                    ulong processId = ulong.Parse(processIdString.EnsureNotNull());

                    var transaction = new Transaction<TData>(_core, this, processId, true);

                    var atoms = File.ReadLines(transactionFile).ToList();
                    foreach (var atom in atoms)
                    {
                        var ra = JsonConvert.DeserializeObject<Atom>(atom).EnsureNotNull();
                        transaction.Atoms.Write((obj) => obj.Add(ra));
                    }

                    LogManager.Warning($"Rolling back session {transaction.ProcessId} with {atoms.Count} actions.");

                    try
                    {
                        transaction.Rollback();
                    }
                    catch (Exception ex)
                    {
                        LogManager.Error($"Failed to rollback transaction for process {transaction.ProcessId}.", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to recover uncommitted transactions.", ex);
                throw;
            }
        }

        /// <summary>
        /// Begin an atomic operation. If the session already has an open transaction then its
        /// reference count is incremented and then decremented on TransactionReference.Dispose();
        /// </summary>
        /// <param name="processId"></param>
        /// <returns></returns>
        internal TransactionReference<TData> Acquire(SessionState session, bool isUserCreated)
        {
            var startTime = DateTime.UtcNow;

            try
            {
                return _collection.Write((obj) =>
                {
                    InstrumentationDurationToken? ptAcquireTransaction = null;
                    var transaction = GetByProcessId(session.ProcessId);
                    if (transaction == null)
                    {
                        transaction = new Transaction<TData>(_core, this, session.ProcessId, false)
                        {
                            IsUserCreated = isUserCreated
                        };

                        ptAcquireTransaction = transaction.Instrumentation.CreateToken(PerformanceCounter.AcquireTransaction);

                        obj.Add(transaction);
                    }

                    if (isUserCreated)
                    {
                        //We might be several transactions deep when we see the first user created transaction.
                        //That means we need to convert this transaction to a user transaction.
                        transaction.IsUserCreated = true;
                    }

                    transaction.AddReference();

                    ptAcquireTransaction?.StopAndAccumulate((DateTime.UtcNow - startTime).TotalMilliseconds);

                    return new TransactionReference<TData>(transaction);
                });
            }
            catch (Exception ex)
            {
                LogManager.Error($"Failed to acquire transaction for process {session.ProcessId}.", ex);
                throw;
            }
        }

        internal void Commit(SessionState session)
            => Commit(session.ProcessId);

        internal void Commit(ulong processId)
        {
            try
            {
                GetByProcessId(processId)?.Commit();
            }
            catch (Exception ex)
            {
                LogManager.Error($"Failed to commit transaction for process {processId}.", ex);
                throw;
            }
        }

        internal void Rollback(SessionState session)
            => Rollback(session.ProcessId);

        internal void Rollback(ulong processId)
        {
            try
            {
                GetByProcessId(processId)?.Rollback();
            }
            catch (Exception ex)
            {
                LogManager.Error($"Failed to rollback transaction for process {processId}.", ex);
                throw;
            }
        }
    }
}
