﻿using Katzebase.Engine.KbLib;
using Katzebase.Engine.Transactions;
using Katzebase.PublicLibrary;
using Newtonsoft.Json;
using static Katzebase.Engine.KbLib.EngineConstants;
using static Katzebase.Engine.Trace.PerformanceTrace;

namespace Katzebase.Engine.IO
{
    internal class IOManager
    {
        private readonly Core core;
        public IOManager(Core core)
        {
            this.core = core;
        }

        #region Getters.

        public T? GetJsonNonTracked<T>(string filePath)
        {
            return JsonConvert.DeserializeObject<T>(File.ReadAllText(filePath));
        }

        public T? GetPBufNonTracked<T>(string filePath)
        {
            using (var file = File.OpenRead(filePath))
            {
                return ProtoBuf.Serializer.Deserialize<T>(file);
            }
        }

        internal T? GetJson<T>(Transaction transaction, string filePath, LockOperation intendedOperation)
        {
            return InternalTrackedGet<T>(transaction, filePath, intendedOperation, IOFormat.JSON);
        }

        internal T? GetPBuf<T>(Transaction transaction, string filePath, LockOperation intendedOperation)
        {
            return InternalTrackedGet<T>(transaction, filePath, intendedOperation, IOFormat.PBuf);
        }

        internal T? InternalTrackedGet<T>(Transaction transaction, string filePath, LockOperation intendedOperation, IOFormat format)
        {
            try
            {
                string cacheKey = Helpers.RemoveModFileName(filePath.ToLower());

                transaction.LockFile(intendedOperation, cacheKey);

                if (core.settings.CacheEnabled)
                {
                    var ptCacheRead = transaction.PT?.BeginTrace<T>(PerformanceTraceType.CacheRead);
                    var cachedObject = core.Cache.Get(cacheKey);
                    ptCacheRead?.EndTrace();

                    if (cachedObject != null)
                    {
                        core.Health.Increment(HealthCounterType.IOCacheReadHits);

                        core.Log.Trace($"IO:CacheHit:{transaction.ProcessId}->{filePath}");

                        return (T?)cachedObject;
                    }
                }

                core.Health.Increment(HealthCounterType.IOCacheReadMisses);

                core.Log.Trace($"IO:Read:{transaction.ProcessId}->{filePath}");

                T? deserializedObject;

                if (format == IOFormat.JSON)
                {
                    var ptIORead = transaction.PT?.BeginTrace<T>(PerformanceTraceType.IORead);
                    string text = File.ReadAllText(filePath);
                    ptIORead?.EndTrace();

                    var ptDeserialize = transaction.PT?.BeginTrace<T>(PerformanceTraceType.Deserialize);
                    deserializedObject = JsonConvert.DeserializeObject<T?>(text);
                    ptDeserialize?.EndTrace();
                }
                else if (format == IOFormat.PBuf)
                {
                    var ptIORead = transaction.PT?.BeginTrace<T>(PerformanceTraceType.IORead);
                    using (var file = File.OpenRead(filePath))
                    {
                        ptIORead?.EndTrace();
                        var ptDeserialize = transaction.PT?.BeginTrace<T>(PerformanceTraceType.Deserialize);
                        deserializedObject = ProtoBuf.Serializer.Deserialize<T>(file);
                        ptDeserialize?.EndTrace();
                        file.Close();
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }

                if (core.settings.CacheEnabled && deserializedObject != null)
                {
                    var ptCacheWrite = transaction.PT?.BeginTrace<T>(PerformanceTraceType.CacheWrite);
                    core.Cache.Upsert(cacheKey, deserializedObject);
                    ptCacheWrite?.EndTrace();
                    core.Health.Increment(HealthCounterType.IOCacheReadAdditions);
                }

                return deserializedObject;
            }
            catch (Exception ex)
            {
                core.Log.Write("Failed to get JSON object.", ex);
                throw;
            }
        }

        #endregion

        #region Putters.

        internal void PutJsonNonTracked(string filePath, object deserializedObject)
        {
            File.WriteAllText(filePath, JsonConvert.SerializeObject(deserializedObject));
        }

        internal void PutPBufNonTracked(string filePath, object deserializedObject)
        {
            using (var file = File.Create(filePath))
            {
                ProtoBuf.Serializer.Serialize(file, deserializedObject);
            }
        }

        internal void PutJson(Transaction transaction, string filePath, object deserializedObject)
        {
            InternalTrackedPut(transaction, filePath, deserializedObject, IOFormat.JSON);
        }

        internal void PutPBuf(Transaction transaction, string filePath, object deserializedObject)
        {
            InternalTrackedPut(transaction, filePath, deserializedObject, IOFormat.PBuf);
        }

        private void InternalTrackedPut(Transaction transaction, string filePath, object deserializedObject, IOFormat format)
        {
            try
            {
                string cacheKey = Helpers.RemoveModFileName(filePath.ToLower());
                transaction.LockFile(LockOperation.Write, cacheKey);

                bool deferDiskWrite = false;

                if (transaction != null)
                {
                    bool doesFileExist = File.Exists(filePath);

                    if (doesFileExist == false)
                    {
                        transaction.RecordFileCreate(filePath);
                    }
                    else
                    {
                        transaction.RecordFileAlter(filePath);
                    }

                    if (core.settings.DeferredIOEnabled && transaction.IsUserCreated)
                    {
                        Utility.EnsureNotNull(transaction.DeferredIOs);
                        var ptDeferredWrite = transaction.PT?.BeginTrace(PerformanceTraceType.DeferredWrite);
                        deferDiskWrite = transaction.DeferredIOs.RecordDeferredDiskIO(cacheKey, filePath, deserializedObject, format);
                        ptDeferredWrite?.EndTrace();
                    }
                }

                if (deferDiskWrite == false)
                {
                    core.Log.Trace($"IO:Write:{filePath}");

                    if (format == IOFormat.JSON)
                    {
                        var ptSerialize = transaction?.PT?.BeginTrace(PerformanceTraceType.Serialize);
                        string text = JsonConvert.SerializeObject(deserializedObject);
                        ptSerialize?.EndTrace();
                        File.WriteAllText(filePath, text);
                    }
                    else if (format == IOFormat.PBuf)
                    {
                        using (var file = File.Create(filePath))
                        {
                            var ptSerialize = transaction?.PT?.BeginTrace(PerformanceTraceType.Serialize);
                            ProtoBuf.Serializer.Serialize(file, deserializedObject);
                            ptSerialize?.EndTrace();
                            file.Close();
                        }
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                }
                else
                {
                    core.Log.Trace($"IO:Write-Deferred:{filePath}");
                }

                if (core.settings.CacheEnabled)
                {
                    var ptCacheWrite = transaction?.PT?.BeginTrace(PerformanceTraceType.CacheWrite);
                    core.Cache.Upsert(cacheKey, deserializedObject);
                    ptCacheWrite?.EndTrace();
                    core.Health.Increment(HealthCounterType.IOCacheWriteAdditions);
                }
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to put JSON file for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        #endregion

        internal bool DirectoryExists(Transaction transaction, string diskPath, LockOperation intendedOperation)
        {
            try
            {
                string cacheKey = Helpers.RemoveModFileName(diskPath.ToLower());
                transaction.LockDirectory(intendedOperation, cacheKey);

                core.Log.Trace($"IO:Exists-Directory:{transaction.ProcessId}->{diskPath}");

                return Directory.Exists(diskPath);
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to verify directory for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        internal void CreateDirectory(Transaction transaction, string? diskPath)
        {
            if (diskPath == null)
            {
                throw new ArgumentNullException(nameof(diskPath));
            }

            try
            {
                string cacheKey = Helpers.RemoveModFileName(diskPath.ToLower());
                transaction.LockDirectory(LockOperation.Write, cacheKey);

                bool doesFileExist = Directory.Exists(diskPath);

                core.Log.Trace($"IO:Create-Directory:{transaction.ProcessId}->{diskPath}");

                if (doesFileExist == false)
                {
                    Directory.CreateDirectory(diskPath);
                    transaction.RecordDirectoryCreate(diskPath);
                }
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to create directory for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        internal bool FileExists(Transaction transaction, string filePath, LockOperation intendedOperation)
        {
            try
            {
                string lowerFilePath = filePath.ToLower();

                Utility.EnsureNotNull(transaction.DeferredIOs);

                if (transaction.DeferredIOs.Collection.Values.Any(o => o.DiskPath == lowerFilePath))
                {
                    return true; //The file might not yet exist, but its in the cache.
                }

                string cacheKey = Helpers.RemoveModFileName(lowerFilePath);
                transaction.LockFile(intendedOperation, cacheKey);

                core.Log.Trace($"IO:Exits-File:{transaction.ProcessId}->{filePath}");

                return File.Exists(filePath);
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to verify file for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        internal void DeleteFile(Transaction transaction, string filePath)
        {
            try
            {
                string cacheKey = Helpers.RemoveModFileName(filePath.ToLower());
                transaction.LockFile(LockOperation.Write, cacheKey);

                if (core.settings.CacheEnabled)
                {
                    var ptCacheWrite = transaction.PT?.BeginTrace(PerformanceTraceType.CacheWrite);
                    core.Cache.Remove(cacheKey);
                    ptCacheWrite?.EndTrace();
                }

                transaction.RecordFileDelete(filePath);

                core.Log.Trace($"IO:Delete-File:{transaction.ProcessId}->{filePath}");

                File.Delete(filePath);
                Helpers.RemoveDirectoryIfEmpty(Path.GetDirectoryName(filePath));
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to delete file for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        internal void DeletePath(Transaction transaction, string diskPath)
        {
            try
            {
                string cacheKey = Helpers.RemoveModFileName(diskPath.ToLower());
                transaction.LockDirectory(LockOperation.Write, cacheKey);

                if (core.settings.CacheEnabled)
                {
                    var ptCacheWrite = transaction.PT?.BeginTrace(PerformanceTraceType.CacheWrite);
                    core.Cache.RemoveItemsWithPrefix(cacheKey);
                    ptCacheWrite?.EndTrace();
                }

                transaction.RecordPathDelete(diskPath);

                core.Log.Trace($"IO:Delete-Directory:{transaction.ProcessId}->{diskPath}");

                Directory.Delete(diskPath, true);
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to delete path for process {transaction.ProcessId}.", ex);
                throw;
            }
        }
    }
}
