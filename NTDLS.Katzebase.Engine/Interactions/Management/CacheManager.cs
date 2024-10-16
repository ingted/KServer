﻿using NTDLS.FastMemoryCache;
using NTDLS.FastMemoryCache.Metrics;
using System.Diagnostics.CodeAnalysis;

namespace NTDLS.Katzebase.Engine.Interactions.Management
{
    /// <summary>
    /// Public core class methods for locking, reading, writing and managing tasks related to cache.
    /// </summary>
    internal class CacheManager<TData> where TData : IStringable
    {
        private readonly EngineCore<TData> _core;
        private readonly PartitionedMemoryCache _cache;

        internal int PartitionCount { get; private set; }

        internal CacheManager(EngineCore<TData> core)
        {
            _core = core;

            try
            {
                var config = new PartitionedCacheConfiguration
                {
                    MaxMemoryBytes = core.Settings.CacheMaxMemoryMegabytes * 1024L * 1024L,
                    IsCaseSensitive = false,
                    PartitionCount = core.Settings.CachePartitions > 0 ? core.Settings.CachePartitions : Environment.ProcessorCount,
                    ScavengeIntervalSeconds = core.Settings.CacheScavengeInterval > 0 ? core.Settings.CacheScavengeInterval : 30
                };

                _cache = new PartitionedMemoryCache(config);
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to instantiate cache manager.", ex);
                throw;
            }
        }

        internal void Close()
        {
            _cache.Dispose();
        }

        internal void Upsert(string key, object value, int approximateSizeInBytes = 0)
        {
            try
            {
                _cache.Upsert(key, value, approximateSizeInBytes);
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to upsert cache object.", ex);
                throw;
            }
        }

        internal void Clear()
        {
            try
            {
                _cache.Clear();
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to clear cache.", ex);
                throw;
            }
        }

        internal CachePartitionAllocationStats GetPartitionAllocationStatistics()
        {
            try
            {
                return _cache.GetPartitionAllocationStatistics(); ;
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to clear cache.", ex);
                throw;
            }
        }

        internal CachePartitionAllocationDetails GetPartitionAllocationDetails()
        {
            try
            {
                return _cache.GetPartitionAllocationDetails();
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to clear cache.", ex);
                throw;
            }
        }

        internal object? TryGet(string key)
        {
            try
            {
                return _cache.TryGet(key);
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to get cache object.", ex);
                throw;
            }
        }

        internal bool TryGet(string key, [NotNullWhen(true)] out object? value)
        {
            try
            {
                if (_cache.TryGet(key, out value))
                {
                    return true;
                }
                value = default;
                return false;
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to get cache object.", ex);
                throw;
            }
        }

        internal bool TryGet<T>(string key, [NotNullWhen(true)] out T? value)
        {
            try
            {
                if (_cache.TryGet(key, out value))
                {
                    return true;
                }
                value = default;
                return false;
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to get cache object.", ex);
                throw;
            }
        }

        internal object Get(string key)
        {
            try
            {
                return _cache.Get(key);
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to get cache object.", ex);
                throw;
            }
        }

        internal bool Remove(string key)
        {
            try
            {
                return _cache.Remove(key);
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to remove cache object.", ex);
                throw;
            }
        }

        internal void RemoveItemsWithPrefix(string prefix)
        {
            try
            {
                _cache.RemoveItemsWithPrefix(prefix);
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to remove cache prefixed-object.", ex);
                throw;
            }
        }
    }
}
