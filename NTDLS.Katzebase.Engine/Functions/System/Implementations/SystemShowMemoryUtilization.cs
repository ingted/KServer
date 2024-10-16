﻿using NTDLS.Helpers;
using NTDLS.Katzebase.Client.Payloads;
using NTDLS.Katzebase.Engine.Atomicity;
using System.Diagnostics;

namespace NTDLS.Katzebase.Engine.Functions.System.Implementations
{
    internal static class SystemShowMemoryUtilization<TData> where TData : IStringable
    {
        public static KbQueryResultCollection<TData> Execute(EngineCore<TData> core, Transaction<TData> transaction, SystemFunctionParameterValueCollection<TData> function)
        {
            var cachePartitions = core.Cache.GetPartitionAllocationDetails();
            long totalCacheSize = 0;
            foreach (var partition in cachePartitions.Items)
            {
                totalCacheSize += partition.ApproximateSizeInBytes;
            }

            var collection = new KbQueryResultCollection<TData>();
            var result = collection.AddNew();
            result.AddField("Working Set");
            result.AddField("Min. Working Set");
            result.AddField("Max. WorkingSet");
            result.AddField("Peak Working Set");
            result.AddField("Paged Memory");
            result.AddField("Non-paged System Memory");
            result.AddField("Peak Paged Memory");
            result.AddField("Peak Virtual Memory");
            result.AddField("Virtual Memory");
            result.AddField("Private Memory");
            result.AddField("Cache Size");

            var process = Process.GetCurrentProcess();

            var values = new List<TData>(new[]
            {
                $"{Formatters.FileSize(process.WorkingSet64)}",
                $"{Formatters.FileSize(process.MinWorkingSet)}",
                $"{Formatters.FileSize(process.MaxWorkingSet)}",
                $"{Formatters.FileSize(process.PeakWorkingSet64)}",
                $"{Formatters.FileSize(process.PagedMemorySize64)}",
                $"{Formatters.FileSize(process.NonpagedSystemMemorySize64)}",
                $"{Formatters.FileSize(process.PeakPagedMemorySize64)}",
                $"{Formatters.FileSize(process.PeakVirtualMemorySize64)}",
                $"{Formatters.FileSize(process.VirtualMemorySize64)}",
                $"{Formatters.FileSize(process.PrivateMemorySize64)}",
                $"{Formatters.FileSize(totalCacheSize)}",
            }.Select(s => s.CastToT<TData>(EngineCore<TData>.StrCast)));

            result.AddRow(values);

            return collection;
        }
    }
}
