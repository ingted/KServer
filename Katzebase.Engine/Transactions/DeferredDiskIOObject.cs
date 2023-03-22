﻿using static Katzebase.Engine.Constants;

namespace Katzebase.Engine.Transactions
{
    public class DeferredDiskIOObject
    {
        public string LowerDiskPath { get; set; }
        public string DiskPath { get; set; }
        public object Reference { get; set; }
        public long Hits { get; set; } = 0;
        public IOFormat DeferredFormat { get; set; }

        public DeferredDiskIOObject(string diskPath, object reference)
        {
            DiskPath = diskPath;
            LowerDiskPath = diskPath.ToLower();
            Reference = reference;
        }
    }
}
