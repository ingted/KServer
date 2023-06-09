﻿namespace Katzebase.PrivateLibrary
{
    public class KatzebaseSettings
    {

        /// <summary>
        /// Whether the engine will keep instance level health metrics. This can be useful but will have a serious impact on performance.
        /// </summary>
        public bool RecordInstanceHealth { get; set; }
        /// <summary>
        /// The maximum amount of memory that the server will be allowed to use before scavenging the cache.
        /// </summary>
        public long MaxCacheMemory { get; set; }
        /// <summary>
        /// The base listening URL for the web-services.
        /// </summary>
        public string BaseAddress { get; set; } = string.Empty;
        /// <summary>
        /// The top level directory for all schemas.
        /// </summary>
        public string DataRootPath { get; set; } = string.Empty;
        /// <summary>
        /// The directory where transaction logs are stored.
        /// </summary>
        public string TransactionDataPath { get; set; } = string.Empty;
        /// <summary>
        /// The directory where text and performance logs are stores.
        /// </summary>
        public string LogDirectory { get; set; } = string.Empty;
        /// <summary>
        /// If true, text logs will be flused at every write. This ensures that the log file is always up-to-date on disk.
        /// </summary>
        public bool FlushLog { get; set; }
        /// <summary>
        /// If true, the all applicable IO operations will be cached on read and write.
        /// </summary>
        public bool AllowIOCaching { get; set; }
        /// <summary>
        /// When true, IO operations that occur in a transaction will be deferred until transaction commit.
        /// </summary>
        public bool AllowDeferredIO { get; set; }

        /// <summary>
        /// Causes the server to write super-verbose information about almost every internal operation.
        /// </summary>
        public bool WriteTraceData { get; set; }
    }
}
