﻿using NTDLS.Katzebase.Engine.IO;
using NTDLS.Katzebase.Engine.Locking;

namespace NTDLS.Katzebase.Engine.Atomicity
{
    /// <summary>
    /// Snapshot class for Transaction, used to snapshot the state of the associated class.
    /// </summary>
    internal class TransactionSnapshot
    {
        public ObjectLockIntention? CurrentLockIntention { get; set; }
        public string TopLevelOperation { get; set; } = string.Empty;
        public Guid Id { get; set; } = Guid.NewGuid();
        public HashSet<string> FilesReadForCache { get; set; } = new();
        public List<AtomSnapshot> Atoms { get; set; } = new();
        public ulong ProcessId { get; set; }
        public DateTime StartTime { get; set; }
        public bool IsDeadlocked { get; set; }

        /// <summary>
        /// Lock _core.Locking.Collection when accessing.
        /// </summary>
        public List<ObjectLockKeySnapshot> BlockedByKeys { get; set; } = new();

        /// <summary>
        /// Lock _core.Locking.Collection when accessing.
        /// </summary>
        public List<ObjectLockKeySnapshot> HeldLockKeys { get; set; } = new();
        public HashSet<string> TemporarySchemas { get; set; } = new();
        public HashSet<string> GrantedLockCache { get; set; } = new();
        public bool IsUserCreated { get; set; }
        public DeferredDiskIOSnapshot DeferredIOs { get; set; } = new();
        public bool IsCommittedOrRolledBack { get; set; } = false;
        public bool IsCancelled { get; set; } = false;
        public long ReferenceCount { get; set; }
    }
}
