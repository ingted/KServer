﻿using NTDLS.Katzebase.Client.Payloads;
using NTDLS.Katzebase.Engine.Atomicity;
using NTDLS.Katzebase.Engine.Functions.Procedures.Persistent;
using NTDLS.Katzebase.Engine.Interactions.APIHandlers;
using NTDLS.Katzebase.Engine.Interactions.QueryHandlers;
using NTDLS.Katzebase.Engine.Schemas;
using NTDLS.Katzebase.Shared;
using static NTDLS.Katzebase.Engine.Library.EngineConstants;

namespace NTDLS.Katzebase.Engine.Interactions.Management
{
    /// <summary>
    /// Public core class methods for locking, reading, writing and managing tasks related to procedures.
    /// </summary>
    public class ProcedureManager<TData> where TData : IStringable
    {
        private readonly EngineCore<TData> _core;

        internal ProcedureQueryHandlers<TData> QueryHandlers { get; private set; }
        public ProcedureAPIHandlers<TData> APIHandlers { get; private set; }

        internal ProcedureManager(EngineCore<TData> core)
        {
            _core = core;

            try
            {
                QueryHandlers = new ProcedureQueryHandlers<TData>(core);
                APIHandlers = new ProcedureAPIHandlers<TData>(core);

                //ProcedureCollection.Initialize();
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to instantiate procedures manager.", ex);
                throw;
            }
        }

        internal void CreateCustomProcedure(Transaction<TData> transaction, string schemaName,
            string objectName, List<PhysicalProcedureParameter> parameters, List<string> Batches)
        {
            var physicalSchema = _core.Schemas.Acquire(transaction, schemaName, LockOperation.Write);
            var physicalProcedureCatalog = Acquire(transaction, physicalSchema, LockOperation.Write);

            var physicalProcedure = physicalProcedureCatalog.GetByName(objectName);
            if (physicalProcedure == null)
            {
                physicalProcedure = new PhysicalProcedure()
                {
                    Id = Guid.NewGuid(),
                    Name = objectName,
                    Created = DateTime.UtcNow,
                    Modified = DateTime.UtcNow,
                    Parameters = parameters,
                    Batches = Batches,
                };

                physicalProcedureCatalog.Add(physicalProcedure);

                _core.IO.PutJson(transaction, physicalSchema.ProcedureCatalogFilePath(), physicalProcedureCatalog);
            }
            else
            {
                physicalProcedure.Parameters = parameters;
                physicalProcedure.Batches = Batches;
                physicalProcedure.Modified = DateTime.UtcNow;

                _core.IO.PutJson(transaction, physicalSchema.ProcedureCatalogFilePath(), physicalProcedureCatalog);
            }
        }

        internal PhysicalProcedureCatalog Acquire(Transaction<TData> transaction, PhysicalSchema<TData> physicalSchema, LockOperation intendedOperation)
        {
            if (File.Exists(physicalSchema.ProcedureCatalogFilePath()) == false)
            {
                _core.IO.PutJson(transaction, physicalSchema.ProcedureCatalogFilePath(), new PhysicalProcedureCatalog());
            }

            return _core.IO.GetJson<PhysicalProcedureCatalog>(transaction, physicalSchema.ProcedureCatalogFilePath(), intendedOperation);
        }

        internal PhysicalProcedure? Acquire(Transaction<TData> transaction,
            PhysicalSchema<TData> physicalSchema, string procedureName, LockOperation intendedOperation)
        {
            procedureName = procedureName.ToLowerInvariant();

            if (File.Exists(physicalSchema.ProcedureCatalogFilePath()) == false)
            {
                _core.IO.PutJson(transaction, physicalSchema.ProcedureCatalogFilePath(), new PhysicalProcedureCatalog());
            }

            var procedureCatalog = _core.IO.GetJson<PhysicalProcedureCatalog>(
                transaction, physicalSchema.ProcedureCatalogFilePath(), intendedOperation);

            return procedureCatalog.Collection.FirstOrDefault(o => o.Name.Is(procedureName));
        }

        internal KbQueryResultCollection<TData> ExecuteProcedure(Transaction<TData> transaction, string schemaName, string procedureName)
        {
            throw new NotImplementedException("Reimplement user procedures");
        }
    }
}
