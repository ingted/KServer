﻿using NTDLS.Katzebase.Engine.Atomicity;
using NTDLS.Katzebase.Engine.Documents;
using NTDLS.Katzebase.Engine.Indexes.Matching;
using NTDLS.Katzebase.Engine.Parsers.Query.WhereAndJoinConditions;
using NTDLS.Katzebase.Engine.Schemas;

namespace NTDLS.Katzebase.Engine.QueryProcessing.Searchers.Mapping
{
    /// <summary>
    /// This class maps the schema and documents to a query supplied schema alias.
    /// </summary>
    internal class QuerySchemaMapItem<TData> where TData :IStringable
    {
        public string Prefix { get; private set; }
        public PhysicalSchema<TData> PhysicalSchema { get; private set; }
        public PhysicalDocumentPageCatalog DocumentPageCatalog { get; private set; }
        public ConditionCollection<TData>? Conditions { get; private set; }
        public IndexingConditionOptimization<TData>? Optimization { get; private set; }

        public QuerySchemaMapItem(EngineCore<TData> core, Transaction<TData> transaction, QuerySchemaMap<TData> schemaMap, PhysicalSchema<TData> physicalSchema,
            PhysicalDocumentPageCatalog documentPageCatalog, ConditionCollection<TData>? conditions, string prefix)
        {
            Prefix = prefix;
            PhysicalSchema = physicalSchema;
            DocumentPageCatalog = documentPageCatalog;
            Conditions = conditions;

            if (conditions != null)
            {
                Optimization = IndexingConditionOptimization<TData>.BuildTree(core, transaction, schemaMap.Query, physicalSchema, conditions, prefix);
            }
        }
    }
}
