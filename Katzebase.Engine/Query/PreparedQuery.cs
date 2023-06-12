﻿using Katzebase.Engine.KbLib;
using Katzebase.Engine.Query.Constraints;
using static Katzebase.Engine.KbLib.EngineConstants;

namespace Katzebase.Engine.Query
{
    internal class PreparedQuery
    {
        public List<QuerySchema> Schemas { get; set; } = new();
        public int RowLimit { get; set; }
        public QueryType QueryType { get; set; }
        public Conditions Conditions { get; set; } = new();
        public PrefixedFields SelectFields { get; set; } = new();
        public PrefixedFields GroupBy { get; set; } = new();
        public PrefixedFields OrderBy { get; set; } = new();
        public UpsertKeyValues UpsertKeyValuePairs { get; set; } = new();
        public List<KbNameValuePair> VariableValues { get; set; } = new();
    }
}
