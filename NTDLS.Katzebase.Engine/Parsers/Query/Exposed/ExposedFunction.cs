﻿using NTDLS.Katzebase.Engine.Parsers.Query.Fields.Expressions;

namespace NTDLS.Katzebase.Engine.Parsers.Query.Exposed
{
    /// <summary>
    /// The "exposed" classes are helpers that allow us to access the ordinal of fields as well as the some of the nester properties.
    /// This one is for fields that have function call dependencies, and their ordinals.
    /// </summary>
    internal class ExposedFunction<TData> where TData : IStringable
    {
        public int Ordinal { get; set; }
        public string FieldAlias { get; set; }

        public IQueryFieldExpression<TData> FieldExpression { get; set; }

        public ExposedFunction(int ordinal, string fieldAlias, IQueryFieldExpression<TData> fieldExpression)
        {
            Ordinal = ordinal;
            FieldAlias = fieldAlias.ToLowerInvariant();
            FieldExpression = fieldExpression;
        }
    }
}
