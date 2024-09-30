﻿using NTDLS.Katzebase.Parsers.Query.Fields.Expressions;

namespace NTDLS.Katzebase.Parsers.Query.Exposed
{
    /// <summary>
    /// The "exposed" classes are helpers that allow us to access the ordinal of fields as well as the some of the nester properties.
    /// This one is for fields that have function call dependencies, and their ordinals.
    /// </summary>
    public class ExposedFunction
    {
        public int Ordinal { get; set; }
        public string FieldAlias { get; set; }

        public IQueryFieldExpression FieldExpression { get; set; }

        public ExposedFunction(int ordinal, string fieldAlias, IQueryFieldExpression fieldExpression)
        {
            Ordinal = ordinal;
            FieldAlias = fieldAlias.ToLowerInvariant();
            FieldExpression = fieldExpression;
        }
    }
}
