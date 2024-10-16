﻿namespace NTDLS.Katzebase.Engine.Parsers.Query.Fields
{
    /// <summary>
    /// Contains a pre-collapsed value.
    /// </summary>
    internal class QueryFieldCollapsedValue<TData> : IQueryField<TData> where TData : IStringable
    {
        public TData Value { get; set; }
        public string SchemaAlias { get; private set; } = string.Empty;

        public QueryFieldCollapsedValue(TData value)
        {
            Value = value;
        }

        public IQueryField<TData> Clone()
        {
            var clone = new QueryFieldCollapsedValue<TData>(Value)
            {
                SchemaAlias = SchemaAlias,
            };

            return clone;
        }
    }
}
