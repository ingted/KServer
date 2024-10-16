﻿namespace NTDLS.Katzebase.Engine.Parsers.Query.Fields
{
    /// <summary>
    /// Contains a numeric constant, string constant, or the name of a schema.field or just a field name if the schema was not specified.
    /// </summary>
    internal interface IQueryField<TData> where TData : IStringable
    {
        TData Value { get; set; }
        string SchemaAlias { get; }

        public IQueryField<TData> Clone();
    }

    //internal interface IQueryField
    //{
    //    string Value { get; set; }
    //    string SchemaAlias { get; }

    //    public IQueryField Clone();
    //}
}
