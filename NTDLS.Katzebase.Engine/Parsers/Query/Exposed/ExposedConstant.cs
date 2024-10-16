﻿using static NTDLS.Katzebase.Client.KbConstants;

namespace NTDLS.Katzebase.Engine.Parsers.Query.Exposed
{
    /// <summary>
    /// The "exposed" classes are helpers that allow us to access the ordinal of fields as well as the some of the nester properties.
    /// This one is for fields that are constants.
    /// </summary>
    internal class ExposedConstant<TData> where TData : IStringable
    {
        public int Ordinal { get; private set; }
        public string FieldAlias { get; private set; }
        public TData Value { get; private set; }
        public KbBasicDataType DataType { get; private set; }

        public ExposedConstant(int ordinal, KbBasicDataType dataType, string alias, TData value)
        {
            Ordinal = ordinal;
            DataType = dataType;
            FieldAlias = alias.ToLowerInvariant();
            Value = value;
        }
    }
}
