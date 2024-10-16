﻿using NTDLS.Katzebase.Client.Types;
using static NTDLS.Katzebase.Client.KbConstants;

namespace NTDLS.Katzebase.Engine.Parsers.Query.SupportingTypes
{
    internal class QueryBatch<TData> : List<PreparedQuery<TData>> where TData : IStringable
    {
        public KbInsensitiveDictionary<ConditionFieldLiteral<TData>> Literals { get; set; } = new();

        public QueryBatch(KbInsensitiveDictionary<ConditionFieldLiteral<TData>> literals)
        {
            Literals = literals;
        }

        public TData? GetLiteralValue(string value)
        {
            if (Literals.TryGetValue(value, out var literal))
            {
                return literal.Value;
            }
            else return value.ParseToT<TData>(EngineCore<TData>.StrCast);
        }

        public TData? GetLiteralValue(string value, out KbBasicDataType outDataType)
        {
            if (Literals.TryGetValue(value, out var literal))
            {
                outDataType = KbBasicDataType.String;
                return literal.Value;
            }

            outDataType = KbBasicDataType.Undefined;
            return value.ParseToT<TData>(EngineCore<TData>.StrCast);
        }
    }
}
