﻿namespace NTDLS.Katzebase.Engine.Functions.Aggregate
{
    /// <summary>
    /// A parsed function parameter prototype
    /// </summary>
    public class AggregateFunctionParameterPrototype<TData> where TData : IStringable
    {
        public KbAggregateFunctionParameterType Type { get; private set; }
        public string Name { get; private set; }
        public TData? DefaultValue { get; private set; }
        public bool HasDefault { get; private set; }

        public AggregateFunctionParameterPrototype(KbAggregateFunctionParameterType type, string name)
        {
            Type = type;
            Name = name;
            HasDefault = false;
        }

        public AggregateFunctionParameterPrototype(KbAggregateFunctionParameterType type, string name, TData? defaultValue)
        {
            Type = type;
            Name = name;
            DefaultValue = defaultValue;
            HasDefault = true;
        }
    }
}
