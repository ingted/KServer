﻿using NTDLS.Katzebase.Client.Exceptions;
using NTDLS.Katzebase.Shared;
using System.Diagnostics.CodeAnalysis;

namespace NTDLS.Katzebase.Engine.Functions.Aggregate
{
    public static class AggregateFunctionCollection<TData> where TData : IStringable
    {
        private static List<AggregateFunction<TData>>? _protypes = null;

        public static List<AggregateFunction<TData>> Prototypes
        {
            get
            {
                if (_protypes == null)
                {
                    throw new KbFatalException("Function prototypes were not initialized.");
                }
                return _protypes;
            }
        }

        public static void Initialize()
        {
            if (_protypes == null)
            {
                _protypes = new List<AggregateFunction<TData>>();

                foreach (var prototype in AggregateFunctionImplementation<TData>.PrototypeStrings)
                {
                    _protypes.Add(AggregateFunction<TData>.Parse(prototype));
                }
            }
        }

        public static bool TryGetFunction(string name, [NotNullWhen(true)] out AggregateFunction<TData>? function)
        {
            function = Prototypes.FirstOrDefault(o => o.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase));
            return function != null;
        }

        public static AggregateFunctionParameterValueCollection<TData> ApplyFunctionPrototype(string functionName, List<TData?> parameters)
        {
            if (_protypes == null)
            {
                throw new KbFatalException("Function prototypes were not initialized.");
            }

            var function = _protypes.FirstOrDefault(o => o.Name.Is(functionName));

            if (function == null)
            {
                throw new KbFunctionException($"Undefined function: [{functionName}].");
            }

            return function.ApplyParameters(parameters);
        }
    }
}
