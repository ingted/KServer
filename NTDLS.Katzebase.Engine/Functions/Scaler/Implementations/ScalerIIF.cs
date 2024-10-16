﻿namespace NTDLS.Katzebase.Engine.Functions.Scaler.Implementations
{
    internal static class ScalerIIF
    {
        public static string? Execute<TData>(ScalerFunctionParameterValueCollection<TData> function) where TData : IStringable
        {
            return function.Get<bool>("condition") ? function.Get<string>("whenTrue") : function.Get<string>("whenFalse");
        }
    }
}
