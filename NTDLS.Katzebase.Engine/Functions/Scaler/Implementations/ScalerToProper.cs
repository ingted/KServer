﻿using System.Globalization;

namespace NTDLS.Katzebase.Engine.Functions.Scaler.Implementations
{
    internal static class ScalerToProper
    {
        public static string? Execute<TData>(ScalerFunctionParameterValueCollection<TData> function) where TData : IStringable
        {
            return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(function.Get<string>("text"));
        }
    }
}
