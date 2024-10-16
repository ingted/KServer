﻿using NTDLS.Katzebase.Engine.QueryProcessing.Searchers.Intersection;
using System.Security.Cryptography;
using System.Text;

namespace NTDLS.Katzebase.Engine.Functions.Aggregate.Implementations
{
    internal static class AggregateSha512Agg<TData> where TData : IStringable
    {
        public static string Execute(GroupAggregateFunctionParameter<TData> parameters)
        {
            using var sha512 = SHA512.Create();
            foreach (var str in parameters.AggregationValues.OrderBy(o => o))
            {
                var inputBytes = Encoding.UTF8.GetBytes(str.ToT<string>());
                sha512.TransformBlock(inputBytes, 0, inputBytes.Length, null, 0);
            }

            sha512.TransformFinalBlock(Array.Empty<byte>(), 0, 0);

            var sb = new StringBuilder();
            var hashBytes = sha512.Hash ?? Array.Empty<byte>();
            foreach (var b in hashBytes)
            {
                sb.Append(b.ToString("x2"));
            }

            return sb.ToString();
        }
    }
}
