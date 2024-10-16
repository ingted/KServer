﻿using NTDLS.Katzebase.Engine.QueryProcessing.Searchers.Intersection;
using System.Security.Cryptography;
using System.Text;

namespace NTDLS.Katzebase.Engine.Functions.Aggregate.Implementations
{
    internal static class AggregateSha256Agg<TData> where TData : IStringable
    {
        public static string Execute(GroupAggregateFunctionParameter<TData> parameters)
        {
            using var sha256 = SHA256.Create();
            foreach (var str in parameters.AggregationValues.OrderBy(o => o.ToT<string>()))
            {
                var inputBytes = Encoding.UTF8.GetBytes(str.ToT<string>());
                sha256.TransformBlock(inputBytes, 0, inputBytes.Length, null, 0);
            }

            sha256.TransformFinalBlock(Array.Empty<byte>(), 0, 0);

            var sb = new StringBuilder();
            var hashBytes = sha256.Hash ?? Array.Empty<byte>();
            foreach (var b in hashBytes)
            {
                sb.Append(b.ToString("x2"));
            }

            return sb.ToString();
        }
    }
}
