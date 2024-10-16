﻿using NTDLS.Katzebase.Client.Exceptions;
using NTDLS.Katzebase.Client.Types;
using NTDLS.Katzebase.Engine.Documents;

namespace NTDLS.Katzebase.Engine.QueryProcessing.Searchers.Intersection
{
    internal class SchemaIntersectionRow<TData> : List<TData?> where TData :IStringable
    {
        public KbInsensitiveDictionary<DocumentPointer<TData>> SchemaDocumentPointers { get; private set; } = new();

        /// <summary>
        /// The schemas that were used to make up this row.
        /// </summary>
        public HashSet<string> SchemaKeys { get; set; } = new();

        /// <summary>
        /// Auxiliary fields are values that may be used for method calls, sorting, grouping, etc.
        ///     where the fields value may not necessarily be returned directly in the results.
        /// </summary>
        public KbInsensitiveDictionary<TData?> AuxiliaryFields { get; private set; } = new();

        public void InsertValue(string fieldNameForException, int ordinal, TData? value)
        {
            if (Count <= ordinal)
            {
                int difference = ordinal + 1 - Count;
                if (difference > 0)
                {
                    AddRange(new TData[difference]);
                }
            }
            if (this[ordinal] != null)
            {
                throw new KbEngineException($"Ambiguous field [{fieldNameForException}].");
            }

            this[ordinal] = value;
        }

        public void AddSchemaDocumentPointer(string schemaPrefix, DocumentPointer<TData> documentPointer)
        {
            SchemaDocumentPointers.Add(schemaPrefix, documentPointer);
        }

        public SchemaIntersectionRow<TData> Clone()
        {
            var newRow = new SchemaIntersectionRow<TData>
            {
                SchemaKeys = new HashSet<string>(SchemaKeys)
            };

            newRow.AddRange(this);

            newRow.AuxiliaryFields = AuxiliaryFields.Clone();
            newRow.SchemaDocumentPointers = SchemaDocumentPointers.Clone();

            return newRow;
        }
    }
}
