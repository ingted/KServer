﻿using Katzebase.Engine.Documents;
using Katzebase.Engine.Query.Searchers.MultiSchema;
using Katzebase.Engine.Query.Searchers.MultiSchema.Mapping;
using Katzebase.Engine.Query.Searchers.SingleSchema;
using Katzebase.Engine.Schemas;
using Katzebase.Engine.Trace;
using Katzebase.Engine.Transactions;
using Katzebase.PublicLibrary;
using Katzebase.PublicLibrary.Exceptions;
using Katzebase.PublicLibrary.Payloads;
using Newtonsoft.Json.Linq;
using static Katzebase.Engine.KbLib.EngineConstants;
using static Katzebase.Engine.Trace.PerformanceTrace;

namespace Katzebase.Engine.Query.Searchers
{
    internal class StaticSearcherMethods
    {
        /// <summary>
        /// Returns a random sample of all docuemnt fields from a schema.
        /// </summary>
        internal static KbQueryResult SampleSchemaDocuments(Core core, Transaction transaction, string schemaName, int rowLimit = -1)
        {
            var result = new KbQueryResult();

            //Lock the schema:
            var schemaMeta = core.Schemas.VirtualPathToMeta(transaction, schemaName, LockOperation.Read);
            if (schemaMeta == null || schemaMeta.Exists == false)
            {
                throw new KbInvalidSchemaException(schemaName);
            }
            Utility.EnsureNotNull(schemaMeta.DiskPath);

            //Lock the document catalog:
            var documentCatalogDiskPath = Path.Combine(schemaMeta.DiskPath, DocumentCatalogFile);
            var documentCatalog = core.IO.GetJson<PersistDocumentCatalog>(transaction, documentCatalogDiskPath, LockOperation.Read);
            Utility.EnsureNotNull(documentCatalog);

            if (documentCatalog.Collection.Count > 0)
            {
                Random random = new Random(Environment.TickCount);

                for (int i = 0; i < rowLimit || rowLimit == 0; i++)
                {
                    int documentIndex = random.Next(0, documentCatalog.Collection.Count - 1);
                    var persistDocumentCatalogItem = documentCatalog.Collection[documentIndex];

                    var persistDocumentDiskPath = Path.Combine(schemaMeta.DiskPath, persistDocumentCatalogItem.FileName);
                    var persistDocument = core.IO.GetJson<PersistDocument>(transaction, persistDocumentDiskPath, LockOperation.Read);
                    Utility.EnsureNotNull(persistDocument);
                    Utility.EnsureNotNull(persistDocument.Content);

                    var jContent = JObject.Parse(persistDocument.Content);

                    if (i == 0)
                    {
                        foreach (var jToken in jContent)
                        {
                            result.Fields.Add(new KbQueryField(jToken.Key));
                        }
                    }

                    if (rowLimit == 0) //We just want a field list.
                    {
                        break;
                    }

                    Utility.EnsureNotNull(persistDocumentCatalogItem.Id);

                    var resultRow = new KbQueryRow();
                    resultRow.AddValue(persistDocumentCatalogItem.Id.ToString());

                    foreach (var field in result.Fields.Skip(1))
                    {
                        jContent.TryGetValue(field.Name, StringComparison.CurrentCultureIgnoreCase, out JToken? jToken);
                        resultRow.AddValue(jToken?.ToString() ?? string.Empty);
                    }

                    result.Rows.Add(resultRow);
                }
            }

            return result;
        }

        /// <summary>
        /// Returns a top list of all docuemnt fields from a schema.
        /// </summary>
        internal static KbQueryResult ListSchemaDocuments(Core core, Transaction transaction, string schemaName, int topCount)
        {
            var result = new KbQueryResult();

            //Lock the schema:
            var schemaMeta = core.Schemas.VirtualPathToMeta(transaction, schemaName, LockOperation.Read);
            if (schemaMeta == null || schemaMeta.Exists == false)
            {
                throw new KbInvalidSchemaException(schemaName);
            }
            Utility.EnsureNotNull(schemaMeta.DiskPath);

            //Lock the document catalog:
            var documentCatalogDiskPath = Path.Combine(schemaMeta.DiskPath, DocumentCatalogFile);
            var documentCatalog = core.IO.GetJson<PersistDocumentCatalog>(transaction, documentCatalogDiskPath, LockOperation.Read);
            Utility.EnsureNotNull(documentCatalog);

            for (int i = 0; i < documentCatalog.Collection.Count && (i < topCount || topCount < 0); i++)
            {
                var persistDocumentCatalogItem = documentCatalog.Collection[i];

                var persistDocumentDiskPath = Path.Combine(schemaMeta.DiskPath, persistDocumentCatalogItem.FileName);
                var persistDocument = core.IO.GetJson<PersistDocument>(transaction, persistDocumentDiskPath, LockOperation.Read);
                Utility.EnsureNotNull(persistDocument);
                Utility.EnsureNotNull(persistDocument.Content);

                var jContent = JObject.Parse(persistDocument.Content);

                if (i == 0)
                {
                    foreach (var jToken in jContent)
                    {
                        result.Fields.Add(new KbQueryField(jToken.Key));
                    }
                }

                Utility.EnsureNotNull(persistDocumentCatalogItem.Id);

                var resultRow = new KbQueryRow();
                foreach (var field in result.Fields)
                {
                    jContent.TryGetValue(field.Name, StringComparison.CurrentCultureIgnoreCase, out JToken? jToken);
                    resultRow.AddValue(jToken?.ToString() ?? string.Empty);
                }

                result.Rows.Add(resultRow);
            }

            return result;
        }

        /// <summary>
        /// Finds all documents using a prepared query.
        /// </summary>
        internal static KbQueryResult FindDocumentsByPreparedQuery(Core core, Transaction transaction, PreparedQuery query)
        {
            var result = new KbQueryResult();

            if (query.SelectFields.Count == 1 && query.SelectFields[0].Field == "*")
            {
                query.SelectFields.Clear();

                var ptSample = transaction.PT?.BeginTrace(PerformanceTraceType.Sampling);
                foreach (var schema in query.Schemas)
                {
                    var sample = SampleSchemaDocuments(core, transaction, schema.Name, 0);

                    foreach (var field in sample.Fields)
                    {
                        if (schema.Prefix != string.Empty)
                        {
                            query.SelectFields.Add(schema.Prefix, field.Name, $"{schema.Prefix}.{field.Name}");
                        }
                        else
                        {
                            query.SelectFields.Add($"{field.Name}");
                        }
                    }
                }
                ptSample?.EndTrace();
            }
            else if (query.SelectFields.Count == 0)
            {
                query.SelectFields.Clear();
                throw new KbGenericException("No fields were selected.");
            }

            if (query.Schemas.Count == 0)
            {
                //I'm not even sure we can get here. Thats an exception, right?
                throw new KbGenericException("No schemas were selected.");
            }
            //If we are querying a single schema, then we just have to apply the conditions in a few threads. Hand off the request and make it so.
            else if (query.Schemas.Count == 1)
            {
                //-------------------------------------------------------------------------------------------------------------
                //This is where we do SSQ stuff (Single Schema Query), e.g. queried with NO joins.
                //-------------------------------------------------------------------------------------------------------------
                var singleSchema = query.Schemas.First();

                var subsetResults = SSQStaticMethods.GetDocumentsByConditions(core, transaction, singleSchema.Name, query);

                foreach (var field in query.SelectFields)
                {
                    result.Fields.Add(new KbQueryField(field.Alias));
                }

                foreach (var subsetResult in subsetResults.Collection)
                {
                    result.Rows.Add(new KbQueryRow(subsetResult.Values));
                }
            }
            //If we are querying multiple schemas then we have to intersect the schemas and apply the conditions. Oh boy.
            else if (query.Schemas.Count > 1)
            {
                //-------------------------------------------------------------------------------------------------------------
                //This is where we do MSQ stuff (Multi Schema Query), e.g. queried WITH joins.
                //-------------------------------------------------------------------------------------------------------------

                var schemaMap = new MSQQuerySchemaMap(core, transaction);

                foreach (var querySchema in query.Schemas)
                {
                    //Lock the schema:
                    var schemaMeta = core.Schemas.VirtualPathToMeta(transaction, querySchema.Name, LockOperation.Read);
                    if (schemaMeta == null || schemaMeta.Exists == false)
                    {
                        throw new KbInvalidSchemaException(querySchema.Name);
                    }
                    Utility.EnsureNotNull(schemaMeta.DiskPath);

                    //Lock the document catalog:
                    var documentCatalogDiskPath = Path.Combine(schemaMeta.DiskPath, DocumentCatalogFile);
                    var documentCatalog = core.IO.GetJson<PersistDocumentCatalog>(transaction, documentCatalogDiskPath, LockOperation.Read);
                    Utility.EnsureNotNull(documentCatalog);

                    schemaMap.Add(querySchema.Prefix, schemaMeta, documentCatalog, querySchema.Conditions);
                }

                /*
                 *  We need to build a generic key/value dataset which is the combined fieldset from each inner joined document.
                 *  Then we use the conditions that were supplied to eliminate results from that dataset.
                */

                var subsetResults = MSQStaticMethods.GetDocumentsByConditions(core, transaction, schemaMap, query);

                foreach (var field in query.SelectFields)
                {
                    result.Fields.Add(new KbQueryField(field.Alias));
                }

                foreach (var subsetResult in subsetResults.Collection)
                {
                    result.Rows.Add(new KbQueryRow(subsetResult.Values));
                }
            }

            return result;
        }

    }
}
