﻿using Katzebase.Library.Payloads;
using Newtonsoft.Json;
using System.Text;

namespace Katzebase.Library.Client.Management
{
    public class Document
    {
        private readonly KatzebaseClient client;

        public Document(KatzebaseClient client)
        {
            this.client = client;
        }

        /// <summary>
        /// Stores a document in the given schema.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="document"></param>
        public void Store(string schema, Payloads.KbDocument document)
        {
            string url = $"api/Document/{client.SessionId}/{schema}/Store";

            var postContent = new StringContent(JsonConvert.SerializeObject(document), Encoding.UTF8, "text/plain");

            using var response = client.Client.PostAsync(url, postContent);
            string resultText = response.Result.Content.ReadAsStringAsync().Result;
            var result = JsonConvert.DeserializeObject<KbActionResponse>(resultText);
            if (result == null || result.Success == false)
            {
                throw new Exception(result == null ? "Invalid response" : result.Message);
            }
        }

        /// <summary>
        /// Deletes a document in the given schema by its Id.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="document"></param>
        public void DeleteById(string schema, Guid id)
        {
            string url = $"api/Document/{client.SessionId}/{schema}/{id}/DeleteById";

            using var response = client.Client.GetAsync(url);
            string resultText = response.Result.Content.ReadAsStringAsync().Result;
            var result = JsonConvert.DeserializeObject<KbActionResponse>(resultText);
            if (result == null || result.Success == false)
            {
                throw new Exception(result == null ? "Invalid response" : result.Message);
            }
        }

        /// <summary>
        /// Lists the documents within a given schema.
        /// </summary>
        /// <param name="schema"></param>
        public List<KbDocumentCatalogItem> Catalog(string schema)
        {
            string url = $"api/Document/{client.SessionId}/{schema}/Catalog";

            using var response = client.Client.GetAsync(url);
            string resultText = response.Result.Content.ReadAsStringAsync().Result;
            return JsonConvert.DeserializeObject<List<KbDocumentCatalogItem>>(resultText) ?? new List<KbDocumentCatalogItem>();
        }
    }
}
