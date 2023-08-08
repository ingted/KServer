﻿using Katzebase.PublicLibrary;
using Katzebase.PublicLibrary.Payloads;
using Newtonsoft.Json;
using ProtoBuf;

namespace Katzebase.Engine.Documents
{
    /// <summary>
    /// This is the page document that is physically written to the disk by virture of being contained in the collection in PhysicalDocumentPage.
    /// </summary>
    [Serializable]
    [ProtoContract]
    public class PhysicalDocument
    {
        [ProtoMember(1)]
        public KBCILookup<string?> Dictonary = new();

        [ProtoMember(2)]
        public DateTime Created { get; set; }

        [ProtoMember(3)]
        public DateTime Modfied { get; set; }

        [ProtoIgnore]
        public int ContentLength => Dictonary.Sum(o => o.Key.Length + (o.Value?.Length ?? 0));

        public PhysicalDocument()
        {
        }

        public PhysicalDocument(string jsonString)
        {
            var dictonary = JsonConvert.DeserializeObject<KBCILookup<string?>>(jsonString);
            KbUtility.EnsureNotNull(dictonary);
            Dictonary = dictonary;
        }

        public PhysicalDocument Clone()
        {
            return new PhysicalDocument
            {
                Dictonary = Dictonary,
                Created = Created,
                Modfied = Modfied
            };
        }
    }
}
