﻿using Newtonsoft.Json;

namespace Katzebase.Engine.Indexes
{
    [Serializable]
    public class PhysicalIndex
    {
        public List<PhysicalIndexAttribute> Attributes { get; set; } = new List<PhysicalIndexAttribute>();
        public string Name { get; set; } = string.Empty;
        public Guid Id { get; set; }
        public DateTime Created { get; set; }
        public DateTime Modfied { get; set; }
        public bool IsUnique { get; set; } = false;

        [JsonIgnore]
        public string DiskPath { get; set; } = string.Empty;

        public PhysicalIndex()
        {
        }

        public PhysicalIndex Clone()
        {
            return new PhysicalIndex
            {
                Id = Id,
                Name = Name,
                Created = Created,
                Modfied = Modfied,
                IsUnique = IsUnique
            };
        }

        public void AddAttribute(string name)
        {
            AddAttribute(new PhysicalIndexAttribute()
            {
                Field = name
            });
        }
        public void AddAttribute(PhysicalIndexAttribute attribute)
        {
            Attributes.Add(attribute);
        }

        static public PhysicalIndex FromPayload(PublicLibrary.Payloads.KbIndex index)
        {
            var persistIndex = new PhysicalIndex()
            {
                Id = index.Id,
                Name = index.Name,
                Created = index.Created,
                Modfied = index.Modfied,
                IsUnique = index.IsUnique
            };

            foreach (var indexAttribute in index.Attributes)
            {
                persistIndex.AddAttribute(PhysicalIndexAttribute.FromPayload(indexAttribute));
            }

            return persistIndex;
        }

        static public PublicLibrary.Payloads.KbIndex ToPayload(PhysicalIndex index)
        {
            var persistIndex = new PublicLibrary.Payloads.KbIndex()
            {
                Id = index.Id,
                Name = index.Name,
                Created = index.Created,
                Modfied = index.Modfied,
                IsUnique = index.IsUnique
            };

            foreach (var indexAttribute in index.Attributes)
            {
                persistIndex.AddAttribute(PhysicalIndexAttribute.ToPayload(indexAttribute));
            }

            return persistIndex;
        }
    }
}
