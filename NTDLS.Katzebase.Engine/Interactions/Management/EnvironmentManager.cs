﻿using NTDLS.Katzebase.Client.Exceptions;
using NTDLS.Katzebase.Engine;
using NTDLS.Katzebase.Engine.Atomicity;
using NTDLS.Katzebase.Engine.Interactions.APIHandlers;
using NTDLS.Katzebase.Engine.Interactions.QueryHandlers;
using NTDLS.Katzebase.Shared;
using System.Text.Json;
//using static NTDLS.Katzebase.Engine.Parsers.Query.SupportingTypes.PreparedQuery;
using NTDLS.Katzebase.Engine.Parsers.Query.SupportingTypes;

namespace NTDLS.Katzebase.Engine.Interactions.Management
{
    /// <summary>
    /// Public core class methods for locking, reading, writing and managing tasks related to configuration.
    /// </summary>
    public class EnvironmentManager<TData> where TData : IStringable
    {
        private readonly EngineCore<TData> _core;

        internal EnvironmentQueryHandlers<TData> QueryHandlers { get; private set; }
        public EnvironmentAPIHandlers<TData> APIHandlers { get; private set; }

        internal EnvironmentManager(EngineCore<TData> core)
        {
            _core = core;

            try
            {
                QueryHandlers = new EnvironmentQueryHandlers<TData>(core);
                APIHandlers = new EnvironmentAPIHandlers<TData>(core);
            }
            catch (Exception ex)
            {
                LogManager.Error("Failed to instantiate environment manager.", ex);
                throw;
            }
        }

        static void UpdateSettingProperty<T>(T obj, string propertyName, object newValue)
        {
            Type type = typeof(T);
            var property = type.GetProperty(propertyName);
            if (property != null && property.CanWrite)
            {
                property.SetValue(obj, Convert.ChangeType(newValue, property.PropertyType));
            }
        }

        internal void Alter(Transaction<TData> transaction, Dictionary<PreparedQuery<TData>.QueryAttribute, object> attributes)
        {
            try
            {
                var appSettingsPath = Path.Combine(AppContext.BaseDirectory, "appsettings.json");

                if (!File.Exists(appSettingsPath))
                {
                    throw new KbEngineException($"Could not locate configuration file: [{appSettingsPath}[.");
                }

                string json = File.ReadAllText(appSettingsPath);

                // Parse the JSON into a JsonDocument
                using (JsonDocument document = JsonDocument.Parse(json))
                {
                    var root = document.RootElement;
                    //var settingsElement = root.GetProperty("Settings");
                    var settings = JsonSerializer.Deserialize<KatzebaseSettings>(root.ToString());

                    foreach (var settingElement in root.EnumerateObject())
                    {
                        if (Enum.TryParse(settingElement.Name, true, out PreparedQuery<TData>.QueryAttribute optionType))
                        {
                            if (attributes.TryGetValue(optionType, out var value))
                            {
                                UpdateSettingProperty(settings, settingElement.Name, value); //Save the value in the JSON settings file.
                                UpdateSettingProperty(_core.Settings, settingElement.Name, value); //Save the setting in the live core.
                            }
                        }
                    }

                    string updatedSettingsJson = JsonSerializer.Serialize(settings);
                    using (var file = File.Create(appSettingsPath))
                    {
                        using (var writer = new Utf8JsonWriter(file, new JsonWriterOptions { Indented = true }))
                        {
                            writer.WriteStartObject();
                            foreach (var property in root.EnumerateObject())
                            {
                                if (property.Name == "Settings")
                                {
                                    writer.WritePropertyName(property.Name);
                                    writer.WriteRawValue(updatedSettingsJson);
                                }
                                else
                                {
                                    property.WriteTo(writer);
                                }
                            }
                            writer.WriteEndObject();
                        }
                        file.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.Error($"Failed to alter environment manager for process id {transaction.ProcessId}.", ex);
                throw;
            }
        }

    }
}
