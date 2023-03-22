
using Katzebase.Engine;
using Katzebase.Library;
using System.Diagnostics;

namespace Katzebase.Service
{
    public class Program
    {
        private static Core? _core = null;
        public static Core Core
        {
            get
            {
                if (_core == null)
                {
                    _core = new Core(Configuration);
                }
                return _core;
            }
        }

        private static Settings? _configuration = null;
        public static Settings Configuration
        {
            get
            {
                if (_configuration == null)
                {
                    IConfiguration config = new ConfigurationBuilder()
                                 .AddJsonFile("appsettings.json")
                                 .AddEnvironmentVariables()
                                 .Build();

                    // Get values from the config given their key and their target type.
                    var settings = config.GetRequiredSection("Settings").Get<Library.Settings>();
                    if (settings == null)
                    {
                        throw new Exception("Failed to load settings");
                    }

                    _configuration = new Library.Settings()
                    {
                        BaseAddress = settings.BaseAddress,
                        DataRootPath = settings.DataRootPath.TrimEnd(new char[] { '/', '\\' }),
                        TransactionDataPath = settings.TransactionDataPath.TrimEnd(new char[] { '/', '\\' }),
                        LogDirectory = settings.LogDirectory.TrimEnd(new char[] { '/', '\\' }),
                        FlushLog = settings.FlushLog,
                        AllowIOCaching = settings.AllowIOCaching,
                        AllowDeferredIO = settings.AllowDeferredIO,
                        WriteTraceData = settings.WriteTraceData,
                        CacheScavengeBuffer = settings.CacheScavengeBuffer,
                        CacheScavengeRate = settings.CacheScavengeRate,
                        MaxCacheMemory = settings.MaxCacheMemory,
                        RecordInstanceHealth = settings.RecordInstanceHealth
                    };
                }

                return _configuration;
            }
        }

        public static void Main(string[] args)
        {
            Core.Start();

            // Add services to the container.
            var builder = WebApplication.CreateBuilder(args);
            builder.Services.AddControllers();
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseAuthorization();
            app.MapControllers();
            app.RunAsync(Configuration.BaseAddress);
            //app.RunAsync();

            if (app.Environment.IsDevelopment())
            {
                Process.Start("explorer", $"{Configuration.BaseAddress}swagger/index.html");
            }

            Core.Log.Write($"Listening on {Configuration.BaseAddress}.");
            Core.Log.Write($"Press [enter] to stop.");
            Console.ReadLine();
            Core.Log.Write($"Stopping...");

            app.StopAsync();
            Core.Shutdown();
        }
    }
}
