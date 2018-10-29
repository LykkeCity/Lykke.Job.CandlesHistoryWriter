using System;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Lykke.Logs.Loggers.LykkeSlack;
using Lykke.Sdk;

namespace Lykke.Job.CandlesHistoryWriter
{
    [UsedImplicitly]
    public class Startup
    {
        private readonly LykkeSwaggerOptions _swaggerOptions = new LykkeSwaggerOptions
        {
            ApiTitle = "Candles History Writer Job",
            ApiVersion = "v1"
        };

        [UsedImplicitly]
        public IServiceProvider ConfigureServices(IServiceCollection services)
        {
            return services.BuildServiceProvider<AppSettings>(options =>
            {
                options.SwaggerOptions = _swaggerOptions;

                options.Logs = logs =>
                {
                    logs.AzureTableName = "CandlesHistoryWriterLogs";
                    logs.AzureTableConnectionStringResolver = settings => settings.CandlesHistoryWriter.Db.LogsConnectionString;

                    logs.Extended = extendedLogs =>
                    {
                        extendedLogs.AddAdditionalSlackChannel("Prices", channelOptions =>
                        {
                            channelOptions.MinLogLevel = Microsoft.Extensions.Logging.LogLevel.Information;
                            channelOptions.SpamGuard.DisableGuarding();
                            channelOptions.IncludeHealthNotifications();
                        });
                    };
                };
            });
        }

        [UsedImplicitly]
        public void Configure(IApplicationBuilder app)
        {
            app.UseLykkeConfiguration(options =>
            {
                options.SwaggerOptions = _swaggerOptions;
            });
        }
    }
}
