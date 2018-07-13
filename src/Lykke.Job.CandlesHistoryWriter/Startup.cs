using System;
using System.Threading.Tasks;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using AzureStorage.Tables;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Common.ApiLibrary.Middleware;
using Lykke.Common.ApiLibrary.Swagger;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Logs;
using Lykke.Logs.Slack;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.DependencyInjection;
using Lykke.SettingsReader;
using Lykke.SlackNotification.AzureQueue;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Converters;
using Lykke.Job.CandlesHistoryWriter.Models;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using AzureQueueSettings = Lykke.AzureQueueIntegration.AzureQueueSettings;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.MonitoringServiceApiCaller;
using Lykke.Logs.MsSql;

namespace Lykke.Job.CandlesHistoryWriter
{
    [UsedImplicitly]
    public class Startup
    {
        private IContainer ApplicationContainer { get; set; }
        private IConfigurationRoot Configuration { get; }
        private ILog Log { get; set; }
        public static string monitoringServiceUrl { get; set; }

        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("env.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        [UsedImplicitly]
        public IServiceProvider ConfigureServices(IServiceCollection services)
        {
            try
            {
                services.AddMvc()
                    .AddJsonOptions(options =>
                    {
                        options.SerializerSettings.Converters.Add(new StringEnumConverter());
                        options.SerializerSettings.ContractResolver =
                            new Newtonsoft.Json.Serialization.DefaultContractResolver();
                    });

                services.AddSwaggerGen(options =>
                {
                    options.DefaultLykkeConfiguration("v1", "Candles History Writer Job");
                });

                var builder = new ContainerBuilder();
                var settings = Configuration.LoadSettings<AppSettings>();
                var marketType = settings.CurrentValue.CandlesHistoryWriter != null
                    ? MarketType.Spot
                    : MarketType.Mt;

                var candlesHistoryWriter = settings.CurrentValue.CandlesHistoryWriter != null
                    ? settings.Nested(x => x.CandlesHistoryWriter)
                    : settings.Nested(x => x.MtCandlesHistoryWriter);
                var candleHistoryAssetConnection = settings.CurrentValue.CandleHistoryAssetConnections != null
                    ? settings.Nested(x => x.CandleHistoryAssetConnections)
                    : settings.Nested(x => x.MtCandleHistoryAssetConnections);

                if (candlesHistoryWriter.CurrentValue.Db.StorageMode == StorageMode.Azure)
                {
                    Log = CreateLogWithSlack(
                        services,
                        settings.CurrentValue.SlackNotifications,
                        candlesHistoryWriter.ConnectionString(x => x.Db.LogsConnectionString),
                        candlesHistoryWriter.CurrentValue.Db.StorageMode);
                }
                else if (candlesHistoryWriter.CurrentValue.Db.StorageMode == StorageMode.SqlServer)
                {
                    Log = CreateLogWithSlack(
                        services,
                        settings.CurrentValue.SlackNotifications,
                        candlesHistoryWriter.ConnectionString(x => x.Db.SqlConnectionString),
                        candlesHistoryWriter.CurrentValue.Db.StorageMode);
                }


                builder.RegisterModule(new JobModule(
                    marketType,
                    candlesHistoryWriter.CurrentValue,
                    settings.CurrentValue.Assets,
                    settings.CurrentValue.RedisSettings,
                    candleHistoryAssetConnection,
                    candlesHistoryWriter.Nested(x => x.Db),
                    Log));
                builder.Populate(services);
                ApplicationContainer = builder.Build();

                return new AutofacServiceProvider(ApplicationContainer);
            }
            catch (Exception ex)
            {
                Log?.WriteFatalErrorAsync(nameof(Startup), nameof(ConfigureServices), "", ex).Wait();
                throw;
            }
        }

        [UsedImplicitly]
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IApplicationLifetime appLifetime)
        {
            try
            {
                if (env.IsDevelopment())
                {
                    app.UseDeveloperExceptionPage();
                }

                app.UseLykkeMiddleware(nameof(Startup), ex => ErrorResponse.Create("Technical problem"));

                app.UseMvc();
                app.UseSwagger(c =>
                {
                    c.PreSerializeFilters.Add((swagger, httpReq) => swagger.Host = httpReq.Host.Value);
                });
                app.UseSwaggerUI(x =>
                {
                    x.RoutePrefix = "swagger/ui";
                    x.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
                });
                app.UseStaticFiles();

                appLifetime.ApplicationStarted.Register(() => StartApplication().GetAwaiter().GetResult());
                appLifetime.ApplicationStopping.Register(() => StopApplication().GetAwaiter().GetResult());
                appLifetime.ApplicationStopped.Register(() => CleanUp().GetAwaiter().GetResult());
            }
            catch (Exception ex)
            {
                Log?.WriteFatalErrorAsync(nameof(Startup), nameof(Configure), "", ex).Wait();
                throw;
            }
        }

        private async Task StartApplication()
        {
            try
            {
                await ApplicationContainer.Resolve<IStartupManager>().StartAsync();
                monitoringServiceUrl = Configuration.LoadSettings<AppSettings>().CurrentValue.MonitoringServiceClient.MonitoringServiceUrl;

                if (!string.IsNullOrEmpty(monitoringServiceUrl) && monitoringServiceUrl != "n/a")
                {
                    await AutoRegistrationInMonitoring.RegisterAsync(Configuration,
                    monitoringServiceUrl,
                    Log);

                    await Log.WriteMonitorAsync("", "", "Started");
                }

            }
            catch (Exception ex)
            {
                await Log.WriteFatalErrorAsync(nameof(Startup), nameof(StartApplication), "", ex);
                throw;
            }
        }

        private async Task StopApplication()
        {
            try
            {
                await ApplicationContainer.Resolve<IShutdownManager>().ShutdownAsync();
            }
            catch (Exception ex)
            {
                if (Log != null)
                {
                    await Log.WriteFatalErrorAsync(nameof(Startup), nameof(StopApplication), "", ex);
                }
                throw;
            }
        }

        private async Task CleanUp()
        {
            try
            {
                if (Log != null)
                {
                    await Log.WriteMonitorAsync("", "", "Terminating");
                }

                ApplicationContainer.Dispose();
            }
            catch (Exception ex)
            {
                if (Log != null)
                {
                    await Log.WriteFatalErrorAsync(nameof(Startup), nameof(CleanUp), "", ex);
                }
                throw;
            }
        }

        private static ILog CreateLogWithSlack(IServiceCollection services, SlackNotificationsSettings slackSettings, IReloadingManager<string> dbLogConnectionStringManager, StorageMode smode)
        {
            var consoleLogger = new LogToConsole();
            var aggregateLogger = new AggregateLogger();

            aggregateLogger.AddLog(consoleLogger);
            LykkeLogToAzureSlackNotificationsManager slackNotificationsManager = null;
            if (slackSettings != null && slackSettings.AzureQueue != null && slackSettings.AzureQueue.ConnectionString != null
                && slackSettings.AzureQueue.QueueName != null && aggregateLogger != null)
            {

                // Creating slack notification service, which logs own azure queue processing messages to aggregate log
                var slackService = services.UseSlackNotificationsSenderViaAzureQueue(new AzureQueueSettings
                {
                    ConnectionString = slackSettings.AzureQueue.ConnectionString,
                    QueueName = slackSettings.AzureQueue.QueueName
                }, aggregateLogger);

                slackNotificationsManager = new LykkeLogToAzureSlackNotificationsManager(slackService, consoleLogger);

                var logToSlack = LykkeLogToSlack.Create(slackService, "Prices");

                aggregateLogger.AddLog(logToSlack);
            }


            if (smode == StorageMode.SqlServer)
            {
                var sqlLogger = new LogToSql(new LogMsSql("CandlesHistoryWriterServiceLog",
                    dbLogConnectionStringManager.CurrentValue));

                aggregateLogger.AddLog(sqlLogger);
            }
            else if (smode == StorageMode.Azure)
            {
                var dbLogConnectionString = dbLogConnectionStringManager.CurrentValue;

                // Creating azure storage logger, which logs own messages to concole log
                if (!string.IsNullOrEmpty(dbLogConnectionString) && !(dbLogConnectionString.StartsWith("${") && dbLogConnectionString.EndsWith("}")))
                {
                    var persistenceManager = new LykkeLogToAzureStoragePersistenceManager(
                        AzureTableStorage<LogEntity>.Create(dbLogConnectionStringManager, "CandlesHistoryWriterLogs", consoleLogger),
                        consoleLogger);

                    var azureStorageLogger = new LykkeLogToAzureStorage(
                        persistenceManager,
                        slackNotificationsManager,
                        consoleLogger);

                    azureStorageLogger.Start();

                    aggregateLogger.AddLog(azureStorageLogger);
                }
            }

            return aggregateLogger;
        }
    }
}
