// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using AzureStorage.Tables;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Common.ApiLibrary.Middleware;
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
using Lykke.Logs.MsSql.Repositories;
using Lykke.Logs.Serilog;
using Microsoft.Extensions.Logging;
using Lykke.Snow.Common.Startup.Log;
using Lykke.Snow.Common.Startup.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;

namespace Lykke.Job.CandlesHistoryWriter
{
    [UsedImplicitly]
    public class Startup
    {
        private IReloadingManager<AppSettings> _mtSettingsManager;
        private IWebHostEnvironment Environment { get; }
        private ILifetimeScope ApplicationContainer { get; set; }
        private IConfigurationRoot Configuration { get; }
        private ILog Log { get; set; }

        private const string ApiVersion = "v1";
        private const string ApiTitle = "Candles History Writer Job";

        public Startup(IWebHostEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("env.json", true)
                .AddSerilogJson(env)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
            Environment = env;
        }

        [UsedImplicitly]
        public void ConfigureServices(IServiceCollection services)
        {
            try
            {
                services
                    .AddControllers()
                    .AddNewtonsoftJson(options =>
                    {
                        options.SerializerSettings.Converters.Add(new StringEnumConverter());
                        options.SerializerSettings.ContractResolver =
                            new Newtonsoft.Json.Serialization.DefaultContractResolver();
                    });

                services.AddSwaggerGen(options =>
                {
                    options.SwaggerDoc(ApiVersion, new OpenApiInfo {Version = ApiVersion, Title = ApiTitle});
                });
                
                _mtSettingsManager = Configuration.LoadSettings<AppSettings>();

                var candlesHistoryWriter = _mtSettingsManager.CurrentValue.CandlesHistoryWriter != null
                    ? _mtSettingsManager.Nested(x => x.CandlesHistoryWriter)
                    : _mtSettingsManager.Nested(x => x.MtCandlesHistoryWriter);
                
                Log = CreateLogWithSlack(Configuration, services, candlesHistoryWriter, 
                    _mtSettingsManager.CurrentValue.SlackNotifications);

                services.AddSingleton<ILoggerFactory>(x => new WebHostLoggerFactory(Log));
                
                services.AddApplicationInsightsTelemetry();
            }
            catch (Exception ex)
            {
                Log?.WriteFatalErrorAsync(nameof(Startup), nameof(ConfigureServices), "", ex).Wait();
                throw;
            }
        }

        [UsedImplicitly]
        public void ConfigureContainer(ContainerBuilder builder)
        {
            var marketType = _mtSettingsManager.CurrentValue.CandlesHistoryWriter != null
                ? MarketType.Spot
                : MarketType.Mt;
            
            var candlesHistoryWriter = _mtSettingsManager.CurrentValue.CandlesHistoryWriter != null
                ? _mtSettingsManager.Nested(x => x.CandlesHistoryWriter)
                : _mtSettingsManager.Nested(x => x.MtCandlesHistoryWriter);
            
            builder.RegisterModule(new JobModule(
                marketType,
                candlesHistoryWriter.CurrentValue,
                _mtSettingsManager.CurrentValue.Assets,
                _mtSettingsManager.CurrentValue.RedisSettings,
                _mtSettingsManager.CurrentValue.MonitoringServiceClient,
                candlesHistoryWriter.Nested(x => x.Db),
                Log));
            
            builder.RegisterModule(new CqrsModule(_mtSettingsManager.CurrentValue.MtCandlesHistoryWriter.Cqrs, Log));
        }

        [UsedImplicitly]
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, IHostApplicationLifetime appLifetime)
        {
            try
            {
                ApplicationContainer = app.ApplicationServices.GetAutofacRoot();
                
                if (env.IsDevelopment())
                {
                    app.UseDeveloperExceptionPage();
                }
                
                app.UseLykkeMiddleware(ex => ErrorResponse.Create("Technical problem"));

                app.UseRouting();
                app.UseEndpoints(endpoints => {
                    endpoints.MapControllers();
                });
                app.UseSwagger(c =>
                {
                    c.PreSerializeFilters.Add((swagger, httpReq) => 
                        swagger.Servers =
                            new List<OpenApiServer>
                            {
                                new OpenApiServer
                                {
                                    Url = $"{httpReq.Scheme}://{httpReq.Host.Value}"
                                }
                            });
                });
                app.UseSwaggerUI(x =>
                {
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

                var monitoringServiceClientSettings =
                    ApplicationContainer.ResolveOptional<MonitoringServiceClientSettings>();

                Program.AppHost.WriteLogs(Environment, Log);

                if (monitoringServiceClientSettings != null &&
                    !string.IsNullOrEmpty(monitoringServiceClientSettings.MonitoringServiceUrl))
                {
                    await AutoRegistrationInMonitoring.RegisterAsync(Configuration,
                        monitoringServiceClientSettings.MonitoringServiceUrl,
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

        private ILog CreateLogWithSlack(IConfiguration configuration, IServiceCollection services,
            IReloadingManager<CandlesHistoryWriterSettings> settings, SlackNotificationsSettings slackSettings)
        {
            const string tableName = "CandlesHistoryWriterServiceLog";
            var consoleLogger = new LogToConsole();
            var aggregateLogger = new AggregateLogger();
            var settingsValue = settings.CurrentValue;
            
            aggregateLogger.AddLog(consoleLogger);
            LykkeLogToAzureSlackNotificationsManager slackNotificationsManager = null;
            if (slackSettings?.AzureQueue?.ConnectionString != null 
                && slackSettings.AzureQueue.QueueName != null)
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

            if (settings.CurrentValue.UseSerilog)
            {
                aggregateLogger.AddLog(new SerilogLogger(typeof(Startup).Assembly, configuration));
            }
            else if (settingsValue.Db.StorageMode == StorageMode.SqlServer)
            {
                aggregateLogger.AddLog(new LogToSql(new SqlLogRepository(tableName,
                    settingsValue.Db.LogsConnectionString)));
            }
            else if (settingsValue.Db.StorageMode == StorageMode.Azure)
            {
                var dbLogConnectionString = settingsValue.Db.LogsConnectionString;

                // Creating azure storage logger, which logs own messages to console log
                if (!string.IsNullOrEmpty(dbLogConnectionString) && !(dbLogConnectionString.StartsWith("${") 
                                                                      && dbLogConnectionString.EndsWith("}")))
                {
                    var persistenceManager = new LykkeLogToAzureStoragePersistenceManager(
                        AzureTableStorage<Logs.LogEntity>.Create(settings.Nested(x => 
                            x.Db.LogsConnectionString), tableName, consoleLogger),
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
