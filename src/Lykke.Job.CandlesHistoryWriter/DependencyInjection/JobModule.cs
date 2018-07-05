﻿using System;
using System.Collections.Generic;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using AzureStorage.Blob;
using AzureStorage.Tables;
using Common.Log;
using Lykke.Common;
using Lykke.Job.CandleHistoryWriter.Repositories.Candles;
using Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.MeFeedHistory;
using Lykke.Job.CandleHistoryWriter.Repositories.Snapshots;
using Lykke.Service.Assets.Client;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders;
using Lykke.Job.CandlesHistoryWriter.Services;
using Lykke.Job.CandlesHistoryWriter.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Lykke.SettingsReader;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.DependencyInjection
{
    public class JobModule : Module
    {
        private readonly IServiceCollection _services;
        private readonly MarketType _marketType;
        private readonly CandlesHistoryWriterSettings _settings;
        private readonly AssetsSettings _assetSettings;
        private readonly RedisSettings _redisSettings;
        private readonly IReloadingManager<Dictionary<string, string>> _candleHistoryAssetConnections;
        private readonly IReloadingManager<DbSettings> _dbSettings;
        private readonly ILog _log;

        public JobModule(
            MarketType marketType,
            CandlesHistoryWriterSettings settings,
            AssetsSettings assetSettings,
            RedisSettings redisSettings,
            IReloadingManager<Dictionary<string, string>> candleHistoryAssetConnections,  
            IReloadingManager<DbSettings> dbSettings,
            ILog log)
        {
            _services = new ServiceCollection();
            _marketType = marketType;
            _settings = settings;
            _assetSettings = assetSettings;
            _redisSettings = redisSettings;
            _candleHistoryAssetConnections = candleHistoryAssetConnections;
            _dbSettings = dbSettings;
            _log = log;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();
                        
            builder.RegisterType<Clock>().As<IClock>();

            RegisterResourceMonitor(builder);

            RegisterRedis(builder);

            RegisterAssets(builder);
            RegisterCandles(builder);

            builder.Populate(_services);
        }

        private void RegisterResourceMonitor(ContainerBuilder builder)
        {
            var monitorSettings = _settings.ResourceMonitor;

            switch (monitorSettings.MonitorMode)
            {
                case ResourceMonitorMode.Off:
                    // Do not register any resource monitor.
                    break;

                case ResourceMonitorMode.AppInsightsOnly:
                    builder.RegisterResourcesMonitoring(_log);
                    break;

                case ResourceMonitorMode.AppInsightsWithLog:
                    builder.RegisterResourcesMonitoringWithLogging(
                        _log,
                        monitorSettings.CpuThreshold,
                        monitorSettings.RamThreshold);
                    break;
            }
        }

        private void RegisterRedis(ContainerBuilder builder)
        {
            builder.Register(c => ConnectionMultiplexer.Connect(_redisSettings.Configuration))
                .As<IConnectionMultiplexer>()
                .SingleInstance();

            builder.Register(c => c.Resolve<IConnectionMultiplexer>().GetDatabase())
                .As<IDatabase>();
        }


        private void RegisterAssets(ContainerBuilder builder)
        {
            _services.RegisterAssetsClient(AssetServiceSettings.Create(
                    new Uri(_assetSettings.ServiceUrl),
                    _settings.AssetsCache.ExpirationPeriod),
                _log);

            builder.RegisterType<AssetPairsManager>()
                .As<IAssetPairsManager>()
                .SingleInstance();
        }

        private void RegisterCandles(ContainerBuilder builder)
        {
            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<HealthLogger>()
                .As<IStartable>()
                .SingleInstance()
                .AutoActivate();

            builder.RegisterType<CandlesHistoryRepository>()
                .As<ICandlesHistoryRepository>()
                .WithParameter(TypedParameter.From(_candleHistoryAssetConnections))
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>()
                .WithParameter(TypedParameter.From(_settings.Migration.MigrationEnabled))
                .SingleInstance();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>()
                .WithParameter(TypedParameter.From(_settings.Migration.MigrationEnabled))
                .SingleInstance();

            builder.RegisterType<SnapshotSerializer>()
                .As<ISnapshotSerializer>()
                .SingleInstance();

            // Now creating a silent -or- logging candles checker object.
            // CandlesChecker -- logs notifications on candles without properly configured connection strings for asset pair using the specified timeout between similar notifications.
            // CandlesHistorySilent -- does not log notifications.
            if (_settings.ErrorManagement.NotifyOnCantStoreAssetPair)
                builder.RegisterType<CandlesChecker>()
                    .As<ICandlesChecker>()
                    .WithParameter(TypedParameter.From(_settings.ErrorManagement.NotifyOnCantStoreAssetPairTimeout))
                    .SingleInstance();
            else
                builder.RegisterType<CandlesCheckerSilent>()
                    .As<ICandlesChecker>()
                    .SingleInstance();

            builder.RegisterType<CandlesSubscriber>()
                .As<ICandlesSubscriber>()
                .WithParameter(TypedParameter.From(_settings.Rabbit.CandlesSubscription))
                .SingleInstance();

            builder.RegisterType<CandlesManager>()
                .As<ICandlesManager>()
                .SingleInstance();
            
            builder.RegisterType<RedisCandlesCacheService>()
                .As<ICandlesCacheService>()
                .WithParameter(TypedParameter.From(_marketType))
                .SingleInstance();

            builder.RegisterType<CandlesPersistenceManager>()
                .As<ICandlesPersistenceManager>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.Persistence));

            builder.RegisterType<CandlesPersistenceQueue>()
                .As<ICandlesPersistenceQueue>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.Persistence));

            builder.RegisterType<QueueMonitor>()
                .As<IStartable>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.QueueMonitor))
                .AutoActivate();

            builder.RegisterType<CandlesCacheInitalizationService>()
                .WithParameter(TypedParameter.From(_settings.HistoryCache.HistoryTicksCacheSize))
                .As<ICandlesCacheInitalizationService>()
                .SingleInstance();

            builder.RegisterType<CandlesPersistenceQueueSnapshotRepository>()
                .As<ICandlesPersistenceQueueSnapshotRepository>()
                .WithParameter(TypedParameter.From(AzureBlobStorage.Create(_dbSettings.ConnectionString(x => x.SnapshotsConnectionString), TimeSpan.FromMinutes(10))));

            builder.RegisterType<RedisCacheCaretaker>()
                .As<IStartable>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_marketType))
                .WithParameter(TypedParameter.From(_settings.HistoryCache.CacheCheckupPeriod))
                .WithParameter(TypedParameter.From(_settings.HistoryCache.HistoryTicksCacheSize))
                .AutoActivate();

            RegisterCandlesMigration(builder);

            RegisterCandlesFiltration(builder);
        }

        private void RegisterCandlesMigration(ContainerBuilder builder)
        {
            if (!string.IsNullOrWhiteSpace(_dbSettings.CurrentValue.FeedHistoryConnectionString))
            {
                builder.RegisterType<FeedHistoryRepository>()
                    .As<IFeedHistoryRepository>()
                    .WithParameter(TypedParameter.From(AzureTableStorage<FeedHistoryEntity>.Create(
                        _dbSettings.ConnectionString(x => x.FeedHistoryConnectionString), 
                        "FeedHistory", 
                        _log, 
                        maxExecutionTimeout: TimeSpan.FromMinutes(5))))
                    .SingleInstance();
            }

            builder.RegisterType<CandlesMigrationManager>()
                .AsSelf()
                .WithParameter(TypedParameter.From(_settings.Migration))
                .SingleInstance();

            builder.RegisterType<CandlesesHistoryMigrationService>()
                .As<ICandlesHistoryMigrationService>()
                .SingleInstance();

            builder.RegisterType<MigrationCandlesGenerator>()
                .AsSelf()
                .SingleInstance();

            builder.RegisterType<EmptyMissedCandlesGenerator>()
                .As<IMissedCandlesGenerator>()
                .SingleInstance();

            builder.RegisterType<HistoryProvidersManager>()
                .As<IHistoryProvidersManager>()
                .SingleInstance();
                
            RegisterHistoryProvider<MeFeedHistoryProvider>(builder);

            builder.RegisterType<TradesMigrationHealthService>()
                .AsSelf()
                .SingleInstance();

            builder.RegisterType<TradesMigrationService>()
                .As<ITradesMigrationService>()
                .WithParameter(TypedParameter.From(_settings.Migration.Trades.SqlTradesDataSourceConnString))
                .WithParameter(TypedParameter.From(_settings.Migration.Trades.SqlQueryBatchSize))
                .WithParameter(TypedParameter.From(_settings.Migration.Trades.SqlCommandTimeout))
                .WithParameter(TypedParameter.From(_settings.Migration.Trades.CandlesPersistenceQueueLimit))
                .SingleInstance();

            builder.RegisterType<TradesMigrationManager>()
                .AsSelf()
                .WithParameter(TypedParameter.From(_settings.Migration.Trades.SqlQueryBatchSize))
                .WithParameter(TypedParameter.From(_settings.Migration.MigrationEnabled))
                .SingleInstance();
        }

        private void RegisterCandlesFiltration(ContainerBuilder builder)
        {
            builder.RegisterType<CandlesFiltrationService>()
                .As<ICandlesFiltrationService>()
                .SingleInstance();

            builder.RegisterType<CandlesFiltrationManager>()
                .AsSelf()
                .SingleInstance();
        }

        private static void RegisterHistoryProvider<TProvider>(ContainerBuilder builder) 
            where TProvider : IHistoryProvider
        {
            builder.RegisterType<TProvider>()
                .Named<IHistoryProvider>(typeof(TProvider).Name);
        }
    }
}
