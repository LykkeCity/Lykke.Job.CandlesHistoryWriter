﻿using System;
using Autofac;
using AzureStorage.Blob;
using AzureStorage.Tables;
using JetBrains.Annotations;
using Lykke.Common;
using Lykke.Common.Log;
using Lykke.Job.CandleHistoryWriter.Repositories.Candles;
using Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.MeFeedHistory;
using Lykke.Job.CandleHistoryWriter.Repositories.Snapshots;
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
using Lykke.Service.Assets.Client;
using Lykke.SettingsReader;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Modules
{
    [UsedImplicitly]
    public class JobModule : Module
    {
        private readonly IReloadingManager<AppSettings> _settings;
        private readonly MarketType _marketType;

        public JobModule(IReloadingManager<AppSettings> settings)
        {
            _settings = settings;
            _marketType = settings.CurrentValue.CandlesHistoryWriter != null 
                ? MarketType.Spot 
                : MarketType.Mt;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<Clock>().As<IClock>();

            RegisterCacheSemaphore(builder);

            RegisterResourceMonitor(builder);

            RegisterRedis(builder);

            RegisterAssets(builder);
            RegisterCandles(builder);
        }

        private void RegisterCacheSemaphore(ContainerBuilder builder)
        {
            builder.RegisterType<CandlesCacheSemaphore>()
                .As<ICandlesCacheSemaphore>()
                .SingleInstance(); // <-- Important
        }

        private void RegisterResourceMonitor(ContainerBuilder builder)
        {
            var monitorSettings = _settings.CurrentValue.CandlesHistoryWriter.ResourceMonitor;

            switch (monitorSettings.MonitorMode)
            {
                case ResourceMonitorMode.Off:
                    // Do not register any resource monitor.
                    break;

                case ResourceMonitorMode.AppInsightsOnly:
                    builder.RegisterResourcesMonitoring();
                    break;

                case ResourceMonitorMode.AppInsightsWithLog:
                    builder.RegisterResourcesMonitoringWithLogging(
                        monitorSettings.CpuThreshold,
                        monitorSettings.RamThreshold);
                    break;
            }
        }

        private void RegisterRedis(ContainerBuilder builder)
        {
            builder.Register(c => ConnectionMultiplexer.Connect(_settings.CurrentValue.RedisSettings.Configuration))
                .As<IConnectionMultiplexer>()
                .SingleInstance();

            builder.Register(c => c.Resolve<IConnectionMultiplexer>().GetDatabase())
                .As<IDatabase>();
        }

        private void RegisterAssets(ContainerBuilder builder)
        {
            builder.RegisterAssetsClient(AssetServiceSettings.Create(
                    new Uri(_settings.CurrentValue.Assets.ServiceUrl),
                    _settings.CurrentValue.Assets.CacheExpirationPeriod));

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
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandleHistoryAssetConnections))
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>()
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration.MigrationEnabled))
                .SingleInstance();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>()
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration.MigrationEnabled))
                .SingleInstance();

            builder.RegisterType<SnapshotSerializer>()
                .As<ISnapshotSerializer>()
                .SingleInstance();

            // Now creating a silent -or- logging candles checker object.
            // CandlesChecker -- logs notifications on candles without properly configured connection strings for asset pair using the specified timeout between similar notifications.
            // CandlesHistorySilent -- does not log notifications.
            if (_settings.CurrentValue.CandlesHistoryWriter.ErrorManagement.NotifyOnCantStoreAssetPair)
                builder.RegisterType<CandlesChecker>()
                    .As<ICandlesChecker>()
                    .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.ErrorManagement.NotifyOnCantStoreAssetPairTimeout))
                    .SingleInstance();
            else
                builder.RegisterType<CandlesCheckerSilent>()
                    .As<ICandlesChecker>()
                    .SingleInstance();

            builder.RegisterType<CandlesSubscriber>()
                .As<ICandlesSubscriber>()
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Rabbit.CandlesSubscription))
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
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Persistence));

            builder.RegisterType<CandlesPersistenceQueue>()
                .As<ICandlesPersistenceQueue>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Persistence));

            builder.RegisterType<QueueMonitor>()
                .As<IStartable>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.QueueMonitor))
                .AutoActivate();

            builder.RegisterType<CandlesCacheInitalizationService>()
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.HistoryCache.HistoryTicksCacheSize))
                .WithParameter(TypedParameter.From(_marketType))
                .As<ICandlesCacheInitalizationService>()
                .SingleInstance();

            builder.RegisterType<CandlesPersistenceQueueSnapshotRepository>()
                .As<ICandlesPersistenceQueueSnapshotRepository>()
                .WithParameter(TypedParameter.From(AzureBlobStorage.Create(_settings.ConnectionString(x => x.CandlesHistoryWriter.Db.SnapshotsConnectionString), TimeSpan.FromMinutes(10))));

            builder.RegisterType<RedisCacheCaretaker>()
                .AsSelf()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_marketType))
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.HistoryCache.CacheCheckupPeriod))
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.HistoryCache.HistoryTicksCacheSize));

            RegisterCandlesMigration(builder);

            RegisterCandlesFiltration(builder);
        }

        private void RegisterCandlesMigration(ContainerBuilder builder)
        {
            if (!string.IsNullOrWhiteSpace(_settings.CurrentValue.CandlesHistoryWriter.Db.FeedHistoryConnectionString))
            {
                builder.Register(ctx =>
                    new FeedHistoryRepository(AzureTableStorage<FeedHistoryEntity>.Create(
                        _settings.ConnectionString(x => x.CandlesHistoryWriter.Db.FeedHistoryConnectionString), 
                        "FeedHistory", 
                        ctx.Resolve<ILogFactory>(), 
                        maxExecutionTimeout: TimeSpan.FromMinutes(5))))
                    .As<IFeedHistoryRepository>()
                    .SingleInstance();
            }

            builder.RegisterType<CandlesMigrationManager>()
                .AsSelf()
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration))
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
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration.Trades.SqlTradesDataSourceConnString))
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration.Trades.SqlQueryBatchSize))
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration.Trades.SqlCommandTimeout))
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration.Trades.CandlesPersistenceQueueLimit))
                .SingleInstance();

            builder.RegisterType<TradesMigrationManager>()
                .AsSelf()
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration.Trades.SqlQueryBatchSize))
                .WithParameter(TypedParameter.From(_settings.CurrentValue.CandlesHistoryWriter.Migration.MigrationEnabled))
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