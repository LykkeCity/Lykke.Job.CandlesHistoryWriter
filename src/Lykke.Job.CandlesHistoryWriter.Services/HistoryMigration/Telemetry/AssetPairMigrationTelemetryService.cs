using System;
using System.Collections.Generic;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.Telemetry
{
    public class AssetPairMigrationTelemetryService
    {
        public class ProgressHistoryItem
        {
            [UsedImplicitly]
            public string Progress { get; }
            [UsedImplicitly]
            public DateTime Moment { get; }

            public ProgressHistoryItem(string progress)
            {
                Progress = progress;
                Moment = DateTime.UtcNow;
            }
        }

        [UsedImplicitly]
        public IReadOnlyList<ProgressHistoryItem> OverallProgressHistory => _overallProgressHistory;
        [UsedImplicitly]
        public DateTime? AskStartDate { get; private set; }
        [UsedImplicitly]
        public DateTime? BidStartDate { get; private set; }
        [UsedImplicitly]
        public DateTime AskEndDate { get; private set; }
        [UsedImplicitly]
        public DateTime BidEndDate { get; private set; }
        [UsedImplicitly]
        public DateTime CurrentAskDate { get; private set; }
        [UsedImplicitly]
        public DateTime CurrentBidDate { get; private set; }
        [UsedImplicitly]
        public DateTime CurrentMidDate { get; private set; }
        
        private readonly List<ProgressHistoryItem> _overallProgressHistory;
        private readonly ILog _log;
        private readonly string _assetPair;

        public AssetPairMigrationTelemetryService(ILog log, string assetPair)
        {
            _log = log;
            _assetPair = assetPair;
            _overallProgressHistory = new List<ProgressHistoryItem>();
        }

        public void UpdateOverallProgress(string progress)
        {
            _log.WriteInfoAsync(nameof(AssetPairMigrationTelemetryService), nameof(UpdateOverallProgress), _assetPair, progress);

            _overallProgressHistory.Add(new ProgressHistoryItem(progress));
        }

        public void UpdateStartDates(DateTime? askStartDate, DateTime? bidStartDate)
        {
            _log.WriteInfoAsync(nameof(AssetPairMigrationTelemetryService), nameof(UpdateStartDates), _assetPair, $"Start dates - ask: {askStartDate:O}, bid: {bidStartDate:O}");

            AskStartDate = askStartDate;
            BidStartDate = bidStartDate;
        }
        
        public void UpdateEndDates(DateTime askEndDate, DateTime bidEndDate)
        {
            _log.WriteInfoAsync(nameof(AssetPairMigrationTelemetryService), nameof(UpdateEndDates), _assetPair, $"End dates - ask: {askEndDate:O}, bid: {bidEndDate:O}");
            
            AskEndDate = askEndDate;
            BidEndDate = bidEndDate;
        }
        
        public void UpdateCurrentHistoryDate(DateTime date, CandlePriceType priceType)
        {
            switch (priceType)
            {
                case CandlePriceType.Bid:
                    CurrentBidDate = date;
                    break;

                case CandlePriceType.Ask:
                    CurrentAskDate = date;
                    break;

                case CandlePriceType.Mid:
                    CurrentMidDate = date;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(priceType), priceType, "Invalid price type");
            }
        }
    }
}
