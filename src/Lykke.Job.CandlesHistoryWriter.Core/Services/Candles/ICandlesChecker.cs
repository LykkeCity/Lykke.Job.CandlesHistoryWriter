namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    public interface ICandlesChecker
    {
        bool CanHandleAssetPair(string assetPairId);
    }
}
