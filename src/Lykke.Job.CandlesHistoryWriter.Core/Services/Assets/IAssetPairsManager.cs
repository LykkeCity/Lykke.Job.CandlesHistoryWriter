using System.Threading.Tasks;
using System.Collections.Generic;
using Lykke.Service.Assets.Client.Custom;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Assets
{
    public interface IAssetPairsManager
    {
        Task<IAssetPair> TryGetAssetPairAsync(string assetPairId);
        Task<IAssetPair> TryGetEnabledPairAsync(string assetPairId);
        Task<IEnumerable<IAssetPair>> GetAllEnabledAsync();
    }
}
