using System.Threading.Tasks;
using System.Collections.Generic;
using Lykke.Service.Assets.Client.Models;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Assets
{
    public interface IAssetPairsManager
    {
        Task<AssetPair> TryGetAssetPairAsync(string assetPairId);
        Task<AssetPair> TryGetEnabledPairAsync(string assetPairId);
        Task<IEnumerable<AssetPair>> GetAllEnabledAsync();
    }
}
