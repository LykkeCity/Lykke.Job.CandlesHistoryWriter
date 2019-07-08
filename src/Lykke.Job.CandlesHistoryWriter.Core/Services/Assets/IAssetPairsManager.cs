// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

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
