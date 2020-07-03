// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Text.RegularExpressions;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesShardValidator : ICandlesShardValidator
    {
        private readonly CandlesShardRemoteSettings _candlesShardRemoteSettings;

        public CandlesShardValidator(CandlesShardRemoteSettings candlesShardRemoteSettings)
        {
            _candlesShardRemoteSettings = candlesShardRemoteSettings;
        }

        public bool CanHandle(string assetPairId)
        {
            if (string.IsNullOrEmpty(assetPairId))
                throw new ArgumentNullException(assetPairId);

            if (string.IsNullOrEmpty(_candlesShardRemoteSettings.Pattern))
                return true;

            return Regex.IsMatch(
                assetPairId,
                _candlesShardRemoteSettings.Pattern,
                RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
        }
    }
}
