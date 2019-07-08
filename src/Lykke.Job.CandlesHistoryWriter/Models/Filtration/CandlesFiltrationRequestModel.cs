// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration;
using System.Collections.Generic;

namespace Lykke.Job.CandlesHistoryWriter.Models.Filtration
{
    public class CandlesFiltrationRequestModel : ICandlesFiltrationRequest
    {
        public string AssetPairId { get; set; }
        public double LimitLow { get; set; }
        public double LimitHigh { get; set; }

        public static IReadOnlyDictionary<string, string> CheckupModel(CandlesFiltrationRequestModel request)
        {
            var result = new Dictionary<string, string>();

            if (request == null)
                result.Add("Request", "The request can not be null.");
            else
            {
                if (string.IsNullOrWhiteSpace(request.AssetPairId))
                    result.Add("AssetPairId", "Can not be null or empty.");

                if (request.LimitHigh < request.LimitLow)
                    result.Add("Limits", "LimitLow must be less than LimitHigh.");
            }

            return result;
        }
    }
}
