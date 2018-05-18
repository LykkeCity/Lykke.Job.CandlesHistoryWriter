using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;

namespace Lykke.Job.CandlesHistoryWriter.Models.Migration
{
    /// <summary>
    /// API request model for trades migration routine.
    /// </summary>
    public class TradesMigrationRequestModel
    {
        /// <summary>
        /// The date (and time) wich limits the trades selection (and old candles replacing) from the top. Please, use UTC time zone.
        /// </summary>
        public DateTime? TimestampUpperLimit { get; set; }

        /// <summary>
        /// An array of strings representing asset pair IDs selected for trades migration in the current session.
        /// </summary>
        [Required]
        public string[] AssetPairIds { get; set; }

        public bool CheckupModel(out Dictionary<string, List<string>> modelErrors)
        {
            var result = new Dictionary<string, List<string>>();
            var timeLimitErrors = new List<string>();
            var assetPairsErrors = new List<string>();

            if (TimestampUpperLimit != null &&
                (TimestampUpperLimit <= DateTime.MinValue || TimestampUpperLimit >= DateTime.MaxValue))
                timeLimitErrors.Add($"Date and time should have a consistent value. Actually, it does not: {TimestampUpperLimit:O}.");

            if (AssetPairIds == null ||
                !AssetPairIds.Any())
                assetPairsErrors.Add("Asset pair ID list should not be empty. Actually, it is.");

            if (timeLimitErrors.Any())
                result["TimestampUpperLimit"] = timeLimitErrors;
            if (assetPairsErrors.Any())
                result["AssetPairIds"] = assetPairsErrors;

            if (result.Any())
            {
                modelErrors = result;
                return false;
            }

            modelErrors = null;
            return true;
        }
    }
}
