// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.AspNetCore.Mvc.ModelBinding;

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

        public ModelStateDictionary CheckupModel()
        {
            var result = new ModelStateDictionary();

            if (TimestampUpperLimit != null &&
                (TimestampUpperLimit <= DateTime.MinValue || TimestampUpperLimit >= DateTime.MaxValue))
                result.AddModelError("TimestampUpperLimit", $"Date and time should have a consistent value. Actually, it does not: {TimestampUpperLimit:O}.");

            if (AssetPairIds == null ||
                !AssetPairIds.Any())
                result.AddModelError("AssetPairIds", "Asset pair ID list should not be empty. Actually, it is.");

            return result;
        }
    }
}
