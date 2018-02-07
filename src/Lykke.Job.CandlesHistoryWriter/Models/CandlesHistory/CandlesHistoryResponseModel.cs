using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Lykke.Job.CandlesHistoryWriter.Models.CandlesHistory
{
    public class CandlesHistoryResponseModel
    {
        public IEnumerable<Candle> History { get; set; }

        public class Candle
        {
            [Required]
            public DateTime DateTime { get; set; }

            [Required]
            public double Open { get; set; }

            [Required]
            public double Close { get; set; }

            [Required]
            public double High { get; set; }

            [Required]
            public double Low { get; set; }

            [Required]
            public double TradingVolume { get; set; }

            [Required]
            public double TradingOppositeVolume { get; set; }

            [Required]
            public double LastTradePrice { get; set; }
        }
    }
}
