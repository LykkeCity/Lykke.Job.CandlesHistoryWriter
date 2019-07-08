// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    public class ConfigurationException : System.Exception
    {
        public ConfigurationException(string message) :
            base(message)
        {
        }
    }
}
