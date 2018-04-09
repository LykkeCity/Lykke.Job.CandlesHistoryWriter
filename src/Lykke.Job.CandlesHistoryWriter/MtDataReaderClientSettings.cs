using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter
{
    public class MtDataReaderClientSettings
    {
        [HttpCheck("/api/isalive")] 
        public string ServiceUrl { get; set; }
        public string ApiKey { get; set; }
    }
}
