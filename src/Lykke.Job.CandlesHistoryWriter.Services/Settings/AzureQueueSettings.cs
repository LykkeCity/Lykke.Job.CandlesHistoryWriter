namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class AzureQueueSettings
    {
        public string ConnectionString { get; set; }

        public string QueueName { get; set; }
    }
}
