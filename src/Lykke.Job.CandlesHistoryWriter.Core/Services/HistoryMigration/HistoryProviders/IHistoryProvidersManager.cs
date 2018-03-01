namespace Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders
{
    public interface IHistoryProvidersManager
    {
        IHistoryProvider GetProvider<TProvider>() where TProvider : IHistoryProvider;
    }
}
