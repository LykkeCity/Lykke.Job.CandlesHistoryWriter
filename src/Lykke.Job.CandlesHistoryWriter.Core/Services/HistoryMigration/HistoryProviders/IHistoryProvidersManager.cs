// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders
{
    public interface IHistoryProvidersManager
    {
        IHistoryProvider GetProvider<TProvider>() where TProvider : IHistoryProvider;
    }
}
