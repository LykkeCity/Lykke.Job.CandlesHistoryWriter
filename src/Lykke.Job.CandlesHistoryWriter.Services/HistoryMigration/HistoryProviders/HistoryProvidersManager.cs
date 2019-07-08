// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using Autofac;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders
{
    [UsedImplicitly]
    public class HistoryProvidersManager : IHistoryProvidersManager
    {
        private readonly IComponentContext _componentContext;

        public HistoryProvidersManager(IComponentContext componentContext)
        {
            _componentContext = componentContext;
        }

        public IHistoryProvider GetProvider<TProvider>() where TProvider : IHistoryProvider
        {
            return _componentContext.ResolveNamed<IHistoryProvider>(typeof(TProvider).Name);
        }
    }
}
