// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class CqrsContextNamesSettings
    {
        [Optional] public string CandlesHistoryWriter { get; set; } = nameof(CandlesHistoryWriter);
        
        [Optional] public string BookKeeper { get; set; } = nameof(BookKeeper);
    }
}
