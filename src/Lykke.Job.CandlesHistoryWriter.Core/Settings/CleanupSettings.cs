// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Linq;
using JetBrains.Annotations;
using Lykke.SettingsReader.Attributes;
// ReSharper disable MemberCanBePrivate.Global

namespace Lykke.Job.CandlesHistoryWriter.Core.Settings
{
    [UsedImplicitly]
    public class CleanupSettings
    {
        [Optional] public bool Enabled { get; set; } = true;
        
        [Optional] public int NumberOfTi1 { get; set; }
        [Optional] public int NumberOfTi60 { get; set; }
        [Optional] public int NumberOfTi300 { get; set; }
        [Optional] public int NumberOfTi900 { get; set; }
        [Optional] public int NumberOfTi1800 { get; set; }
        [Optional] public int NumberOfTi3600 { get; set; }
        [Optional] public int NumberOfTi7200 { get; set; }
        [Optional] public int NumberOfTi21600 { get; set; }
        [Optional] public int NumberOfTi43200 { get; set; }
        [Optional] public int NumberOfTi86400 { get; set; }
        [Optional] public int NumberOfTi604800 { get; set; }
        [Optional] public int NumberOfTi3000000 { get; set; }
        [Optional] public int NumberOfTiDefault { get; set; } = 10000;

        public object[] GetFormatParams() => new[]
        {
            NumberOfTi1, NumberOfTi60, NumberOfTi300, NumberOfTi900, NumberOfTi1800, NumberOfTi3600, NumberOfTi7200,
            NumberOfTi21600, NumberOfTi43200, NumberOfTi86400, NumberOfTi604800, NumberOfTi3000000,
        }.Select(x => x == default ? NumberOfTiDefault: x).Cast<object>().ToArray();
    }
}
