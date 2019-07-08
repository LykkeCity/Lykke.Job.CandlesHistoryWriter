// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using JetBrains.Annotations;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    [UsedImplicitly]
    public class ErrorManagementSettings
    {
        public bool NotifyOnCantStoreAssetPair { get; set; }
        /// <summary>
        /// Log notification timeout.
        /// </summary>
        public TimeSpan NotifyOnCantStoreAssetPairTimeout { get; set; }
    }
}
