// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IClock
    {
        DateTime UtcNow { get; }
    }
}
