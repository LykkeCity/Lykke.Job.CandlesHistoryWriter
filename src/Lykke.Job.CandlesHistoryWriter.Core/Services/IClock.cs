﻿using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IClock
    {
        DateTime UtcNow { get; }
    }
}
