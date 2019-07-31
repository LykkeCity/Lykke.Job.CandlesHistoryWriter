// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Cleanup
{
    public class JobStatus
    {
        public string JobName { get; set; }

        public int StepNumber { get; set; }

        public string StepName { get; set; }

        public string StepStatus { get; set; }

        public DateTime ExecutedAt { get; set; }

        public int ExecutingHours { get; set; }

        public int ExecutingMinutes { get; set; }

        public string Message { get; set; }
    }
}
