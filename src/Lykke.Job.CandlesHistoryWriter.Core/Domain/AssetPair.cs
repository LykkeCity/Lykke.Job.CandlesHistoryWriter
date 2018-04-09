using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain
{
    public class AssetPair
    {
        public AssetPair(string id, int accuracy)
        {
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException(nameof(id));
            
            if (accuracy < 0)
                throw new ArgumentException(nameof(accuracy));
            
            Id = id;
            Accuracy = accuracy;
        }

        public string Id { get; }
        public int Accuracy { get; }
    }
}
