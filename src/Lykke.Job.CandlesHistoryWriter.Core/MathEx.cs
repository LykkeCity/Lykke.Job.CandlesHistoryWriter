namespace Lykke.Job.CandlesHistoryWriter.Core
{
    public static class MathEx
    {
        /// <summary>
        /// Linear interpolation
        /// </summary>
        public static decimal Lerp(decimal v0, decimal v1, decimal t)
        {
            return (1m - t) * v0 + t * v1;
        }

        /// <summary>
        /// Clamps decimal value by the given boundaries
        /// </summary>
        public static decimal Clamp(decimal value, decimal lowerBound, decimal upperBound)
        {
            if (value < lowerBound)
            {
                return lowerBound;
            }
            if (value > upperBound)
            {
                return upperBound;
            }
            return value;
        }
    }
}
