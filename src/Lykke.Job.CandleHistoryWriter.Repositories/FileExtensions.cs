// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;

namespace Lykke.Job.CandleHistoryWriter.Repositories
{
    public static class FileExtensions
    {
        public static string GetFileContent(this string scriptFileName)
        {
            // TODO: Should be exposed via settings
            var debugLocation = $"{Directory.GetCurrentDirectory()}/../{typeof(FileExtensions).Namespace}/Scripts/{scriptFileName}";
            var prodLocation = $"./Scripts/{scriptFileName}";

            var fileContent = ReadFileContent(prodLocation) ?? ReadFileContent(debugLocation);
            if (fileContent == null)
            {
                throw new Exception($"Both prod and debug locations contain no [{scriptFileName}] file. DEBUG: [{debugLocation}]. PROD: [{prodLocation}]");
            }

            return fileContent;
        }
        
        private static string ReadFileContent(string filePath) => 
            File.Exists(filePath) ? File.ReadAllText(filePath) : null;
    }
}
