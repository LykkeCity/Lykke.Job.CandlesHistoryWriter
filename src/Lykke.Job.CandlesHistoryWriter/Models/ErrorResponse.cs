// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace Lykke.Job.CandlesHistoryWriter.Models
{
    public class ErrorResponse
    {
        public Dictionary<string, List<string>> ErrorMessages { get; } = new Dictionary<string, List<string>>();

        public static ErrorResponse Create(ModelStateDictionary modelState)
        {
            var response = new ErrorResponse();

            foreach (var state in modelState)
            {
                var messages = state.Value.Errors
                    .Where(e => !string.IsNullOrWhiteSpace(e.ErrorMessage))
                    .Select(e => e.ErrorMessage)
                    .Concat(state.Value.Errors
                        .Where(e => string.IsNullOrWhiteSpace(e.ErrorMessage))
                        .Select(e => e.Exception.Message))
                    .ToList();

                response.ErrorMessages.Add(state.Key, messages);
            }

            return response;
        }

        public static ErrorResponse Create(string message)
        {
            var response = new ErrorResponse();

            response.ErrorMessages.Add(string.Empty, new List<string> { message });

            return response;
        }

        public static ErrorResponse Create(string field, string message)
        {
            var response = new ErrorResponse();

            response.ErrorMessages.Add(field, new List<string> { message });

            return response;
        }
    }
}
