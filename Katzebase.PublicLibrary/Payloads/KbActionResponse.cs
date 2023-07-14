﻿namespace Katzebase.PublicLibrary.Payloads
{
    public class KbActionResponse : KbIActionResponse
    {
        public bool Success { get; set; }
        public string? ExceptionText { get; set; }
        public KbMetricCollection? Metrics { get; set; }
        public string? Explanation { get; set; }
        public int RowCount { get; set; }

        public KbActionResponse()
        {
        }

        public KbActionResponse(Exception ex)
        {
            ExceptionText = ex.Message;
            Success = false;
        }
    }
}
