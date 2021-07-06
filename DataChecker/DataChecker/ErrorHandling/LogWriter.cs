using System;
using System.IO;
using System.Text.RegularExpressions;

namespace DataCheckerProj.ErrorHandling
{
    /// <summary>
    /// Class used to write to text files in a specified directory in a standardised way.
    /// </summary>
    public static class LogWriter
    {
        public static void LogError(string source, string table, string errorDescription, string dataDescription, string filepath)
        {
            string output = "ERROR|| " + source + " || " + table + " || " + errorDescription + " || " + dataDescription;
            WriteToLog(output, filepath + table + ".txt");
        }

        public static void LogInformation(string source, string table, string information, string filepath)
        {
            string output = "INFO|| " + source + " || " + table + " || " + information;
            WriteToLog(output, filepath + table + ".txt");
        }

        private static void WriteToLog(string logText, string logPath)
        {
            try
            {
                using (StreamWriter outputFile = File.AppendText(logPath))
                {
                    outputFile.WriteLine(DateTime.Now.ToString() + "|| " + Regex.Replace(logText, @"\t|\n|\r", ""));
                }
            }
            catch (IOException ex)
            {
                throw ex;
            }
        }
    }
}