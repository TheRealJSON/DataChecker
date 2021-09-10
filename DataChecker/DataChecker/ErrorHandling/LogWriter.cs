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
        /// <summary>
        ///     Write information to a text file in the specified directory in the format expected of information about an error
        ///     during program execution. 
        /// </summary>
        /// <param name="source">The area of code that the method is invoked from. The source of the error.</param>
        /// <param name="table">
        ///     The name of the (SQL) table being processed when the method is invoked / the error is thrown.
        ///     This value is used to construct the text file name that is written to. 
        /// </param>
        /// <param name="errorDescription">A description of the error that occured.</param>
        /// <param name="dataDescription">A description of the data being processed, that describes application state, when the error occured.</param>
        /// <param name="filepath">The directory that contains, or will contain, the text file to be written to. Make sure it includes a trailing \ to denote a folder.</param>
        public static void LogError(string source, string table, string errorDescription, string dataDescription, string filepath)
        {
            string output = "ERROR|| " + source + " || " + table + " || " + errorDescription + " || " + dataDescription;
            WriteToLog(output, filepath + table + ".txt");
        }

        /// <summary>
        ///     Write information to a text file in the specified directory in the format expected of general information.
        /// </summary>
        /// <param name="source">The area of code that the method is invoked from. The source of the information.</param>
        /// <param name="table">
        ///     The name of the (SQL) table being processed when the method is invoked / the information is logged.
        ///     This value is used to construct the text file name that is written to. 
        /// </param>
        /// <param name="information">The information to be written to the text file.</param>
        /// <param name="filepath">The directory that contains, or will contain, the text file to be written to. Make sure it includes a trailing \ to denote a folder.</param>
        public static void LogInformation(string source, string table, string information, string filepath)
        {
            string output = "INFO|| " + source + " || " + table + " || " + information;
            WriteToLog(output, filepath + table + ".txt");
        }

        /// <summary>
        ///     Performs the I/O operation(s) necessary to write text to a file.
        /// </summary>
        /// <param name="logText">Text to write/output to the provided filepath.</param>
        /// <param name="logPath">The filepath to write/output text to. Must contain a file reference, not just a directory.</param>
        /// <exception cref="IOException">Exception when trying to write to specified log file.</exception>
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