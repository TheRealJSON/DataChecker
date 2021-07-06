using DataCheckerProj.ErrorHandling;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System;

namespace DataCheckerTest.ErrorHandling
{
    [TestClass]
    public class LogWriterClassTest
    {
        private string logFile;

        [TestMethod]
        public void TestLogError()
        {
            /* Method doesn't test Timestamp/DateTime.Now functionality 
             * because desired approach to making timestamps testable (provider injection)
             * required dependency inaccessible on deployment environment...resulting in need to change code before deployment
             * https://docs.microsoft.com/en-gb/archive/blogs/ploeh/provider-injection
             * */
            string logFolder = Environment.CurrentDirectory + @"\";
            logFile = logFolder + "table.txt";

            string firstExpectedOutput = "|| ERROR|| source || table || error description || data description";
            string secondExpectedOutput = "|| ERROR|| source2 || table || error description2 || data description2";

            if (File.Exists(logFile))
            {
                Assert.IsTrue(false, "file used for testing " + logFile + " already exists. Cancelling test.");
                return;
            }

            LogWriter.LogError("source", "table", "error description", "data description", logFolder);

            if (!File.Exists(logFile))
            {
                Assert.IsTrue(false, "log file was not created as expected.");
            }
            else
            {
                string[] readText = File.ReadAllLines(logFile);
                string actualOutputWithoutTimestamp = readText[0].Substring(readText[0].IndexOf("||"), readText[0].Length - readText[0].IndexOf("||")); // trim timestamp because untested

                Assert.AreEqual(1, readText.Length); // expect 1 line of text
                Assert.AreEqual(firstExpectedOutput, actualOutputWithoutTimestamp); // expect first line of file to match expectation
            }

            /* test successive writes to same file */
            LogWriter.LogError("source2", "table", "error description2", "data description2", logFolder);

            if (!File.Exists(logFile))
            {
                Assert.IsTrue(false); // fail test because expect this file to be created
            }
            else
            {
                string[] readText = File.ReadAllLines(logFile);
                string actualOutputWithoutTimestamp = readText[0].Substring(readText[0].IndexOf("||"), readText[0].Length - readText[0].IndexOf("||")); // trim timestamp because untested
                string actualOutputWithoutTimestamp2 = readText[1].Substring(readText[1].IndexOf("||"), readText[1].Length - readText[1].IndexOf("||")); // trim timestamp because untested

                Assert.AreEqual(2, readText.Length); // expect 2 line of text and 1 new line
                Assert.AreEqual(firstExpectedOutput, actualOutputWithoutTimestamp); // expect first line of file to match expectation
                Assert.AreEqual(secondExpectedOutput, actualOutputWithoutTimestamp2); // expect second line of file to match expectation
            }

            File.Delete(logFile); // DELETE file after test, cleanup
        }

        [TestCleanup]
        public void PostExecution()
        {
            if (File.Exists(logFile))
            {
                File.Delete(logFile);
            }
        }
    }
}
