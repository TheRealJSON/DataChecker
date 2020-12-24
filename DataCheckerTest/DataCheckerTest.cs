using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataCheckerProj;
using System.Data;

namespace DataCheckerTest
{
    [TestClass]
    public class DataCheckerTest
    {
        [TestMethod]
        public void testGetData()
        {
            DataChecker checker = new DataChecker();

            DataTable data = checker.getData();

            Assert.IsNotNull(data);
        }
    }
}
