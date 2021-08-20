using DataCheckerProj.Mapping;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DataCheckerTest.Mapping
{
    [TestClass]
    public class ColumnMappingTest
    {
        [TestMethod]
        public void TestConstruction()
        {
            int mappingID = 1;
            string srcCol = "source_col";
            string srcType = "srcType";
            string dstCol = "destination_col";
            string dstType = "dstType";
            bool isIdentity = false;
            bool isOrderBy = false;

            ColumnMapping colMap = new ColumnMapping(mappingID, srcCol, srcType, dstCol, dstType, isIdentity, isOrderBy);

            Assert.AreEqual(mappingID, colMap.MappingID);
            Assert.AreEqual(srcCol, colMap.SourceColumnName);
            Assert.AreEqual(srcType, colMap.SourceColumnType);
            Assert.AreEqual(dstCol, colMap.DestinationColumnName);
            Assert.AreEqual(dstType, colMap.DestinationColumnType);
            Assert.AreEqual(isIdentity, colMap.IsIdentityColumn);
        }
    }
}
