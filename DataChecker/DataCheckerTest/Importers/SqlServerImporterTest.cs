using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataCheckerProj;
using System.Data;
using DataCheckerProj.Importers;
using Moq;
using System;

namespace DataCheckerTest.Importers
{
    [TestClass]
    public class SqlServerImporterTest
    {
        private Mock<IDbConnection> connectionMock;
        private Mock<IDbCommand> commandMock;
        private Mock<IDataReader> readerMock;

        private void Setup()
        {
            this.connectionMock = new Mock<IDbConnection>();
            this.commandMock = new Mock<IDbCommand>();
            this.readerMock = new Mock<IDataReader>();

            readerMock.Setup(reader => reader.FieldCount).Returns(4);

            readerMock.Setup(reader => reader.GetName(0)).Returns("First_Col");
            readerMock.Setup(reader => reader.GetName(1)).Returns("Second_Col");
            readerMock.Setup(reader => reader.GetName(2)).Returns("Third_Col");
            readerMock.Setup(reader => reader.GetName(3)).Returns("Fourth_Col");

            readerMock.Setup(reader => reader.GetFieldType(0)).Returns(typeof(string));
            readerMock.Setup(reader => reader.GetFieldType(1)).Returns(typeof(int));
            readerMock.Setup(reader => reader.GetFieldType(2)).Returns(typeof(DateTimeOffset));
            readerMock.Setup(reader => reader.GetFieldType(3)).Returns(typeof(bool));

            commandMock.SetupAllProperties();
            commandMock.Setup(command => command.ExecuteReader()).Returns(readerMock.Object);

            connectionMock.SetupAllProperties();
            connectionMock.Setup(conn => conn.CreateCommand()).Returns(commandMock.Object);
            connectionMock.Object.ConnectionString = "Data Source=empty;Initial Catalog=empty;";
        }

        [TestMethod]
        public void TestConstruction()
        {
            TestConstruction_EmptyOrNullQuery();
            TestConstruction_EmptyOrNullConnString();

            Setup(); // setup default/common aspects of mock objects

            SqlServerImporter importer = new SqlServerImporter(this.connectionMock.Object, "SELECT * FROM TEST");

            /* verify DataSegment (DataTable) is constructed properly based on mocked return results of provided DataReader */
            VerifyTableStructure(importer.GetDataSegment());

            /* verify connection and reader are constructed properly */
            Assert.AreEqual(this.readerMock.Object, importer.GetDataReader());
            Assert.AreEqual(this.connectionMock.Object, importer.GetDbConnection());
        }

        private void TestConstruction_EmptyOrNullQuery()
        {
            Setup(); // setup default/common aspects of mock objects
            SqlServerImporter importer = null;

            try
            {
                importer = new SqlServerImporter(this.connectionMock.Object, ""); // pass empty query
                Assert.IsTrue(false); // fail test, shouldn't get this far
            }
            catch (Exception e)
            {
                Assert.AreEqual(e.GetType(), typeof(ArgumentException));
                Assert.IsTrue(e.Message.Contains("Provided importQuery has null or empty value"));
            }

            Assert.IsNull(importer);

            try
            {
                importer = new SqlServerImporter(this.connectionMock.Object, null); // pass null query
                Assert.IsTrue(false); // fail test, shouldn't get this far
            }
            catch (Exception e)
            {
                Assert.AreEqual(e.GetType(), typeof(ArgumentException));
                Assert.IsTrue(e.Message.Contains("Provided importQuery has null or empty value"));
            }

            Assert.IsNull(importer);
        }
        private void TestConstruction_EmptyOrNullConnString()
        {
            Setup(); // setup default/common aspects of mock objects
            SqlServerImporter importer = null;

            try
            {
                connectionMock.Object.ConnectionString = "";
                importer = new SqlServerImporter(this.connectionMock.Object, "SELECT * FROM fake.fake");
                Assert.IsTrue(false);
            }
            catch (Exception e)
            {
                Assert.AreEqual(e.GetType(), typeof(ArgumentException));
                Assert.IsTrue(e.Message.Contains("Provided DbConnection has null or empty connection string."));
            }

            Assert.IsNull(importer);

            importer = null;
            try
            {
                connectionMock.Object.ConnectionString = null;
                importer = new SqlServerImporter(this.connectionMock.Object, "SELECT * FROM fake.fake");
                Assert.IsTrue(false);
            }
            catch (Exception e)
            {
                Assert.AreEqual(e.GetType(), typeof(ArgumentException));
                Assert.IsTrue(e.Message.Contains("Provided DbConnection has null or empty connection string."));
            }

            Assert.IsNull(importer);
        }

        [TestMethod]
        public void TestNextDataSegment()
        {
            Setup(); // setup default/common aspects of mock objects

            string[] firstColumnValues = new string[5];
            firstColumnValues[0] = "val1";
            firstColumnValues[1] = "val2";
            firstColumnValues[2] = "val3";
            firstColumnValues[3] = "val4";
            firstColumnValues[4] = "val5";

            int[] secondColumnValues = new int[5];
            secondColumnValues[0] = 1;
            secondColumnValues[1] = 2;
            secondColumnValues[2] = 3;
            secondColumnValues[3] = 4;
            secondColumnValues[4] = 5;

            DateTimeOffset[] thirdColumnValues = new DateTimeOffset[5];
            thirdColumnValues[0] = new DateTimeOffset(2008, 1, 1, 1, 1, 1, new TimeSpan(1, 0, 0));
            thirdColumnValues[1] = new DateTimeOffset(2008, 2, 1, 1, 1, 1, new TimeSpan(1, 0, 0));
            thirdColumnValues[2] = new DateTimeOffset(2008, 3, 1, 1, 1, 1, new TimeSpan(1, 0, 0));
            thirdColumnValues[3] = new DateTimeOffset(2008, 4, 1, 1, 1, 1, new TimeSpan(1, 0, 0));
            thirdColumnValues[4] = new DateTimeOffset(2008, 5, 1, 1, 1, 1, new TimeSpan(1, 0, 0));

            bool[] fourthColumnValues = new bool[5];
            fourthColumnValues[0] = true;
            fourthColumnValues[1] = false;
            fourthColumnValues[2] = true;
            fourthColumnValues[3] = false;
            fourthColumnValues[4] = true;

            readerMock.SetupSequence(reader => reader.Read())
                                                            .Returns(true)
                                                            .Returns(true)
                                                            .Returns(true)
                                                            .Returns(true)
                                                            .Returns(true)
                                                            .Returns(false); // reader will return 5 records
            readerMock.SetupSequence(reader => reader[0])
                                                        .Returns(firstColumnValues[0])
                                                        .Returns(firstColumnValues[1])
                                                        .Returns(firstColumnValues[2])
                                                        .Returns(firstColumnValues[3])
                                                        .Returns(firstColumnValues[4]); // reader will return expected values from first column
            readerMock.SetupSequence(reader => reader[1])
                                                        .Returns(secondColumnValues[0])
                                                        .Returns(secondColumnValues[1])
                                                        .Returns(secondColumnValues[2])
                                                        .Returns(secondColumnValues[3])
                                                        .Returns(secondColumnValues[4]); // reader will return expected values from second column
            readerMock.SetupSequence(reader => reader[2])
                                                        .Returns(thirdColumnValues[0])
                                                        .Returns(thirdColumnValues[1])
                                                        .Returns(thirdColumnValues[2])
                                                        .Returns(thirdColumnValues[3])
                                                        .Returns(thirdColumnValues[4]); // reader will return expected values from third column
            readerMock.SetupSequence(reader => reader[3])
                                                       .Returns(fourthColumnValues[0])
                                                       .Returns(fourthColumnValues[1])
                                                       .Returns(fourthColumnValues[2])
                                                       .Returns(fourthColumnValues[3])
                                                       .Returns(fourthColumnValues[4]); // reader will return expected values from fourth column

            SqlServerImporter importer = new SqlServerImporter(connectionMock.Object, "SELECT * FROM TEST");
            DataTable table = importer.NextDataSegment(2); // FIRST LINEAR SEGMENT

            VerifyTableStructure(table);

            /* Verify data/values were correctly read from the data reader and stored in the data segment */
            Assert.AreEqual(2, table.Rows.Count);
            Assert.AreEqual(firstColumnValues[0], table.Rows[0].ItemArray[0]);
            Assert.AreEqual(firstColumnValues[1], table.Rows[1].ItemArray[0]);

            Assert.AreEqual(secondColumnValues[0], table.Rows[0].ItemArray[1]);
            Assert.AreEqual(secondColumnValues[1], table.Rows[1].ItemArray[1]);

            Assert.AreEqual(thirdColumnValues[0], table.Rows[0].ItemArray[2]);
            Assert.AreEqual(thirdColumnValues[1], table.Rows[1].ItemArray[2]);

            Assert.AreEqual(fourthColumnValues[0], table.Rows[0].ItemArray[3]);
            Assert.AreEqual(fourthColumnValues[1], table.Rows[1].ItemArray[3]);

            table = importer.NextDataSegment(2); // NEXT LINEAR SEGMENT
            VerifyTableStructure(table);

            /* Verify data/values were correctly read from the data reader and stored in the data segment */
            Assert.AreEqual(firstColumnValues[2], table.Rows[0].ItemArray[0]); // row array starts at 0 again because it's "next" set of rows, prior not included
            Assert.AreEqual(firstColumnValues[3], table.Rows[1].ItemArray[0]);

            Assert.AreEqual(secondColumnValues[2], table.Rows[0].ItemArray[1]);
            Assert.AreEqual(secondColumnValues[3], table.Rows[1].ItemArray[1]);

            Assert.AreEqual(thirdColumnValues[2], table.Rows[0].ItemArray[2]);
            Assert.AreEqual(thirdColumnValues[3], table.Rows[1].ItemArray[2]);

            Assert.AreEqual(fourthColumnValues[2], table.Rows[0].ItemArray[3]);
            Assert.AreEqual(fourthColumnValues[3], table.Rows[1].ItemArray[3]);

            table = importer.NextDataSegment(2); // NEXT LINEAR SEGMENT, test boundary segmentSize > remaining rows
            VerifyTableStructure(table);

            /* Verify data/values were correctly read from the data reader and stored in the data segment */
            Assert.AreEqual(firstColumnValues[4], table.Rows[0].ItemArray[0]);
            Assert.AreEqual(secondColumnValues[4], table.Rows[0].ItemArray[1]);
            Assert.AreEqual(thirdColumnValues[4], table.Rows[0].ItemArray[2]);
            Assert.AreEqual(fourthColumnValues[4], table.Rows[0].ItemArray[3]);

            /* test case when no data is read */
            Setup();
            readerMock.Setup(reader => reader.Read()).Returns(false);
            importer = new SqlServerImporter(connectionMock.Object, "SELECT * FROM TEST");
            table = importer.NextDataSegment(1);

            Assert.AreEqual(0, table.Rows.Count);
            VerifyTableStructure(table);
        }

        [TestMethod]
        public void TestDispose()
        {
            Setup();

            SqlServerImporter importer = new SqlServerImporter(this.connectionMock.Object, "SELECT * FROM TEST");

            importer.Dispose();

            this.readerMock.Verify(r => r.Close());
            this.connectionMock.Verify(c => c.Close());

            Assert.IsNull(importer.GetDataReader());
            Assert.IsNull(importer.GetDbConnection());
        }

        private void VerifyTableStructure(DataTable table)
        {
            Assert.AreEqual(4, table.Columns.Count);
            Assert.AreEqual("First_Col", table.Columns[0].ColumnName);
            Assert.AreEqual("Second_Col", table.Columns[1].ColumnName);
            Assert.AreEqual("Third_Col", table.Columns[2].ColumnName);
            Assert.AreEqual("Fourth_Col", table.Columns[3].ColumnName);

            Assert.AreEqual(typeof(string), table.Columns[0].DataType);
            Assert.AreEqual(typeof(int), table.Columns[1].DataType);
            Assert.AreEqual(typeof(System.DateTimeOffset), table.Columns[2].DataType);
            Assert.AreEqual(typeof(bool), table.Columns[3].DataType);
        }
    }
}