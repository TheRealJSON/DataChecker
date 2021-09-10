using DataCheckerProj.Sampling;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using Moq;
using System.Data;
using static DataCheckerProj.Helpers.SqlQueryBuilder;
using System.Collections.Generic;
using DataCheckerProj.Importers;

namespace DataCheckerTest.Sampling
{
    [TestClass]
    public class SequentialDataSamplerTest
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
            Setup(); //setup mocks needed for instantiation of data importer(s) used by class

            TableReference dataSource = GetFakeTableReference();

            List<Condition> whereConditions = new List<Condition>(); // used as param for SequentialDataSampler constructor(s)
            List<string> columnsToOrderBy = new List<string>();      // used as param for SequentialDataSampler constructor(s)

            whereConditions.Add(new Condition("col_1", "=", 1));    // 1 where condition
            columnsToOrderBy.Add("id");                             // 1 col to order by
            TestConstruction(dataSource, columnsToOrderBy);                     // construct class and verify object state
            TestConstruction(dataSource, columnsToOrderBy, whereConditions);    // construct class and verify object state

            whereConditions.Add(new Condition("col_2", ">", 8));        // 2 where condition
            whereConditions.Add(new Condition("col_3", "=", "hello"));  // 3 where condition
            columnsToOrderBy.Add("created_date");                       // 2 cols to order by
            columnsToOrderBy.Add("version_id");                         // 3 cols to order by
            TestConstruction(dataSource, columnsToOrderBy);                     // construct class and verify object state       
            TestConstruction(dataSource, columnsToOrderBy, whereConditions);    // construct class and verify object state

            whereConditions.Add(new Condition("col_4", "<>", "test"));  // 4 where condition
            whereConditions.Add(new Condition("col_5", "=", "hello"));  // 5 where condition
            columnsToOrderBy.Add("another_col");                        // 4 cols to order by
            columnsToOrderBy.Add("event_id");                           // 5 cols to order by
            TestConstruction(dataSource, columnsToOrderBy);                     // construct class and verify object state
            TestConstruction(dataSource, columnsToOrderBy, whereConditions);    // construct class and verify object state

            // what about without columnsToOrderBy?
        }

        [TestMethod]
        public void TestGetBoundedSequentialDataSampler()
        {
            Setup(); //setup mocks needed for instantiation of data importer(s) used by class

            TestGetBoundedSequentialDataSampler_RecordIdentitiesDifferentStructure();

            /* prepare for and invoke GetBoundedSequentialDataSampler() */
            TableReference sampleDataSource = GetFakeTableReference();
            Dictionary<string, dynamic> lowerboundRecordIdentity = new Dictionary<string, dynamic>();
            Dictionary<string, dynamic> upperboundRecordIdentity = new Dictionary<string, dynamic>();
            string lowerboundID = "W1ll135";
            string upperboundID = "ABC123";
            DateTime lowerboundDate = new DateTime(1990, 2, 14, 11, 34, 4);
            DateTime upperboundDate = new DateTime(1991, 4, 14, 11, 34, 4);

            lowerboundRecordIdentity.Add("id", lowerboundID);
            lowerboundRecordIdentity.Add("created_date", lowerboundDate);

            upperboundRecordIdentity.Add("id", upperboundID);
            upperboundRecordIdentity.Add("created_date", upperboundDate);

            SequentialDataSampler boundedSampler = SequentialDataSampler.GetBoundedSequentialDataSampler(this.connectionMock.Object, sampleDataSource, lowerboundRecordIdentity, upperboundRecordIdentity, null);

            /* create expected sampling query based on preparation */
            Condition lowerboundIDCondition = new Condition("id", ">=", lowerboundID);
            Condition upperboundIDCondition = new Condition("id", "<=", upperboundID);
            Condition lowerboundDateCondition = new Condition("created_date", ">=", lowerboundDate);
            Condition upperboundDateCondition = new Condition("created_date", "<=", upperboundDate);
            List<Condition> whereConditions = new List<Condition>();
            whereConditions.Add(lowerboundIDCondition);
            whereConditions.Add(upperboundIDCondition);
            whereConditions.Add(lowerboundDateCondition);
            whereConditions.Add(upperboundDateCondition);

            string expectedSelectClause = "SELECT * FROM";
            string expectedTableRef = ConstructSqlServerQueryTableReference(sampleDataSource.DatabaseName, sampleDataSource.SchemaName, sampleDataSource.TableName);
            string expectedWhereClause = ConstructSqlServerWhereClause(whereConditions);
            string expectedSamplingQuery = expectedSelectClause + " " + expectedTableRef + " " + expectedWhereClause;

            /* verify GetBoundedSequentialDataSampler() queries data in expected manner */
            Assert.AreEqual(expectedSamplingQuery, boundedSampler.SamplingQuery);       // query should have expected boundary WHERE clauses added
            Assert.AreEqual(100000, boundedSampler.SampleSize);                              // sets correct default sample size
            Assert.IsNull(boundedSampler.LastSampleTaken);                                   // there is no LastSampleTaken until SampleDataSource() is invoked
            Assert.AreEqual(typeof(PostgreSqlImporter), boundedSampler.GetDataReaderType());  // uses correct type of reader

            /* repeat test with orderByCols */
            List<string> columnsToOrderBy = new List<string>();
            columnsToOrderBy.Add("created_date");
            columnsToOrderBy.Add("version_id");

            boundedSampler = SequentialDataSampler.GetBoundedSequentialDataSampler(this.connectionMock.Object, sampleDataSource, lowerboundRecordIdentity, upperboundRecordIdentity, columnsToOrderBy); // this time columnsToOrderBy passed

            string expectedOrderByClause = ConstructSqlServerOrderByClause(columnsToOrderBy);
            expectedSamplingQuery += " " + expectedOrderByClause;

            Assert.AreEqual(expectedSamplingQuery, boundedSampler.SamplingQuery);       // query should have expected boundary WHERE clauses added
            Assert.AreEqual(100000, boundedSampler.SampleSize);                              // sets correct default sample size
            Assert.IsNull(boundedSampler.LastSampleTaken);                                   // there is no LastSampleTaken until SampleDataSource() is invoked
            Assert.AreEqual(typeof(PostgreSqlImporter), boundedSampler.GetDataReaderType());  // uses correct type of reader
        }

        private void TestGetBoundedSequentialDataSampler_RecordIdentitiesDifferentStructure()
        {
            TableReference sampleDataSource = GetFakeTableReference();
            Dictionary<string, dynamic> lowerboundRecordIdentity = new Dictionary<string, dynamic>();
            Dictionary<string, dynamic> upperboundRecordIdentity = new Dictionary<string, dynamic>();

            /* test effects of different column NAMES between recordIdentity params */
            lowerboundRecordIdentity.Add("id", "ABC123");
            lowerboundRecordIdentity.Add("created_date", new DateTime(1990, 2, 14, 11, 34, 4));

            upperboundRecordIdentity.Add("different_id", "ABC123"); // different column name should cause exception
            upperboundRecordIdentity.Add("created_date", new DateTime(1990, 2, 14, 11, 34, 4));

            TestGetBoundedSequentialDataSampler_RecordIdentitiesDifferentStructureExpectation(this.connectionMock.Object, sampleDataSource, lowerboundRecordIdentity, upperboundRecordIdentity);

            lowerboundRecordIdentity.Clear();
            upperboundRecordIdentity.Clear();

            lowerboundRecordIdentity.Add("id", "ABC123");
            lowerboundRecordIdentity.Add("created_date", new DateTime(1990, 2, 14, 11, 34, 4));

            upperboundRecordIdentity.Add("id", "ABC123");
            upperboundRecordIdentity.Add("created_date_diff", new DateTime(1990, 2, 14, 11, 34, 4)); // different column name should cause exception, different col from lost time

            TestGetBoundedSequentialDataSampler_RecordIdentitiesDifferentStructureExpectation(this.connectionMock.Object, sampleDataSource, lowerboundRecordIdentity, upperboundRecordIdentity);

            /* test effects of different column TYPES between recordIdentity params */
            lowerboundRecordIdentity.Clear();
            upperboundRecordIdentity.Clear();

            lowerboundRecordIdentity.Add("event_id", "ABC123");
            lowerboundRecordIdentity.Add("created_date", new DateTime(1990, 2, 14, 11, 34, 4));

            upperboundRecordIdentity.Add("event_id", 12344); // different data type used for value should cause exception
            upperboundRecordIdentity.Add("created_date", new DateTime(1990, 2, 14, 11, 34, 4));

            TestGetBoundedSequentialDataSampler_RecordIdentitiesDifferentStructureExpectation(this.connectionMock.Object, sampleDataSource, lowerboundRecordIdentity, upperboundRecordIdentity);

            lowerboundRecordIdentity.Clear();
            upperboundRecordIdentity.Clear();

            lowerboundRecordIdentity.Add("event_id", "ABC123");
            lowerboundRecordIdentity.Add("date", new DateTime(1990, 2, 14, 11, 34, 4));

            upperboundRecordIdentity.Add("event_id", "ABC123");
            upperboundRecordIdentity.Add("date", new DateTimeOffset()); // different data type used for value should cause exception

            TestGetBoundedSequentialDataSampler_RecordIdentitiesDifferentStructureExpectation(this.connectionMock.Object, sampleDataSource, lowerboundRecordIdentity, upperboundRecordIdentity);

            lowerboundRecordIdentity.Clear();
            upperboundRecordIdentity.Clear();

            lowerboundRecordIdentity.Add("event_id", "ABC123");
            lowerboundRecordIdentity.Add("date", new DateTime(1990, 2, 14, 11, 34, 4));

            upperboundRecordIdentity.Add("event_id", "ABC123");
            upperboundRecordIdentity.Add("date", new DateTime(1990, 2, 14, 11, 34, 4));
        }

        private void TestGetBoundedSequentialDataSampler_RecordIdentitiesDifferentStructureExpectation(IDbConnection conn, TableReference sampleDataSource, Dictionary<string, dynamic> lowerboundRecordIdentity, Dictionary<string, dynamic> upperboundRecordIdentity)
        {
            try
            {
                SequentialDataSampler sampler = SequentialDataSampler.GetBoundedSequentialDataSampler(conn,
                                                                                                        sampleDataSource,
                                                                                                        lowerboundRecordIdentity,
                                                                                                        upperboundRecordIdentity);
                Assert.IsTrue(false); // fail test because exception should have been thrown
            }
            catch (ArgumentException ex)
            {
                Assert.IsTrue(ex.Message.Contains("Provided lowerboundRecordIdentity and upperboundRecordIdentity parameters do not have the same structure"));
            }
        }

        private void TestConstruction(TableReference sampleDataSource, List<string> columnsToOrderBy, List<Condition> whereConditions = null)
        {
            //what happens if TableReference in query doesn't exist?
            //when import object passed query for table that doesn't exist?
            string expectedSelectClause = "SELECT * FROM";
            string expectedTableRef = ConstructSqlServerQueryTableReference(sampleDataSource.DatabaseName, sampleDataSource.SchemaName, sampleDataSource.TableName);
            string expectedOrderByClause = ConstructSqlServerOrderByClause(columnsToOrderBy);
            string expectedSamplingQuery = expectedSelectClause + " " + expectedTableRef;

            SequentialDataSampler sampler;

            if (whereConditions != null) // different constructor used based on whereConditions param
            {
                string expectedWhereClause = ConstructSqlServerWhereClause(whereConditions);
                expectedSamplingQuery += " " + expectedWhereClause + " " + expectedOrderByClause; // expect where conditions to be included in sampling query
                sampler = new SequentialDataSampler(this.connectionMock.Object, sampleDataSource, whereConditions, columnsToOrderBy); // consutrctor WITH where clauses
            }
            else
            {
                expectedSamplingQuery += " " + expectedOrderByClause; // no where clause
                sampler = new SequentialDataSampler(this.connectionMock.Object, sampleDataSource, columnsToOrderBy); // constructor WITHOUT where clauses
            }

            Assert.AreEqual(expectedSamplingQuery, sampler.SamplingQuery);       // constructs sql query properly
            Assert.AreEqual(100000, sampler.SampleSize);                              // sets correct default sample size
            Assert.IsNull(sampler.LastSampleTaken);                                   // there is no LastSampleTaken until SampleDataSource() is invoked
            Assert.AreEqual(typeof(PostgreSqlImporter), sampler.GetDataReaderType());  // uses correct type of reader
        }

        private TableReference GetFakeTableReference()
        {
            string database = "test";       // fake sql db
            string schema = "test_schema";  // fake sql schema
            string table = "test_table";    // fake sql table

            return new TableReference(database, schema, table);
        }

        private string ConstructSqlServerQueryTableReference(string database, string schema, string table)
        {
            return "\"" + database + "\".\"" + schema + "\".\"" + table + "\"";
        }

        private string ConstructSqlServerOrderByClause(List<string> orderByCols)
        {
            string orderByClause = "ORDER BY ";
            foreach (string columnToOrderBy in orderByCols)
            {
                orderByClause += "\"" + columnToOrderBy + "\"" + ",";
            }

            return orderByClause.Substring(0, orderByClause.Length - 1); // remove trailing comma
        }

        private string ConstructSqlServerWhereClause(List<Condition> whereConditions)
        {
            string identifierStart = "\"";
            string identifierEnd = "\"";
            string finalWhereClause = "";

            if (whereConditions != null)
            {
                foreach (Condition whereCondition in whereConditions)
                {
                    string joiningClause = "WHERE ";
                    if (finalWhereClause.Contains("WHERE")) // this is not good way of doing it. What is Schema is called "WHERE"? CHANGE.
                        joiningClause = " AND ";

                    finalWhereClause += $"{joiningClause} {identifierStart}{whereCondition.ColumnName}{identifierEnd} "
                                     + $"{whereCondition.Operation} "
                                     + $"{TranslateValueToSQL(whereCondition.Value)}";
                }
            }

            return finalWhereClause;
        }

        private string TranslateValueToSQL(dynamic value)
        {
            // would use switch case, but not easy to do prior to c# 7 apparently
            if (value.GetType() == typeof(DateTime))
            {
                return "'" + Convert.ToDateTime(value).ToString("yyy-MM-dd HH:mm:ss.ffffff") + "'";
            }
            else if (value.GetType() == typeof(DateTimeOffset))
            {
                return "'" + value.ToString("yyy-MM-dd HH:mm:ss.ffffff") + "'";
            }
            else if (value.GetType() == typeof(string))
            {
                string strValue = Convert.ToString(value);

                if (strValue.Contains("SELECT") && strValue.Contains("FROM")) // if value is a (sub) query
                    return "(" + strValue + ")";

                return "'" + strValue + "'";
            }

            return Convert.ToString(value);
        }
    }
}