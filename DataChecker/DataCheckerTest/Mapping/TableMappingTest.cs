using DataCheckerProj.Mapping;
using DataCheckerProj.Importers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Reflection;
using System.Collections.Generic;
using Moq;
using System.Data;
using System.Linq;

namespace DataCheckerTest.Mapping
{
    [TestClass]
    public class TableMappingTest
    {
        private Mock<SqlServerImporter> tableMappingImporterMock;
        private DataTable tableMappingData;

        private void SetupSqlServerImporterMock()
        {
            this.tableMappingImporterMock = new Mock<SqlServerImporter>();

            this.tableMappingData = new DataTable();
            tableMappingData.Columns.Add("srcAttribute_1", typeof(string)); // source schema
            tableMappingData.Columns.Add("srcAttribute_2", typeof(string)); // desti  schema
            tableMappingData.Columns.Add("srcAttribute_3", typeof(string)); // source table
            tableMappingData.Columns.Add("srcAttribute_4", typeof(string)); // desti  table
            tableMappingData.Columns.Add("srcAttribute_5", typeof(string)); // source col name
            tableMappingData.Columns.Add("srcAttribute_6", typeof(string)); // dest col name
            tableMappingData.Columns.Add("srcAttribute_7", typeof(string)); // source col type
            tableMappingData.Columns.Add("srcAttribute_8", typeof(string)); // dest col type
            tableMappingData.Columns.Add("srcAttribute_9", typeof(bool));   // isIdentityCol

            tableMappingImporterMock.Setup(imp => imp.GetDataSegment()).Returns(tableMappingData);
        }

        [TestMethod]
        public void TestConstruction()
        {
            TableMapping map = new TableMapping();

            Assert.IsNull(map.SourceSchemaName);
            Assert.IsNull(map.SourceTableName);
            Assert.IsNull(map.DestinationSchemaName);
            Assert.IsNull(map.DestinationTableName);
            Assert.IsNotNull(map.MappedColumns);
            Assert.AreEqual(0, map.MappedColumns.Count);
        }

        [TestMethod]
        public void TestLoadMappingFromSqlServerDatabase()
        {
            SetupSqlServerImporterMock(); // importer used by TableMapping class to read mapping info from Sql Server Database 

            TestLoadMappingFromSqlServerDatabase_InvalidParameters(); // test the logic when provided parameters are invalid

            PropertyInfo[] allPropertyInfos = GetMappingClassPropertyInfo(); // get info about Mapping properties that need to be filled/populated by method we are testing

            DataTable mockedMappingImporterResult = ConstructDataTable(allPropertyInfos.Length, null); // mocked dataSegment will contain data for every mapping property
            Dictionary<string, string> propertyToSrcAttributeMap = ConstructPropertyToSrcAttributeMap(allPropertyInfos, allPropertyInfos.Length); // mapping for every class property

            // add a row with information for one column mapping (and table mapping info)
            List<PropertyInfo> propertyInfoList = allPropertyInfos.OfType<PropertyInfo>().ToList(); // convert to list for easier processing
            FillSqlServerImporterDataSegment(ref mockedMappingImporterResult, propertyToSrcAttributeMap, propertyInfoList, 1);

            /*
             * Setup data importer mock to return mapping info from imaginary database
             */
            DataTable currentDataSegment = mockedMappingImporterResult.Clone(); // when importer class is initialised, DataSegment becomes empty DataTable
            currentDataSegment.Clear();

            tableMappingImporterMock.SetupSequence(imp => imp.NextDataSegment(500)).Returns(() => // ASSUMPTION made here that parameter will be 500 in code...but how to get around this?!
            {
                currentDataSegment = mockedMappingImporterResult; // first call to NextDataSegment() moves to mockedMappingImporterResult
                return mockedMappingImporterResult;
            }).Returns(() =>
            {
                currentDataSegment = new DataTable(); // second call to NextDataSegment() moves to empty dataset because all data consumed
                return new DataTable();  // new DataTable() because it will have no rows
            });

            tableMappingImporterMock.Setup(imp => imp.GetDataSegment()).Returns(() => {
                return currentDataSegment; // what GetDataSegment() returns will depend on order of calls to NextDataSegment() - which, in the mock, sets/updates currentDataSegment
            });

            /*
            * Call method, test properties populated correctly
            */
            TableMapping tblMap = new TableMapping();
            try
            {
                tblMap.LoadMappingFromSqlServerDatabase(this.tableMappingImporterMock.Object, propertyToSrcAttributeMap);
            }
            catch (Exception ex)
            {
                Assert.AreEqual("_msg definitely won't match_so that exception msg can be read", ex.Message);
            }

            Assert.AreEqual(tblMap.SourceSchemaName.Substring(0, tblMap.SourceSchemaName.IndexOf('_')), "SourceSchemaName");
            Assert.AreEqual(tblMap.DestinationSchemaName.Substring(0, tblMap.DestinationSchemaName.IndexOf('_')), "DestinationSchemaName");
            Assert.AreEqual(tblMap.SourceTableName.Substring(0, tblMap.SourceTableName.IndexOf('_')), "SourceTableName");
            Assert.AreEqual(tblMap.DestinationTableName.Substring(0, tblMap.DestinationTableName.IndexOf('_')), "DestinationTableName");
            Assert.AreEqual(tblMap.MappedColumns.Count, 1); // we only had one row in the mocked importer result
            Assert.AreEqual(tblMap.MappedColumns[0].SourceColumnName, "SourceColumnName_0");
            Assert.AreEqual(tblMap.MappedColumns[0].SourceColumnType, "SourceColumnType_0");
            Assert.AreEqual(tblMap.MappedColumns[0].DestinationColumnName, "DestinationColumnName_0");
            Assert.AreEqual(tblMap.MappedColumns[0].DestinationColumnType, "DestinationColumnType_0");
            Assert.AreEqual(tblMap.MappedColumns[0].IsIdentityColumn, false);

            /*
             * repeat test with more iterations
             */
            SetupSqlServerImporterMock();
            currentDataSegment = mockedMappingImporterResult.Clone(); // when importer class is initialised, DataSegment becomes empty DataTable
            currentDataSegment.Clear();

            DataTable firstDataSegment = currentDataSegment.Clone();
            DataTable secondDataSegment = currentDataSegment.Clone();
            DataTable thirdDataSegment = currentDataSegment.Clone();

            FillSqlServerImporterDataSegment(ref firstDataSegment, propertyToSrcAttributeMap, propertyInfoList, 2);
            FillSqlServerImporterDataSegment(ref secondDataSegment, propertyToSrcAttributeMap, propertyInfoList, 4, 2);
            FillSqlServerImporterDataSegment(ref thirdDataSegment, propertyToSrcAttributeMap, propertyInfoList, 10, 6); // ends on row 15

            // slip in a test to verify IsIdentityCol is populated correctly
            string IsIdentityColSourceAttName = propertyToSrcAttributeMap[propertyInfoList.Find(p => p.PropertyType == typeof(bool)).Name];
            int indexOfAlteredBoolCol = firstDataSegment.Rows.Count;
            secondDataSegment.Rows[0][IsIdentityColSourceAttName] = true; // populated as false by default in FillSqlServerImporterDataSegment()

            tableMappingImporterMock.SetupSequence(imp => imp.NextDataSegment(500)).Returns(() => // ASSUMPTION made here that parameter will be 500 in code...but how to get around this?!
            {
                currentDataSegment = firstDataSegment;
                return firstDataSegment;
            }).Returns(() =>
            {
                currentDataSegment = secondDataSegment;
                return secondDataSegment;
            }).Returns(() =>
            {
                currentDataSegment = thirdDataSegment;
                return thirdDataSegment;
            }).Returns(() =>
            {
                currentDataSegment = new DataTable();
                return new DataTable();
            });

            tableMappingImporterMock.Setup(imp => imp.GetDataSegment()).Returns(() => {
                return currentDataSegment; // what GetDataSegment() returns will depend on order of calls to NextDataSegment() - which, in the mock, sets/updates currentDataSegment
            });

            tblMap = new TableMapping();
            try
            {
                tblMap.LoadMappingFromSqlServerDatabase(this.tableMappingImporterMock.Object, propertyToSrcAttributeMap);
            }
            catch (Exception ex)
            {
                Assert.AreEqual("_msg definitely won't match_so that exception msg can be read", ex.Message);
            }

            Assert.AreEqual(tblMap.SourceSchemaName.Substring(0, tblMap.SourceSchemaName.IndexOf('_')), "SourceSchemaName");
            Assert.AreEqual(tblMap.DestinationSchemaName.Substring(0, tblMap.DestinationSchemaName.IndexOf('_')), "DestinationSchemaName");
            Assert.AreEqual(tblMap.SourceTableName.Substring(0, tblMap.SourceTableName.IndexOf('_')), "SourceTableName");
            Assert.AreEqual(tblMap.DestinationTableName.Substring(0, tblMap.DestinationTableName.IndexOf('_')), "DestinationTableName");

            Assert.AreEqual(tblMap.MappedColumns.Count, 16);
            for (int i = 0; i < tblMap.MappedColumns.Count; i++)
            {
                Assert.AreEqual(tblMap.MappedColumns[i].SourceColumnName, "SourceColumnName_" + i);
                Assert.AreEqual(tblMap.MappedColumns[i].SourceColumnType, "SourceColumnType_" + i);
                Assert.AreEqual(tblMap.MappedColumns[i].DestinationColumnName, "DestinationColumnName_" + i);
                Assert.AreEqual(tblMap.MappedColumns[i].DestinationColumnType, "DestinationColumnType_" + i);
                if (i == indexOfAlteredBoolCol)
                {
                    Assert.AreEqual(tblMap.MappedColumns[i].IsIdentityColumn, true);
                }
                else
                {
                    Assert.AreEqual(tblMap.MappedColumns[i].IsIdentityColumn, false);
                }

            }
        }

        [TestMethod]
        public void TestValidate()
        {

        }

        private void TestLoadMappingFromSqlServerDatabase_InvalidParameters()
        {
            SetupSqlServerImporterMock();

            TestLoadMappingFromSqlServerDatabase_InvalidParameters_Nulls();

            TableMapping tblMap = new TableMapping();

            /*
             * test case when propertyToSourceAttributeMap missing property name mappings
             */
            PropertyInfo[] allPropertyInfos = GetMappingClassPropertyInfo();

            Dictionary<string, string> mapMissingPropertyNames = ConstructPropertyToSrcAttributeMap(allPropertyInfos, allPropertyInfos.Length - 1); // construct mappings for all properties EXCEPT LAST ONE
            string nameOfMissingProperty = allPropertyInfos[allPropertyInfos.Length - 1].Name;

            try
            {
                tblMap.LoadMappingFromSqlServerDatabase(this.tableMappingImporterMock.Object, mapMissingPropertyNames);
                Assert.IsTrue(false); // fail test because should not get this far in execution
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual(ex.Message, "Parameter propertyToSourceAttributeMap does not contain mapping for the property " + nameOfMissingProperty);
            }

            /*
             * test case when propertyToSourceAttributeMap with SourceAttribute mappings that don't match source attributes provided by SqlServerImporter
             */
            DataTable mappingData = ConstructDataTable(allPropertyInfos.Length, null); // re-create data to include missing src attribute from previous test
            Dictionary<string, string> propertyToSrcAttributeMap = ConstructPropertyToSrcAttributeMap(allPropertyInfos, allPropertyInfos.Length);
            tableMappingImporterMock.Setup(imp => imp.GetDataSegment()).Returns(mappingData);

            string srcAttributeMissingMapping = propertyToSrcAttributeMap[allPropertyInfos[0].Name]; // propertyToSrcAttributeMap already constructed and valid
            propertyToSrcAttributeMap[allPropertyInfos[0].Name] = "srcAttribute name that doesn't match DataSegment";

            try
            {
                tblMap.LoadMappingFromSqlServerDatabase(this.tableMappingImporterMock.Object, propertyToSrcAttributeMap);
                Assert.IsTrue(false); // fail test because should not get this far in execution
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual(ex.Message, "Parameter propertyToSourceAttributeMap does not contain mapping for the source attribute " + srcAttributeMissingMapping);
            }
        }

        private void TestLoadMappingFromSqlServerDatabase_InvalidParameters_Nulls()
        {
            TableMapping tblMap = new TableMapping();

            string nullImporterMsg = "Parameter mappingImporter cannot be null";
            string nullMapMsg = "Parameter propertyToSourceAttributeMap cannot be null";
            try
            {
                tblMap.LoadMappingFromSqlServerDatabase(null, null);
                Assert.IsTrue(false); // fail test because should not get this far in execution
            }
            catch (ArgumentException ex)
            {
                Assert.IsTrue(ex.Message.Contains(nullImporterMsg) || ex.Message.Contains(nullMapMsg));
            }
            try
            {
                tblMap.LoadMappingFromSqlServerDatabase(null, new Dictionary<string, string>());
                Assert.IsTrue(false); // fail test because should not get this far in execution
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual(ex.Message, nullImporterMsg);
            }
            try
            {
                tblMap.LoadMappingFromSqlServerDatabase(this.tableMappingImporterMock.Object, null);
                Assert.IsTrue(false); // fail test because should not get this far in execution
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual(ex.Message, nullMapMsg);
            }
        }

        private Dictionary<string, string> ConstructPropertyToSrcAttributeMap(PropertyInfo[] propertyNames, int countOfMappings)
        {
            Dictionary<string, string> propertyToSrcAttributeMap = new Dictionary<string, string>();
            for (int i = 0; i < countOfMappings; i++)
            {
                propertyToSrcAttributeMap.Add(propertyNames[i].Name, "srcAttribute_" + i);
            }

            return propertyToSrcAttributeMap;
        }

        private DataTable ConstructDataTable(int colCount, System.Type[] colTypes)
        {
            DataTable table = new DataTable();
            for (int i = 0; i < colCount; i++)
            {
                if (colTypes == null)
                {
                    table.Columns.Add("srcAttribute_" + i, typeof(string));
                }
                else
                {
                    table.Columns.Add("srcAttribute_" + i, colTypes[i]);
                }

            }

            return table;
        }

        private void FillSqlServerImporterDataSegment(ref DataTable dataSegment, Dictionary<string, string> propertyToSrcAttributeMap, List<PropertyInfo> propertyInfoList, int rowCount, int rowStartIndex = 0)
        {
            for (int rowIndex = rowStartIndex; rowIndex < (rowCount + rowStartIndex); rowIndex++)
            {
                DataRow newRow = dataSegment.NewRow();
                foreach (KeyValuePair<string, string> propertyToSrcAtt in propertyToSrcAttributeMap)
                {
                    // Desired end-result is each property stores value equal to the properties name
                    // i.e. importer returns Table with column "srcAttribute_1", storing value "SourceSchemaName",
                    // Dictionary maps property SourceSchemaName to srcAttribute_1, therefore SourceSchemaName is set to "SourceSchemaName" by TableMapping.LoadMappingFromSqlServerDatabase()

                    Type propertyType = propertyInfoList.Find(p => p.Name.Equals(propertyToSrcAtt.Key)).PropertyType;

                    if (propertyType == typeof(bool)) // if current property is supposed to store bool
                    {
                        newRow[propertyToSrcAtt.Value] = false; // assign to bool because importer returning string will not work when mapped to bool property
                    }
                    else if (propertyType == typeof(int))
                    {
                        newRow[propertyToSrcAtt.Value] = rowIndex; // assign to int because importer returning string will not work when mapped to int property
                    }
                    else
                    {
                        newRow[propertyToSrcAtt.Value] = propertyToSrcAtt.Key + "_" + rowIndex; // i.e. row["srcAttribute_1"] = "SourceSchemaName_3" (property name + row index)
                    }
                }

                dataSegment.Rows.Add(newRow); // this is the mapping data that will be Loaded from the sql server DB
            }
        }

        private PropertyInfo[] GetMappingClassPropertyInfo()
        {
            PropertyInfo[] tableMappingPropertyInfos;
            PropertyInfo[] columnMappingPropertyInfos;
            tableMappingPropertyInfos = typeof(TableMapping).GetProperties(); // retrieve names of TableMapping class properties
            columnMappingPropertyInfos = typeof(ColumnMapping).GetProperties().Where(property => property.PropertyType != typeof(List<string>)).ToArray(); // retrieve names of ColumnMapping class properties

            PropertyInfo[] allPropertyInfos = new PropertyInfo[tableMappingPropertyInfos.Length + columnMappingPropertyInfos.Length]; // combine arrays storing propery names into 1
            tableMappingPropertyInfos.CopyTo(allPropertyInfos, 0);
            columnMappingPropertyInfos.CopyTo(allPropertyInfos, tableMappingPropertyInfos.Length);

            return allPropertyInfos;
        }
    }
}