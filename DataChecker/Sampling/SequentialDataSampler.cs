using DataCheckerProj.Importers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCheckerProj.Sampling
{
    public class SequentialDataSampler : DataSampler
    {
        #region properties

        public int lowerboundRowID;
        public int upperboundRowID;

        #endregion

        #region constructors

        public SequentialDataSampler()
        {

        }

        #endregion

        #region method

        public override DataTable SampleDataSource(DataImporter dataSourceReader)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
