using DataCheckerProj.Importers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCheckerProj.Sampling
{
    public abstract class DataSampler : IDataSamplingStrategy
    {
        #region properties

        #endregion

        #region constructors

        #endregion

        #region method

        public abstract DataTable SampleDataSource(DataImporter dataSourceReader);

        #endregion
    }
}
