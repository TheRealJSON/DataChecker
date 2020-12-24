using DataCheckerProj.Importers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCheckerProj.Sampling
{
    public interface IDataSamplingStrategy
    {
        public DataTable SampleDataSource(DataImporter dataSourceReader);
    }
}
