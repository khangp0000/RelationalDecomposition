package uwdb.discovery.dependency.approximate.entropy;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.dependency.JoinDependency;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IDependencySet;

public interface IDataset {
    RelationSchema getSchema();
    int getNumAttributes();
    long getNumRows();
    double computeEntropy(IAttributeSet subset);
    double computeMVD(DataDependency mvd);
    double computeJD(JoinDependency JD);
    void computeMeasures(IDependencySet dependencies);
    
    long getNumDBScans();
    long getTotalScanTime();
}
