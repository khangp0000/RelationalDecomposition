package uwdb.discovery.dependency.approximate.inference;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

import java.util.Iterator;

public class LatticeOrderingInference implements IInferenceModule {
    protected double alpha;
    protected RelationSchema schema;
    protected DependencySet discoveredDependencies;

    public LatticeOrderingInference(RelationSchema schema, DependencySet discoveredDependencies, double alpha) {
        this.alpha = alpha;
        this.schema = schema;
        this.discoveredDependencies = discoveredDependencies;
    }

    public void infer(DataDependency dependency) {
        Iterator<DataDependency> dependencyIterator = discoveredDependencies.iteratorOfDeterminate(dependency.rhs);
        if(dependencyIterator != null) {
            while (dependencyIterator.hasNext()) {
                DataDependency discoveredDependency = dependencyIterator.next();
                if(dependency.lhs.contains(discoveredDependency.lhs)) {
                    double upperBound = discoveredDependency.measure.getUpperBound();
                    dependency.measure.updateUpperBound(upperBound);
                    break;
                }
            }
        }
    }

    public void doBatchInference(DependencySet inferenceSet) {
        //
    }

    public boolean implies(DataDependency dependency) {
        infer(dependency);
        switch (dependency.isApproximate(alpha)) {
            case TRUE:
                return true;
            default:
                return false;
        }
    }
}
