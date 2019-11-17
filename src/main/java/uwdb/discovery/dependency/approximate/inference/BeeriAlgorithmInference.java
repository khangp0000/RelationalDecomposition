package uwdb.discovery.dependency.approximate.inference;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IDependencySet;

import java.util.*;

/**
 * Implements the vanilla beeri's algorithm
 */
public class BeeriAlgorithmInference implements IInferenceModule {
    protected double alpha;
    protected RelationSchema schema;
    protected IDependencySet discoveredDependencies;

    public BeeriAlgorithmInference(IDependencySet discoveredDependencies, RelationSchema schema, double alpha)
    {
        this.alpha = alpha;
        this.schema = schema;
        this.discoveredDependencies = discoveredDependencies;
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

    public void infer(DataDependency dependency) {
        DependencyBasis dependencyBasis = new DependencyBasis(schema, dependency.lhs);
        dependencyBasis.compute(discoveredDependencies);
        dependencyBasis.infer(dependency);
    }

    public void doBatchInference(DependencySet inferenceSet)
    {
        Iterator<IAttributeSet> iterator = inferenceSet.determinantIterator();
        while (iterator.hasNext())
        {
            IAttributeSet lhs = iterator.next();
            DependencyBasis dependencyBasis = new DependencyBasis(schema, lhs);
            dependencyBasis.compute(discoveredDependencies);

            Iterator<DataDependency> dependencyIterator = inferenceSet.iteratorOfDeterminant(lhs);
            while(dependencyIterator.hasNext())
            {
                DataDependency dep = dependencyIterator.next();
                dependencyBasis.infer(dep);
            }
        }
    }

    public static class Factory implements IInferenceModuleFactory
    {
        protected RelationSchema schema;
        protected double alpha;

        public Factory(RelationSchema schema, double alpha)
        {
            this.schema = schema;
            this.alpha = alpha;
        }

        public IInferenceModule getInferenceModule(IDependencySet discoveredDependencies) {
            return new BeeriAlgorithmInference(discoveredDependencies, schema, alpha);
        }
    }
}
