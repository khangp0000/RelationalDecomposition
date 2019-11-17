package uwdb.discovery.dependency.approximate.inference;

import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;

public interface IInferenceModule {
    void infer(DataDependency dependency);
    void doBatchInference(DependencySet inferenceSet);
    boolean implies(DataDependency dependency);
}
