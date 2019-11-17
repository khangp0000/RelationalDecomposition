package uwdb.discovery.dependency.approximate.common.dependency;

import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.Status;
import uwdb.discovery.dependency.approximate.inference.IInferenceModule;

public interface IDataDependency {
    Status isExact();
    Status isApproximate(double alpha);
    boolean addSpecializations(DependencySet destination);
    boolean addSpecializations(IInferenceModule inferenceModule, DependencySet destination);
    boolean addGeneralizations(DependencySet destination);
    DependencyType getType();    
}
