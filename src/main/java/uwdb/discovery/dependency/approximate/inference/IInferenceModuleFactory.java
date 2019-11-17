package uwdb.discovery.dependency.approximate.inference;


import uwdb.discovery.dependency.approximate.common.sets.IDependencySet;

public interface IInferenceModuleFactory {
    IInferenceModule getInferenceModule(IDependencySet discoveredDependencies);
}
