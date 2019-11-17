package uwdb.discovery.dependency.approximate.search;

import uwdb.discovery.dependency.approximate.common.sets.DependencySet;

public interface ISearchAlgorithm {
    DependencySet getDiscoveredDataDependencies();
    void search();
}
