package uwdb.discovery.dependency.approximate.common.sets;

import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;

import java.util.Iterator;

public interface IDependencySet {

    //accessor functions
    long size();
    boolean isEmpty();
    void add(DataDependency dependency);
    void remove(DataDependency dependency);
    boolean contains(DataDependency dependency);

    //iterator functions
    Iterator<DataDependency> iterator();
    Iterator<IAttributeSet> determinantIterator();
    Iterator<IAttributeSet> determinateIterator();
    Iterator<DataDependency> iteratorOfDeterminant(IAttributeSet determinant);
    Iterator<DataDependency> iteratorOfDeterminate(IAttributeSet determinate);
}
