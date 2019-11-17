package uwdb.discovery.dependency.approximate.common.sets;

import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;

import java.util.HashMap;
import java.util.Iterator;

public class DependencySet implements IDependencySet {
    public class InternalIterator implements Iterator<DataDependency>
    {
        protected DependencySet set;
        protected boolean hasNext;
        protected Iterator<IAttributeSet> outerIterator;
        protected Iterator<DataDependency> innerIterator;

        public InternalIterator(DependencySet set)
        {
            this.set = set;
            this.outerIterator = set.set.keySet().iterator();
            moveToNextInnerIterator();
        }

        protected void moveToNextInnerIterator()
        {
            hasNext = false;
            if(!outerIterator.hasNext())
            {
                innerIterator = null;
            }

            while(outerIterator.hasNext())
            {
                IAttributeSet nextDeterminant = outerIterator.next();
                innerIterator = set.iteratorOfDeterminant(nextDeterminant);
                if(innerIterator != null && innerIterator.hasNext())
                {
                    hasNext = true;
                    break;
                }
            }
        }

        public boolean hasNext() {
            return hasNext;
        }

        public DataDependency next() {
            DataDependency value = null;
            if(hasNext)
            {
                value = innerIterator.next();
                if(!innerIterator.hasNext())
                {
                    moveToNextInnerIterator();
                }
            }
            return value;
        }

        public void remove() {

        }
    }

    protected long size;
    protected HashMap<IAttributeSet, HashMap<IAttributeSet, DataDependency>> set;
    protected HashMap<IAttributeSet, HashMap<IAttributeSet, DataDependency>> inverseSet;

    public DependencySet()
    {
        this.size = 0;
        this.set = new HashMap<IAttributeSet, HashMap<IAttributeSet, DataDependency>>();
        this.inverseSet = new HashMap<IAttributeSet, HashMap<IAttributeSet, DataDependency>>();
    }

    //accessor functions
    public long size() {
        return size;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public boolean contains(DataDependency dependency)
    {
        if(set.containsKey(dependency.lhs)) {
            HashMap<IAttributeSet, DataDependency> lhsData = set.get(dependency.lhs);
            if(lhsData.containsKey(dependency.rhs)) {
                return true;
            }         
        }
        return false;
    }
    
    public boolean contains(IAttributeSet LHS, IAttributeSet RHS)
    {
        if(set.containsKey(LHS)) {
            HashMap<IAttributeSet, DataDependency> lhsData = set.get(LHS);
            if(lhsData.containsKey(RHS)) {
                return true;
            }
        }
        return false;
    }

    public void add(DataDependency dependency)
    {
        addToDirectMap(dependency);
        addToInverseMap(dependency);
    }

    public void remove(DataDependency dependency) {
        size--;
        remoteFromDirectMap(dependency);
        removeFromInverseMap(dependency);
    }

    //iterator functions
    public Iterator<DataDependency> iterator() {
        return new InternalIterator(this);
    }

    public Iterator<IAttributeSet> determinantIterator() {
        return set.keySet().iterator();
    }

    public Iterator<IAttributeSet> determinateIterator() {
        return inverseSet.keySet().iterator();
    }

    public Iterator<DataDependency> iteratorOfDeterminant(IAttributeSet determinant) {
        Iterator<DataDependency> iterator = null;
        HashMap<IAttributeSet, DataDependency> lhsData = set.get(determinant);
        if(lhsData != null)
        {
            iterator = lhsData.values().iterator();
        }
        return iterator;
    }

    public Iterator<DataDependency> iteratorOfDeterminate(IAttributeSet determinate) {
        Iterator<DataDependency> iterator = null;
        HashMap<IAttributeSet, DataDependency> rhsData = inverseSet.get(determinate);
        if(rhsData != null)
        {
            iterator = rhsData.values().iterator();
        }
        return iterator;
    }


    public void add(DependencySet ds) {
    	Iterator<DataDependency> iterator = ds.iterator();
    	while(iterator.hasNext()) {
    		DataDependency toAdd = iterator.next();
    		addToDirectMap(toAdd);
    		addToInverseMap(toAdd);
    	}
    }
    
    //helper functions
    protected void addToDirectMap(DataDependency dependency)
    {
        HashMap<IAttributeSet, DataDependency> lhsData = set.get(dependency.lhs);
        if(lhsData == null) {
            lhsData = new HashMap<IAttributeSet, DataDependency>();
            set.put(dependency.lhs, lhsData);
        }
        lhsData.put(dependency.rhs, dependency);
    }

    protected void addToInverseMap(DataDependency dependency)
    {
        HashMap<IAttributeSet, DataDependency> rhsData = inverseSet.get(dependency.rhs);
        if(rhsData == null) {
            rhsData = new HashMap<IAttributeSet, DataDependency>();
            inverseSet.put(dependency.rhs, rhsData);
        }
        if(!rhsData.containsKey(dependency.lhs))
        {
            size++;
            rhsData.put(dependency.lhs, dependency);
        }
    }

    protected void remoteFromDirectMap(DataDependency dependency)
    {
        HashMap<IAttributeSet, DataDependency> lhsData = set.get(dependency.lhs);
        if(lhsData != null) {
            lhsData.remove(dependency.rhs);
        }
    }

    protected void removeFromInverseMap(DataDependency dependency)
    {
        HashMap<IAttributeSet, DataDependency> rhsData = inverseSet.get(dependency.rhs);
        if(rhsData != null) {
            rhsData.remove(dependency.lhs);
        }
    }
}
