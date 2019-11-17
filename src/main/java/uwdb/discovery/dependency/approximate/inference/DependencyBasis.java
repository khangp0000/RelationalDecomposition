package uwdb.discovery.dependency.approximate.inference;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IDependencySet;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class DependencyBasis {

    protected IAttributeSet X;
    protected int numAttributes;
    protected RelationSchema schema;
    protected HashSet<IAttributeSet> basis;
    protected HashMap<IAttributeSet, Double> bounds;

    public DependencyBasis(RelationSchema schema, IAttributeSet lhs)
    {
        this.X = lhs;
        this.schema = schema;
        this.numAttributes = lhs.length();
        this.basis = new HashSet<IAttributeSet>();
        this.bounds = new HashMap<IAttributeSet, Double>();
        initialize();
    }

    protected void initialize()
    {
        basis.add(X);
        bounds.put(X, 0.0);

        IAttributeSet complement = X.complement();
        basis.add(complement);
        bounds.put(complement, 0.0);
    }

    protected void clear()
    {
        basis.clear();
        bounds.clear();
        initialize();
    }

    public void compute(IDependencySet discoveredDependencies)
    {
        HashSet<IAttributeSet> addSet = new HashSet<IAttributeSet>();
        HashSet<IAttributeSet> removeSet = new HashSet<IAttributeSet>();

        boolean converged = false;
        while(!converged)
        {
            boolean split = false;
            Iterator<IAttributeSet> pieceIterator = basis.iterator();
            while (pieceIterator.hasNext())
            {
                //For X ->-> b, we need to find all those S ->-> T such that
                // S \int b = \empty and T \int b and T \setminus b is non-empty
                IAttributeSet b = pieceIterator.next();

                Iterator<DataDependency> dependencyIterator = discoveredDependencies.iterator();
                while (dependencyIterator.hasNext())
                {
                    DataDependency dep = dependencyIterator.next();
                    IAttributeSet S = dep.lhs;
                    IAttributeSet T = dep.rhs;

                    IAttributeSet T_int_b = T.intersect(b);
                    IAttributeSet T_minus_b = T.minus(b);
                    IAttributeSet S_int_b = S.intersect(b);
                    IAttributeSet b_minus_T = b.minus(T);


                    if(S_int_b.isEmpty() && !(T_int_b.isEmpty() && T_minus_b.isEmpty() && b_minus_T.isEmpty()))
                    {

                        double value = dep.measure.getUpperBound();
                        value += bounds.get(b);

                        if(!basis.contains(T_int_b) && !T_int_b.isEmpty())
                        {
                            split = true;
                            removeSet.add(b);
                            addSet.add(T_int_b);
                            addBound(T_int_b, value);
                        }

                        if(!basis.contains(T_minus_b) && !T_minus_b.isEmpty())
                        {
                            split = true;
                            removeSet.add(b);
                            addSet.add(T_minus_b);
                            addBound(T_minus_b, value);
                        }

                        if(!basis.contains(b_minus_T) && !b_minus_T.isEmpty())
                        {
                            split = true;
                            removeSet.add(b);
                            addSet.add(b_minus_T);
                            addBound(b_minus_T, value);
                        }
                    }
                }

                //even if it was split once we want to
                // restart with new basis set to avoid unnecessary splitting
                if(split)
                {
                    converged = false;
                }
            }

            //if no splitting happened in the complete
            //loop then it has converged!
            if(!split)
            {
                converged = true;
            }
            else
            {
                basis.removeAll(removeSet);
                basis.addAll(addSet);
                removeSet.clear();
                addSet.clear();
            }
        }


    }

    protected void addBound(IAttributeSet set, double value)
    {
        if(bounds.containsKey(set))
        {
            double previousValue = bounds.get(set);
            bounds.put(set, Math.min(previousValue, value));
        }
        else
        {
            bounds.put(set, value);
        }
    }

    public void infer(DataDependency dependency)
    {
        double value = 0.0;
        IAttributeSet lhs = dependency.lhs;
        IAttributeSet currentUnion = schema.getEmptyAttributeSet();
        for(IAttributeSet b : basis) {
            if(dependency.rhs.contains(b)) {
                currentUnion = currentUnion.union(b);
                value += bounds.get(b);
            }
        }

        if(currentUnion.contains(dependency.rhs) && dependency.rhs.contains(currentUnion))
        {
            dependency.measure.updateUpperBound(value);
        }
    }
}
