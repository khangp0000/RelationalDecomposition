package uwdb.discovery.dependency.approximate.common.dependency;

import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.inference.IInferenceModule;


import java.io.PrintStream;
import java.util.*;

public class FunctionalDependency extends DataDependency {

    public FunctionalDependency(IAttributeSet lhs, IAttributeSet rhs)
    {
        this(lhs, rhs, 0, Double.MAX_VALUE);
    }

    public FunctionalDependency(IAttributeSet lhs, IAttributeSet rhs, double upperBound)
    {
        this(lhs, rhs, 0, upperBound);
    }

    public FunctionalDependency(IAttributeSet lhs, IAttributeSet rhs, double lowerBound, double upperBound)
    {
        super(lhs, rhs, lowerBound, upperBound);
    }

    public DependencyType getType() {
        return DependencyType.FUNCTIONAL_DEPENDENCY;
    }

    /**
     * For X -> A, we add all FDs of the form XB -> A, where B \neq A and B \not\in X
     * @return
     */
    public boolean addSpecializations(DependencySet destination)
    {

        boolean somethingAdded = false;
        int numAttributes = lhs.length();

        for(int i = 0; i < numAttributes; i++)
        {
            if(!lhs.contains(i) && !rhs.contains(i))
            {
                //XB
                IAttributeSet newLHS = lhs.clone();
                newLHS.add(i);

                //Adding XB -> A
                FunctionalDependency dep = new FunctionalDependency(newLHS, rhs);
                destination.add(dep);

                somethingAdded = true;
            }
        }
        return somethingAdded;
    }

    /**
     * For X -> Y, adds all FDs of the form XB -> Y, where B \not\in Y and B \not\in X and which
     * are not implied by the discovered dependencies
     * @param destination
     * @param inferenceModule
     * @return
     */
    public boolean addSpecializations(IInferenceModule inferenceModule, DependencySet destination)
    {
        boolean somethingAdded = false;
        int numAttributes = lhs.length();

        for(int i = 0; i < numAttributes; i++)
        {
            if(!lhs.contains(i) && !rhs.contains(i))
            {
                //XB
                IAttributeSet newLHS = lhs.clone();
                newLHS.add(i);

                //Adding XB -> Y
                FunctionalDependency dep = new FunctionalDependency(newLHS, rhs);

                // check if the discovered FD is implied
                // by the inference module
                if(!inferenceModule.implies(dep))
                {
                    destination.add(dep);
                }

                somethingAdded = true;
            }
        }
        return somethingAdded;
    }

    /**
     * For X -> Y, adds all FDs of the form X-B -> Y where B \not\in Y
     * @return
     */
    public boolean addGeneralizations(DependencySet destination)
    {
        boolean somethingAdded = false;
        int numAttributes = lhs.length();

        for(int i = lhs.nextAttribute(0); i >= 0; i = lhs.nextAttribute(i+1))
        {
            //X-B
            IAttributeSet newLHS = lhs.clone();
            newLHS.remove(i);

            //Adding X-B -> Y
            FunctionalDependency dep = new FunctionalDependency(newLHS, rhs);
            destination.add(dep);

            somethingAdded = true;
        }
        return somethingAdded;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(lhs);
        sb.append(" -> ");
        sb.append(rhs);
        sb.append(" : ");
        sb.append(this.getMeasure());
        return sb.toString();
    }

    /**
     * Adds all FDs of the form: \phi -> A
     * @param schema
     * @param destination
     */
    public static void addMostGeneralDependencies(RelationSchema schema, DependencySet destination)
    {
        int numAttributes = schema.getNumAttributes();
        for(int i = 0; i < numAttributes; i++)
        {
            //\phi
            IAttributeSet lhs = schema.getEmptyAttributeSet();

            //A
            IAttributeSet rhs = schema.getEmptyAttributeSet();
            rhs.add(i);

            FunctionalDependency dep = new FunctionalDependency(lhs, rhs);
            destination.add(dep);
        }
    }

    /**
     * Adds all FDs of the form: R-A -> A
     * @param destination
     * @param schema
     */
    public static void addMostSpecificDependencies(RelationSchema schema, DependencySet destination)
    {
        int numAttributes = schema.getNumAttributes();
        for(int i = 0; i < numAttributes; i++)
        {
            //R-A
            IAttributeSet lhs = schema.getEmptyAttributeSet();
            lhs.add(i);
            lhs = lhs.complement();

            //A
            IAttributeSet rhs = schema.getEmptyAttributeSet();
            rhs.add(i);

            FunctionalDependency dep = new FunctionalDependency(lhs, rhs);
            destination.add(dep);
        }
    }

    /**
     * Prints the complete search lattice for the given schema
     * @param schema
     * @param out
     */
    public static void printLattice(RelationSchema schema, PrintStream out)
    {
        DependencySet deps = new DependencySet();
        FunctionalDependency.addMostGeneralDependencies(schema, deps);
        int level = 1;
        while(!deps.isEmpty())
        {
            out.printf("Level : %d\n", level);
            DependencySet nextLevel = new DependencySet();
            Iterator<DataDependency> iterator = deps.iterator();
            while(iterator.hasNext())
            {
                DataDependency dep = iterator.next();
                out.println(dep);
                dep.addSpecializations(nextLevel);
            }
            level++;
            deps = nextLevel;
        }
    }

}
