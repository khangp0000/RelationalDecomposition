package uwdb.discovery.dependency.approximate.common.dependency;

import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.sets.IDependencySet;
import uwdb.discovery.dependency.approximate.inference.IInferenceModule;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;

public class MultivaluedDependency extends DataDependency {

    public MultivaluedDependency(IAttributeSet lhs, IAttributeSet rhs)
    {
        this(lhs, rhs, 0, Double.MAX_VALUE);
    }

    public MultivaluedDependency(IAttributeSet lhs, IAttributeSet rhs, double upperBound)
    {
        this(lhs, rhs, 0, upperBound);
    }

    public MultivaluedDependency(IAttributeSet lhs, IAttributeSet rhs, double lowerBound, double upperBound)
    {
        super(lhs, rhs, lowerBound, upperBound);
    }

    public DependencyType getType() {
        return DependencyType.MULTIVALUED_DEPENDENCY;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(lhs);
        sb.append(" ->> ");
        sb.append(rhs);
        sb.append(" : ");
        sb.append(this.getMeasure());
        return sb.toString();
    }

    /**
     * For X ->> Y, we add all MVDs of the form XB ->> Y-B
     * @param destination
     * @return
     */
    public boolean addSpecializations(DependencySet destination)
    {

        boolean somethingAdded = false;
        int numAttributes = lhs.length();

        for(int i = 0; i < numAttributes; i++)
        {
            if(!lhs.contains(i))
            {
                //XB
                IAttributeSet newLHS = lhs.clone();
                newLHS.add(i);

                //Y-B
                IAttributeSet newRHS = rhs;
                if(rhs.contains(i))
                {
                    newRHS = rhs.clone();
                    newRHS.remove(i);
                }

                if(!newRHS.isEmpty()) {
                    //Adding XB -> Y-B
                    MultivaluedDependency dep = new MultivaluedDependency(newLHS, newRHS);
                    destination.add(dep);
                }

                somethingAdded = true;
            }
        }
        return somethingAdded;
    }

    /**
     * For X ->> Y, we add all MVDs of the form XB ->> Y-B, but only if it is not trivially
     * implied by already discovered data dependencies
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
            if(!lhs.contains(i))
            {
                ///XB
                IAttributeSet newLHS = lhs.clone();
                newLHS.add(i);

                //Y-B
                IAttributeSet newRHS = rhs;
                if(rhs.contains(i))
                {
                    newRHS = rhs.clone();
                    newRHS.remove(i);
                }

                if(!newRHS.isEmpty()) {
                    //Adding XB -> Y-B
                    MultivaluedDependency dep = new MultivaluedDependency(newLHS, newRHS);

                    //check if the discovered FDs trivially imply dep and add otherwise
                    if (!inferenceModule.implies(dep)) {
                        destination.add(dep);
                    }
                }

                somethingAdded = true;
            }
        }
        return somethingAdded;
    }

    /**
     * For X -> Y, adds all FDs of the form X-B -> Y where B \not\in Y
     * @param destination
     * @return
     */
    public boolean addGeneralizations(DependencySet destination)
    {
        boolean somethingAdded = false;
        for(int i = lhs.nextAttribute(0); i >= 0; i = lhs.nextAttribute(i+1))
        {
            //X-B
            IAttributeSet newLHS = lhs.clone();
            newLHS.remove(i);

            //YB
            IAttributeSet newRHS = rhs.clone();
            newRHS.add(i);

            //X-B ->> Y
            MultivaluedDependency dep1 = new MultivaluedDependency(newLHS, rhs);
            destination.add(dep1);

            //X-B ->> YB
            MultivaluedDependency dep2 = new MultivaluedDependency(newLHS, newRHS);
            destination.add(dep2);

            somethingAdded = true;
        }
        return somethingAdded;
    }


    /**
     * Given n, adds all MVDs of the form \phi -> S, where S \subset R
     * @param schema
     * @param destination
     */
    public static void addMostGeneralDependencies(RelationSchema schema, IDependencySet destination)
    {
        IAttributeSet lhs = schema.getEmptyAttributeSet();
        IAttributeSet rhs = schema.getEmptyAttributeSet();

        MultivaluedDependency dependency = new MultivaluedDependency(lhs, rhs);

        //Add all A ->> Y, such that A \not\in Y and Y \subset R
        addRHSSupersets(schema, lhs, rhs, destination);
    }

    /**
     * Most specific MVD would be one with R-AB ->> A
     * @param schema
     * @param destination
     */
    public static void addMostSpecificDependencies(RelationSchema schema, IDependencySet destination)
    {
        int numAttributes = schema.getNumAttributes();
        for(int i = 0; i < numAttributes; i++)
        {
            for(int j = 0; j < numAttributes; j++)
            {
                //Add only for R-AB ->> A as R-AB ->> B is implied
                if(i < j)
                {
                    //R-AB
                    IAttributeSet lhs = schema.getEmptyAttributeSet();
                    lhs.add(i);
                    lhs.add(j);
                    lhs = lhs.complement();

                    //A
                    IAttributeSet rhs = schema.getEmptyAttributeSet();
                    rhs.add(i);

                    MultivaluedDependency dep = new MultivaluedDependency(lhs, rhs);
                    destination.add(dep);
                }
            }
        }
    }

    public static void printLattice(RelationSchema schema, PrintStream out)
    {
        DependencySet deps = new DependencySet();
        MultivaluedDependency.addMostGeneralDependencies(schema, deps);
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

    protected static void addRHSSupersets(RelationSchema schema, IAttributeSet lhs, IAttributeSet rhs, IDependencySet destination)
    {
        int numAttributes = schema.getNumAttributes();

        //add only if |Y| <= |R|/2 as other way is implied
        boolean addNextLevel = (rhs.cardinality() <= (double)numAttributes/2);

        if(addNextLevel) {
            for(int i = 0; i < numAttributes; i++) {
                if(!lhs.contains(i) && !rhs.contains(i)) {
                    //Add X -> AY
                    IAttributeSet nextRHS = rhs.clone();
                    nextRHS.add(i);

                    MultivaluedDependency dependency = new MultivaluedDependency(lhs, nextRHS);
                    destination.add(dependency);

                    //Add all X ->> Z such that AY \subset Z and A \not\in X
                    addRHSSupersets(schema, lhs, nextRHS, destination);
                }
            }
        }
    }
}
