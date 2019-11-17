package uwdb.discovery.dependency.approximate.entropy;


//import com.sun.xml.internal.bind.v2.model.core.ID;


import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.dependency.JoinDependency;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IDependencySet;

import java.util.*;

public abstract class AbstractDataset implements IDataset {

    protected long numRows;
    protected int numAttributes;
    protected RelationSchema schema;
    double doubleAccuracyGlitches = 1E-7;

    private static double log_10_2 = Math.log10(2.0);
    public static double log2(double val) {
		  return (Math.log10(val)/log_10_2);
	}
    

    public AbstractDataset(RelationSchema schema)
    {
        this.schema = schema;
        this.numAttributes = schema.getNumAttributes();
    }

    public long getNumRows()
    {
        return numRows;
    }

    public int getNumAttributes() {
        return schema.getNumAttributes();
    }

    public void computeMeasures(IDependencySet dependencies) {
        HashMap<IAttributeSet, Double> entropies = new HashMap<IAttributeSet, Double>();
        addEntropiesForComputingMeasures(dependencies, entropies);
        computeEntropies(entropies);
        computerMeasuresFromEntropies(dependencies, entropies);
    }

    protected void addEntropiesForComputingMeasures(IDependencySet dependencies, HashMap<IAttributeSet, Double> subsets)
    {
        Iterator<DataDependency> iterator = dependencies.iterator();
        while (iterator.hasNext())
        {
            DataDependency dependency = iterator.next();
            switch (dependency.getType())
            {
                case FUNCTIONAL_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Add X, XY
                    subsets.put(X, Double.MAX_VALUE);
                    subsets.put(XY, Double.MAX_VALUE);
                }
                break;
                case MULTIVALUED_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Create X(R-XY) = R-Y
                    IAttributeSet R_Y = dependency.rhs.complement();

                    //Add X, XY, R-Y
                    subsets.put(X, Double.MAX_VALUE);
                    subsets.put(XY, Double.MAX_VALUE);
                    subsets.put(R_Y, Double.MAX_VALUE);
                }
                break;
                default:
                    break;
            }
        }
    }

    protected void computerMeasuresFromEntropies(IDependencySet dependencies, HashMap<IAttributeSet, Double> entropies)
    {
        Iterator<DataDependency> iterator = dependencies.iterator();
        while (iterator.hasNext())
        {
            DataDependency dependency = iterator.next();
            switch (dependency.getType())
            {
                case FUNCTIONAL_DEPENDENCY:
                {
                    int hashCode1, hashCode2;
                    double entropy1, entropy2;

                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    double entropy_X = entropies.get(X);
                    double entropy_XY = entropies.get(XY);
                    double measure = entropy_XY - entropy_X;

                    try {
                          dependency.measure.setValue(measure);
                    	  dependency.setMeasure(measure);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
                case MULTIVALUED_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Create X(R-XY) = R-Y
                    IAttributeSet R_Y = dependency.rhs.complement();

                    double entropy_X = entropies.get(X);
                    double entropy_XY = entropies.get(XY);
                    double entropy_R_Y = entropies.get(R_Y);
                    double entropy_R = getTotalEntropy();
                    double measure = entropy_XY + entropy_R_Y - entropy_R - entropy_X;

                    try {
                         dependency.measure.setValue(measure);
                    	 dependency.setMeasure(measure);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
                default:
                    break;
            }
        }
    }

    
    protected void addCount(HashMap<Integer, Long> counts, long[] key, IAttributeSet set)
    {
        int hashCode = Utilities.getHashCode(key, set);
        if(!counts.containsKey(hashCode))
        {
            counts.put(hashCode, (long)1);
        }
        else
        {
            Long value = counts.get(hashCode);
            value += 1;
            counts.put(hashCode, value);
        }
    }

    protected void addCount(HashMap<Integer, Long> counts, String[] key, IAttributeSet set)
    {
        int hashCode = Utilities.getHashCode(key, set);
        if(!counts.containsKey(hashCode))
        {
            counts.put(hashCode, (long)1);
        }
        else
        {
            Long value = counts.get(hashCode);
            value += 1;
            counts.put(hashCode, value);
        }
    }

    protected double computeEntropyValue(HashMap<Integer, Long> counts, long totalCount)
    {
        double entropy = 0;
        for(Map.Entry<Integer,Long> entry : counts.entrySet())
        {
            double prob = (double)entry.getValue() / (double)totalCount;
            if(prob > 0)
            {
                //entropy += prob * Math.log(prob);
                entropy += prob* AbstractDataset.log2(prob);
            }
        }

        return -entropy;
    }

    public double getTotalEntropy()
    {
       // return Math.log(numRows);
    	return log2(numRows);    	
    }

    public abstract void computeEntropies(HashMap<IAttributeSet, Double> values);
    
    public double computeMVD(DataDependency mvd) {
    	//Create X
        IAttributeSet X = mvd.lhs;

        //Create XY
        IAttributeSet XY = mvd.lhs.union(mvd.rhs);
        
        double entropy_X = computeEntropy(X); //entropies.get(X);
    	double entropy_R = getTotalEntropy();
    	
        //just compute I(Y;Y|X)=h(YX)-H(X)=H(Y|X)
        if(XY.cardinality() == mvd.lhs.length()) {
        	double measure = entropy_R-entropy_X;
        	if(measure <= doubleAccuracyGlitches && measure >= -doubleAccuracyGlitches)
        		measure =0;
        	mvd.setMeasure(measure);
        	return measure;
        }

        //Create X(R-XY) = R-Y
        IAttributeSet R_Y = mvd.rhs.complement();

      //  double entropy_X = computeEntropy(X); //entropies.get(X);
        double entropy_XY = computeEntropy(XY); //entropies.get(XY);
        double entropy_R_Y = computeEntropy(R_Y); //entropies.get(R_Y);
      //  double entropy_R = getTotalEntropy();
        double measure = entropy_XY + entropy_R_Y - entropy_R - entropy_X;
        if(measure <= doubleAccuracyGlitches && measure >= -doubleAccuracyGlitches)
    		measure =0;
        mvd.setMeasure(measure);
        return measure;
    }
    
    public double computeJD(JoinDependency JD) {
    	
    	HashMap<IAttributeSet, Double> entropiesToCompute = new HashMap<IAttributeSet, Double>();
    
    	Iterator<IAttributeSet> it = JD.componentIterator();    	
    	
    	IAttributeSet toCalc = JD.getlhs().clone();
    	int numComponents=0;
    	while(it.hasNext()) {    		
    		toCalc.or(it.next());
    		entropiesToCompute.put(toCalc.clone(), 0.0);    		
    		toCalc.intersectNonConst(JD.getlhs());
    		numComponents++;
    	}
    	
    	entropiesToCompute.put(JD.getlhs(),0.0);    	
    	
    	
    	computeEntropies(entropiesToCompute);
    	double lhsPart = entropiesToCompute.get(JD.getlhs());
    	entropiesToCompute.remove(JD.getlhs());
    	double componentParts = 0;
    	for(Double val : entropiesToCompute.values()) {
    		componentParts+=val;
    	}
    	double totalEntropy = getTotalEntropy();
    	double JDMeasure = componentParts-(numComponents-1.0)*lhsPart-totalEntropy;
    	if(JDMeasure < 0.0) {
    		assert Math.abs(JDMeasure)<0.0001;
    		JDMeasure=0.0;
    	}
    	JD.setMeasure(JDMeasure);
    	return JDMeasure;
    }
}
