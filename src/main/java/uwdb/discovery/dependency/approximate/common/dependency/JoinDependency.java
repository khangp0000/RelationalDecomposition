package uwdb.discovery.dependency.approximate.common.dependency;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IDependencySet;
import uwdb.discovery.dependency.approximate.inference.IInferenceModule;
import uwdb.discovery.dependency.approximate.search.CandidateGenerator;

public class JoinDependency{
	private IAttributeSet lhs;
	private HashSet<IAttributeSet> components;
	private double valueFromData;
	private IAttributeSet addedSoFar;
	private IAttributeSet[] componentArray;
	
	public static long timeSpentMerging=0;
	
	public static void resetMergeTime(){
		timeSpentMerging=0;
	}
	
		
	public Collection<IAttributeSet> getComponents(){
		return components;
	}
	
	public JoinDependency(IAttributeSet lhs, Collection<IAttributeSet> rhss) {
		this.lhs = lhs;
		components = new HashSet<IAttributeSet>(rhss);		
		addedSoFar = new AttributeSet(lhs.length());
		addedSoFar.or(lhs);
		for(IAttributeSet AS: rhss) {
			//addedSoFar.flip(0, lhs.length()-1); //assume correctness
			addedSoFar.or(AS);
		}
		
		componentArray=null;
	}
	
	public JoinDependency(DataDependency other) {
		this.lhs = other.lhs.clone();
		components = new HashSet<IAttributeSet>(2);	
		components.add(other.rhs.clone());
		IAttributeSet rhs2 = lhs.union(other.rhs);
		rhs2 = rhs2.complement();
		components.add(rhs2);
		addedSoFar.flip(0, lhs.length()-1); //assume correctness
	}
	public JoinDependency(JoinDependency other) {
		//this.lhs = other.lhs.clone();
		this.lhs = other.lhs.clone();
		components = new HashSet<IAttributeSet>(other.numComponents());	
		/*
		for(IAttributeSet AS: other.components) {
			//components.add(AS.clone());
			components.add(AS.clone());
		}*/
		addedSoFar = new AttributeSet(lhs.length());
		addedSoFar.flip(0, lhs.length()-1); //assume correctness
		IAttributeSet[] otherComponentArray = other.getComponentArray();
		if(otherComponentArray!= null) {
			componentArray = new IAttributeSet[other.numComponents()];
			for(int i=0 ; i < other.numComponents() ; i++) {
				componentArray[i]=otherComponentArray[i].clone();
				components.add(componentArray[i]);
			}
		}
		
	}
	
	
	public JoinDependency(IAttributeSet lhs) {
		this.lhs = lhs;
		components = new HashSet<IAttributeSet>();		
		addedSoFar = new AttributeSet(lhs.length());		
		addedSoFar.or(lhs);
	}
	
	public void addComponent(IAttributeSet component) {
		if(component.intersects(addedSoFar)){
			throw new ExceptionInInitializerError("Components in JD should be pairwise disjoint");			
		}
		components.add(component);
		addedSoFar.or(component);
		componentArray=null;
	}
	
	public IAttributeSet[] toArray() {
		if(componentArray==null) {
			componentArray = new  IAttributeSet[components.size()];
			Iterator<IAttributeSet> it = components.iterator();
			int i=0;
			while(it.hasNext()) {
				componentArray[i++]=it.next();
			}
		}
		return componentArray;
	}
	
	public  IAttributeSet[] getComponentArray() {
		return componentArray;
	}
	public int numComponents() {
		return components.size();
	}
	public void createComponentFromLHSVar(int var) {
		if(!lhs.contains(var))
			return;
		lhs.remove(var);
		AttributeSet set = new AttributeSet(lhs.length());
		set.add(var);
		components.add(set);
		componentArray=null;
	}
	
	public void moveComponentToLHS(IAttributeSet varSet) {
		if(!components.contains(varSet))
			return;
		
		lhs.or(varSet);
		components.remove(varSet);
		componentArray=null;
	}
	public JoinDependency mergeComponents(int i, int j) {
		long startTime = System.currentTimeMillis();
		JoinDependency retVal = new JoinDependency(this);		
		retVal.mergeComponentsNonConst(i, j);
		/*
		IAttributeSet[] componentsArray = this.getComponentArray();		
		JoinDependency retVal = new JoinDependency(this);		
	
		IAttributeSet newComponent = componentsArray[i].union(componentsArray[j]);
		retVal.components.remove(componentsArray[i]);
		retVal.components.remove(componentsArray[j]);
		retVal.components.add(newComponent);
		retVal.componentArray = null;
		retVal.toArray();
		*/
		timeSpentMerging+=(System.currentTimeMillis()-startTime);
		return retVal;
	}
	
	public IAttributeSet mergeComponentsNonConst(int i, int j) {
		long startTime = System.currentTimeMillis();
		
						
		IAttributeSet[] componentsArray = this.getComponentArray();		
		this.components.remove(componentsArray[i]);
		this.components.remove(componentsArray[j]);
		IAttributeSet newComponent = componentsArray[i];
		newComponent.or(componentsArray[j]);		
		this.components.add(newComponent);
		
		
		int lastIdx =  components.size();		
		componentsArray[j] = componentsArray[lastIdx];		
		componentsArray[lastIdx] = null; //so we get an exception if trying to access
		//this.componentArray = null;
		//this.toArray();
		
		timeSpentMerging+=(System.currentTimeMillis()-startTime);
		return newComponent;
	}
	
	
	public IAttributeSet getlhs() {
		return lhs;
	}
	
	public Iterator<IAttributeSet> componentIterator(){
		return components.iterator();
	}
    
	public int numOfComponents() {
		return components.size();
	}
    public void setMeasure(double val) {
    	valueFromData=val;
    }
    
    public double getMeasure() {
    	return valueFromData;
    }
	/*
	 * Returns true if lhs is an i-j separator
	 */
	private boolean doesSeparate(int i, int j) {
		if(lhs.contains(i)|| lhs.contains(j)) {
			return false;
		}
		Iterator<IAttributeSet> it = components.iterator();
		while(it.hasNext()) {
			IAttributeSet component = it.next();
			if(component.contains(i)) {
				if(component.contains(j))
					return false;
				else {
					return true;
				}
			}
		}
		return false;
		
	}
	
	private boolean hasComponent(IAttributeSet component) {
		Iterator<IAttributeSet> it = components.iterator();
		while(it.hasNext()) {
			IAttributeSet thisComp = it.next();
			if (thisComp.contains(component) && component.contains(thisComp))
				return true;
		}
		return false;
	}
	 @Override
	    public int hashCode() {
	        HashCodeBuilder builder = new HashCodeBuilder();
	        builder.append(lhs.hashCode());
	        Iterator<IAttributeSet> it = components.iterator();
			while(it.hasNext()) {
				IAttributeSet component = it.next();
				builder.append(component.hashCode());
			}	        
	        return builder.toHashCode();
	    }

	    @Override
	    public boolean equals(Object obj) {
	        JoinDependency other = (JoinDependency) obj;
	        if(!lhs.equals(other.lhs))
	        	return false;
	        if(numOfComponents() != other.numOfComponents()) {
	        	return false;
	        }
	        Iterator<IAttributeSet> it = other.componentIterator();
	        while(it.hasNext()) {
	        	IAttributeSet otherComp = it.next();
	        	if(!hasComponent(otherComp))
	        		return false;
	        }
	        return true;
	    }
	    
	    @Override
	    public String toString() {
	    	StringBuilder sb = new StringBuilder("{");
	    	sb.append(lhs.toString());
	    	sb.append("|");
	    	Iterator<IAttributeSet> it = componentIterator();
	        while(it.hasNext()) {
	        	IAttributeSet component = it.next();
	        	sb.append(component.toString());
	        	sb.append(",");
	        }
	        sb.deleteCharAt(sb.lastIndexOf(","));
	        sb.append("},");
	        sb.append(this.getMeasure());
	        return sb.toString();
	    	
	    }
	    
	    public static DependencySet translateToMVDs(JoinDependency JD) {
	    	IAttributeSet[] JDComponentArr = JD.toArray();
	    	IAttributeSet ASForGenerator = new AttributeSet(JDComponentArr.length);
	    	CandidateGenerator CG = new CandidateGenerator(ASForGenerator);
	    	DependencySet toReturn = new DependencySet();
	    	while(CG.hasNext()) {
	    		DataDependency dd = CG.next();
	    		IAttributeSet ddRHS = dd.rhs;
	    		IAttributeSet MVDRHS = new AttributeSet(JD.lhs.length());
	    		for(int i=ddRHS.nextAttribute(0); i >=0 ; i = ddRHS.nextAttribute(i+1)) {
	    			IAttributeSet toInclude = JDComponentArr[i];
	    			MVDRHS = MVDRHS.union(toInclude);
	    		}
	    		DataDependency MVD = new MultivaluedDependency(JD.getlhs(), MVDRHS);
	    		toReturn.add(MVD);
	    	}
	    	return toReturn;
	    }
}
	/**
     * Most specific MVD would be one with R-AB ->> A
     * @param schema
     * @param destination
     */
	/**
    public static void getMostRefinedJD(RelationSchema schema, IAttributeSet lhs)
    {
    	
        int numAttributes = schema.getNumAttributes();
        IAttributeSet nLHS = lhs.complement();
        
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
    */

