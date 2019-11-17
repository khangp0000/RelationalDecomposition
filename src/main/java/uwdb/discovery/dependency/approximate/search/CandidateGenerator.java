package uwdb.discovery.dependency.approximate.search;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.dependency.DependencyType;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.inference.IInferenceModule;

public class CandidateGenerator implements Iterator<DataDependency> {

	IAttributeSet LHS;
	IAttributeSet NonLHS;
	int numNonLHS;
	int numAtts;	
	double numCandidates;
	long curr;
	int[] attSet;
	 
	public CandidateGenerator(IAttributeSet determinant)
    {
        this.LHS = determinant;
        this.NonLHS=determinant.complement();
        numNonLHS=NonLHS.cardinality();
        numAtts = determinant.length();                
        numCandidates = Math.pow(2, numNonLHS-1)-1;        
        attSet = new int[numNonLHS];
        int t=0;
        for(int i=0; i < numAtts ; i++) {
        	if(NonLHS.contains(i))
        		attSet[t++]=i;
        }
        curr =1;
    }
	
	
	@Override
	public boolean hasNext() {
		return curr<=numCandidates;
	}

	@Override
	public DataDependency next() {
		AttributeSet retVal = new AttributeSet(numAtts);
		if(curr == 0 ) {
			for(int i=0 ; i<numNonLHS ; i++ )
				retVal.add(attSet[i]);
		}
		else {
			for(int j = 0; j <= numNonLHS; j++) 
	        {
				/* Check if jth bit in the  
	            counter is set If set then  
	            print jth element from set */
	            if((curr & (1 << j)) > 0) 
	            	retVal.add(attSet[j]);	                
	        }     
		}
		curr++;
		return new DataDependency(LHS, retVal,0,0) {
			
			@Override
			public DependencyType getType() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public boolean addSpecializations(IInferenceModule inferenceModule, DependencySet destination) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean addSpecializations(DependencySet destination) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean addGeneralizations(DependencySet destination) {
				// TODO Auto-generated method stub
				return false;
			}
		};		
	}
		
	
	private static HashSet<IAttributeSet> getAllDeterminants(int numAttributes) {
		double numPossibleDeterminants = Math.pow(2, numAttributes)-1; 
		long curr =0;
		HashSet<IAttributeSet> retVal = new HashSet<IAttributeSet>();
		while(curr < numPossibleDeterminants) {
			AttributeSet determinant = new AttributeSet(numAttributes);
			for(int j = 0; j < numAttributes; j++) 
	        {
				/* Check if jth bit in the  
	            counter is set If set then  
	            print jth element from set */
	            if((curr & (1 << j)) > 0) 
	            	determinant.add(j);	                
	        }
			retVal.add(determinant);
			curr++;
		}		
		return retVal;
	}
	
	public static DependencySet getAllMVDCandidates(int numAttributes) {
		HashSet<IAttributeSet> possibleDeterminants = getAllDeterminants(numAttributes);
		Iterator<IAttributeSet> determinantIterator = possibleDeterminants.iterator();
		DependencySet retVal = new DependencySet();
		while(determinantIterator.hasNext()) {
			IAttributeSet determinant = determinantIterator.next();
			CandidateGenerator CG = new CandidateGenerator(determinant);
			while(CG.hasNext()) {
				retVal.add(CG.next());
			}
		}
		return retVal;
	}
	
	
	public static void main(String[] args) {
		DependencySet dd = getAllMVDCandidates(5);
		System.out.println(dd.size());
		System.out.println("data dependencies");
		Iterator<DataDependency> it = dd.iterator();
		while(it.hasNext()) {
			System.out.println(it.next().toString());
		}
		
		
		AttributeSet LHS=new AttributeSet(6);
		LHS.add(1); LHS.add(5);
		CandidateGenerator CG = new CandidateGenerator(LHS);
		while(CG.hasNext()) {
			DataDependency DD = CG.next();
			System.out.println(DD.toString());
			
		}
		
	}
	
	
	
}
