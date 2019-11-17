package uwdb.discovery.dependency.approximate.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Queue;
import java.util.Set;

import uwdb.discovery.dependency.approximate.common.GraphUtils;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.Transversals;
import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.dependency.DependencyType;
import uwdb.discovery.dependency.approximate.common.dependency.JoinDependency;
import uwdb.discovery.dependency.approximate.common.dependency.MultivaluedDependency;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.entropy.AbstractDataset;
import uwdb.discovery.dependency.approximate.entropy.ExternalFileDataSet;
import uwdb.discovery.dependency.approximate.entropy.IDataset;

public class JDGeneratorGivenLHS {
	
	
	protected double epsilon;    
    protected AbstractDataset queryModule;

    protected DependencySet queue;
    protected DependencySet discoveredDependencies;
    protected int numAttribtues;
    protected Map<IAttributeSet, Set<JoinDependency>> MinedJDs;

    public int scanCount;
    public long totalRunningTime;
    public long totalScanningTime;
    private Map<AttributePair, Set<IAttributeSet>> minPairwiseSeps;
    private Set<IAttributeSet> initialSetOfReducedLHS;
    private Set<IAttributeSet> minedMinSeps;
    
    public long timePreparingEntropies=0;
    public long timeToGetConsistentJD=0;
    public long calculatingIMeasure =0;
    public long timeToMineJDsWithLHS =0;
     
    private class AttributePair{
    	public AttributePair(int AttX, int AttY) {
    		this.AttX = AttX;
    		this.AttY = AttY;
    		if(this.AttX > this.AttY) {
    			int temp = this.AttX;
    			this.AttX = this.AttY;
    			this.AttY = temp;
    		}
    	}
    	
    	public Integer AttX;
    	public Integer AttY;
    	
    	  @Override
    	    public int hashCode() {
    	        HashCodeBuilder builder = new HashCodeBuilder();
    	        builder.append(AttX.hashCode());
    	        builder.append(AttY.hashCode());
    	        return builder.toHashCode();
    	    }

    	    @Override
    	    public boolean equals(Object obj) {
    	    	AttributePair other = (AttributePair) obj;
    	        boolean b1 = (other.AttX == AttX && other.AttY == AttY);
    	        boolean b2= (other.AttX == AttY && other.AttY == AttX);
    	        return (b1||b2);
    	    }
    	    
    	    @Override
    	    public String toString() {
    	        StringBuilder sb = new StringBuilder();
    	        sb.append("<");
    	        sb.append(AttX);
    	        sb.append(",");
    	        sb.append(AttY);
    	        sb.append(">");
    	        return sb.toString();
    	    }
    }
    
    public AbstractDataset getDatasetObject() {
    	return queryModule;
    }
    
    public Collection<IAttributeSet> getDiscoveredDataDependencies(){
    	return minedMinSeps;
    }
    public JDGeneratorGivenLHS(AbstractDataset queryModule, double epsilon, int numAtts) {
    	this.epsilon=epsilon;
    	this.queryModule = queryModule;
    	this.numAttribtues = numAtts;
    	this.totalRunningTime = 0;
    	MinedJDs = new HashMap<IAttributeSet, Set<JoinDependency>>();
    	initialSetOfReducedLHS = new HashSet<IAttributeSet>();
    	minPairwiseSeps = new HashMap<AttributePair, Set<IAttributeSet>>();
    	minedMinSeps=new HashSet<IAttributeSet>();
		this.initSetOfMinSeps(initialSetOfReducedLHS);
   
    }
    
    public static double calculateJDMeasure(JoinDependency JD, AbstractDataset queryModule) {
    	HashMap<IAttributeSet, Double> entropiesToCalc = new HashMap<IAttributeSet, Double>();
    	//HashMap<IAttributeSet, Double> lhsToCalc = new HashMap<IAttributeSet, Double>();
    	
    	entropiesToCalc.put(JD.getlhs(), 0.0);
    	Iterator<IAttributeSet> it = JD.componentIterator();
    	
    	while(it.hasNext()) {    		
    		IAttributeSet toCalc = JD.getlhs().union(it.next());
    		entropiesToCalc.put(toCalc, 0.0);
    	}
    	
    	queryModule.computeEntropies(entropiesToCalc);
    	//queryModule.computeEntropies(lhsToCalc);
    	double componentParts = 0;
    	double lhsPart = entropiesToCalc.get(JD.getlhs());
    	double totalEntropy = queryModule.getTotalEntropy();
    	Iterator<Entry<IAttributeSet, Double>> entropyIt = entropiesToCalc.entrySet().iterator();
    	while(entropyIt.hasNext()) {
    		Entry<IAttributeSet, Double> e = entropyIt.next();
    		componentParts+=e.getValue();
    	}
    	
    	double JDMeasure = componentParts-(entropiesToCalc.size()-1)*lhsPart-totalEntropy;
    	JD.setMeasure(JDMeasure);
    	return JDMeasure;
    }
    
    public double calcuateIMeasure(DataDependency MVD) {
    	long startTime = System.currentTimeMillis();
    	IAttributeSet first = MVD.rhs;
    	IAttributeSet rhslhs = first.union(MVD.lhs);
    	IAttributeSet second = rhslhs.complement();
    	double retVal = calcuateIMeasure(first,second,MVD.lhs, this.queryModule );
    	calculatingIMeasure+=(System.currentTimeMillis()-startTime);
    	return retVal;
    }
    		
    public static double calcuateIMeasure(IAttributeSet first, IAttributeSet second, 
    			IAttributeSet lhs,AbstractDataset queryModule) {
    	
    	
    	HashMap<IAttributeSet, Double> entropiesToCalc = new HashMap<IAttributeSet, Double>();    	
    	IAttributeSet firstlhs = first.union(lhs);
    	IAttributeSet secondlhs = second.union(lhs);
    	IAttributeSet allVars = secondlhs.union(first);
    	
    	entropiesToCalc.put(lhs, 0.0);
    	entropiesToCalc.put(firstlhs, 0.0);
    	entropiesToCalc.put(secondlhs, 0.0);
    	if(allVars.cardinality() < first.length())
    		entropiesToCalc.put(allVars, 0.0);
    	
    	queryModule.computeEntropies(entropiesToCalc);
    	double retVal=0;
    	if(allVars.cardinality() < first.length()) {
	    	retVal = entropiesToCalc.get(firstlhs)+
	    			entropiesToCalc.get(secondlhs)-entropiesToCalc.get(lhs)
	    			-entropiesToCalc.get(allVars);
    	}
    	else {
    		retVal = entropiesToCalc.get(firstlhs)+
	    			entropiesToCalc.get(secondlhs)-entropiesToCalc.get(lhs)
	    			-queryModule.getTotalEntropy();
    	}    	
    	
    	return retVal;
    }
    		
    private JoinDependency mostSpecificJD(IAttributeSet lhs) {    	
    	IAttributeSet rest = lhs.complement();    	
    	JoinDependency JD = new JoinDependency(lhs);
    	for(int i = rest.nextAttribute(0); i >=0 ; i = rest.nextAttribute(i+1)) {
    		IAttributeSet singleiAttSet = new AttributeSet(lhs.length());
    		singleiAttSet.add(i);
    		JD.addComponent(singleiAttSet);
    	}
    	return JD;
    }
    
    private boolean inDifferentComponents(int AttX, int AttY, JoinDependency JD) {
    	Iterator<IAttributeSet> it = JD.componentIterator();
    	while(it.hasNext()) {
    		IAttributeSet component = it.next();
    		if(component.contains(AttX) && component.contains(AttY))
    			return false;
    	}
    	return true;
    }
    public Set<JoinDependency> mineAllJDsWithLHS(int AttX, int AttY, 
    		IAttributeSet lhs, int limit, JoinDependency JDToStart) {
    	long startTime = System.currentTimeMillis();
    	
    	JoinDependency JD0 = getConsistentJDCandidate(AttX, AttY, lhs,epsilon, JDToStart);
    	if(JD0==null)
    		return new HashSet<JoinDependency>(1); //return empty set
    	
    	Queue<JoinDependency> Q = new LinkedList<JoinDependency>();
    	Q.add(JD0);
    	Set<JoinDependency> P = new HashSet<JoinDependency>();
    	while(!Q.isEmpty()) {
    		JoinDependency JD = Q.poll();
    		double iJD = JDGeneratorGivenLHS.calculateJDMeasure(JD,queryModule);
    		if(iJD <= epsilon) {    	
    			P.add(JD);
    			if(limit>0 && P.size() >= limit) {
    				break;
    			}
    		}
    		else {
    			int numComponents = JD.numComponents();    			
    			if(numComponents > 2) { //if it is <= 2 then we know that there is no JD
    				IAttributeSet[] JDArray = JD.toArray();
    				//Set<JoinDependency> thisRoundJDs = new HashSet<JoinDependency>();
	    			for(int i=0; i < numComponents ; i++) {
	    				for(int j=i+1 ; j < numComponents; j++) {
	    					if((JDArray[i].contains(AttX) && JDArray[j].contains(AttY)) ||
	    							(JDArray[i].contains(AttY) && JDArray[j].contains(AttX)))
	    						continue; //we do not want to merge AttX and AttY
	    					
	    					JoinDependency mergedij = JD.mergeComponents(i, j);
	    					JoinDependency mergedijConsistent = getConsistentJDCandidate(AttX, AttY, lhs,epsilon, mergedij);
	    					if(mergedijConsistent != null) {
	    						Q.add(mergedij);	    	
	    						break;
	    					}
	    					//thisRoundJDs.add(mergedij);
	    				}
	    			}
	    			
	    			//prepareEntropiesForJDs(thisRoundJDs);
    			}
    		}
    	}
    	timeToMineJDsWithLHS+=(System.currentTimeMillis()-startTime);
    	return P;
    }
    
    private void prepareEntropiesForJDs(Collection<JoinDependency> JDs) {
    	long startTime = System.currentTimeMillis();
    	HashMap<IAttributeSet, Double> values = new HashMap<IAttributeSet, Double>();
    	//Set<IAttributeSet> componentsAddedToValues = new HashSet<IAttributeSet>();
    	for(JoinDependency JD:JDs) {
    		IAttributeSet[] JDCOmponents = JD.toArray();
        	values.put(JD.getlhs(), 0.0);
        	
        //	componentsAddedToValues.add(JD.getlhs());
        	
        	for(int i=0 ; i < JDCOmponents.length ; i++) {
        	//	if(componentsAddedToValues.contains(JDCOmponents[i]))
        	//		continue;
        		
        		IAttributeSet seti = JDCOmponents[i].union(JD.getlhs());
        		values.put(seti,0.0);        		
        	}
        	
        	
        	for(int i=0 ; i < JDCOmponents.length ; i++) {
        		for(int j=i+1 ; j < JDCOmponents.length ; j++) {
        		//	if(componentsAddedToValues.contains(JDCOmponents[i])&& (componentsAddedToValues.contains(JDCOmponents[j])))
        		//		continue;
        			
        			IAttributeSet setijlhs = JDCOmponents[i].union(JDCOmponents[j]);
        			IAttributeSet lhs = JD.getlhs();
        			for(int index = lhs.nextAttribute(0); index >= 0 ; 
        					index = lhs.nextAttribute(index+1)) {
        				setijlhs.add(index);
        			}
        			values.put(setijlhs,0.0);
        		//	componentsAddedToValues.add(JDCOmponents[i]);
        		//	componentsAddedToValues.add(JDCOmponents[j]);
        		}
        	}        	
    	}
    	timePreparingEntropies+=(System.currentTimeMillis()-startTime);
    	queryModule.computeEntropies(values);
    }
    
    private void prepareEntropiesForComponentAnalysis(JoinDependency JD) {
    	long startTime = System.currentTimeMillis();
    	HashMap<IAttributeSet, Double> values = new HashMap<IAttributeSet, Double>();
    	IAttributeSet[] JDCOmponents = JD.toArray();
    	values.put(JD.getlhs(), 0.0);
    	for(int i=0 ; i < JDCOmponents.length ; i++) {
    		IAttributeSet seti = JDCOmponents[i].union(JD.getlhs());
    		values.put(seti,0.0);
    	}
    	for(int i=0 ; i < JDCOmponents.length ; i++) {
    		for(int j=i+1 ; j < JDCOmponents.length ; j++) {
    			IAttributeSet setijlhs = JDCOmponents[i].union(JDCOmponents[j]);
    			IAttributeSet lhs = JD.getlhs();
    			for(int index = lhs.nextAttribute(0); index >= 0 ; 
    					index = lhs.nextAttribute(index+1)) {
    				setijlhs.add(index);
    			}
    			values.put(setijlhs,0.0);
    		}
    	}
    	timePreparingEntropies+=(System.currentTimeMillis()-startTime);
    	queryModule.computeEntropies(values);
    }
    
    private JoinDependency getConsistentJDCandidate(int AttX, int AttY, 
    		IAttributeSet lhs, double epsilon, JoinDependency JDtoStart) {
    	//JoinDependency JD = mostSpecificJD(lhs); //starting point
    	long startTime = System.currentTimeMillis();
    	JoinDependency JD = JDtoStart;
    	
    	boolean changed;
    	do{
    		changed = false;
    		//do this so that all of the required entropy values are cached.    	
    		prepareEntropiesForComponentAnalysis(JD);
    		IAttributeSet[] JDCOmponents = JD.toArray();
    		
    		//create graph based on the most specific JD
    		List<List<Integer>> graph = GraphUtils.createGraph(JDCOmponents.length);
    		for(int i=0 ; i < JDCOmponents.length ; i++) {
    			IAttributeSet first = JDCOmponents[i];
    			for(int j=i+1 ; j < JDCOmponents.length ; j++) {
        			IAttributeSet second = JDCOmponents[j];
        			if(calcuateIMeasure(first,second, lhs,queryModule)>epsilon) {
        				//i and j must appear together in any constalation
        				GraphUtils.addUndirectedEdge(graph, i, j);
        				changed=true;
        			}
        		}
    		}
    		if(changed) {
	    		GraphUtils GU = new GraphUtils(graph);
	    		int[] cc = GU.getComponents();

	    		//build new JD from the connected components
	    		HashMap<Integer, IAttributeSet> SAHM = new HashMap<Integer, IAttributeSet>();
	    		for (int i=0; i < cc.length ; i++) {
	    			IAttributeSet Ci=null;
	    			if(SAHM.containsKey(cc[i])) {
	    				Ci = SAHM.get(cc[i]);
	    			}
	    			else {
	    				Ci = new AttributeSet(lhs.length());   				
	    			}
	    		
	    			Ci.or(JDCOmponents[i]); //the ith is assigned to this connected component
	    			SAHM.put(cc[i], Ci);
	    		}
	    		boolean XYSameComponent = false;
	    		Collection<IAttributeSet> SAHMcomponents = SAHM.values();
	    		for(IAttributeSet SAHMcomponent: SAHMcomponents) {
	    			if(SAHMcomponent.contains(AttX) && SAHMcomponent.contains(AttY)) {
	    				XYSameComponent = true;
	    				break;
	    			}	    				
	    		}
	    		if(SAHM.size() <= 1 || XYSameComponent) {
	    			JD = null;
	    			break;
	    		}
	    			
	    		//build updated JD
	    		JD = new JoinDependency(lhs);
	    		Set<Entry<Integer,IAttributeSet>> ccSet = SAHM.entrySet();
	    		for(Entry<Integer,IAttributeSet> ccEntry: ccSet) {
	    			JD.addComponent(ccEntry.getValue());
	    		}	    		
    		}
    	}while(changed);
    	
    	timeToGetConsistentJD+=(System.currentTimeMillis()-startTime);
    	return JD;
    }
    
    
    private void prepareEntropiesForReduction(IAttributeSet X) {
    	IAttributeSet Y = X.clone();    	
    	HashMap<IAttributeSet, Double> values = new HashMap<IAttributeSet, Double>();
    	
    	for(int i= Y.nextAttribute(0); i >= 0 ; i = Y.nextAttribute(i+1)) {
    		values.put(Y.clone(), 0.0);
    		IAttributeSet Z = Y.complement();
    		for(int j= Z.nextAttribute(0); j >= 0 ; j = Z.nextAttribute(j+1)) {
    			IAttributeSet Yj = Y.clone();
    			Yj.add(j);
        		values.put(Yj, 0.0);        		
        	}
    		Y.remove(i);
    	}    		
    	queryModule.computeEntropies(values);
    }
    
    private IAttributeSet reduceToMinJD(int AttX, int AttY, IAttributeSet X) {
    	prepareEntropiesForReduction(X);
    	IAttributeSet Y = X.clone();
    	Set<JoinDependency> JD_Y = mineAllJDsWithLHS(AttX, AttY, Y,1, mostSpecificJD(X));    	
    	if(JD_Y.isEmpty()) return null;
    	//JoinDependency lastJD =JD_Y.iterator().next(); 
    	
    	for(int i= Y.nextAttribute(0); i >= 0 ; i = Y.nextAttribute(i+1)) {
    		Y.remove(i);    			
    		JD_Y = mineAllJDsWithLHS(AttX, AttY, Y,1,mostSpecificJD(Y));
    		if(JD_Y.isEmpty()) { //equivalent to: if nonempty, then remove
    			Y.add(i);   	
    		}    		
    	}
    	return Y;
    }
    
    private JoinDependency reduceToMinJDReturnJD(int AttX, int AttY, IAttributeSet X) {
    	prepareEntropiesForReduction(X);
    	IAttributeSet Y = X.clone();
    	Set<JoinDependency> JD_Y = mineAllJDsWithLHS(AttX, AttY, Y,1, mostSpecificJD(X));    	
    	if(JD_Y.isEmpty()) return null;
    	//JoinDependency lastJD =JD_Y.iterator().next(); 
    	
    	JoinDependency toReturn = JD_Y.iterator().next();
    	for(int i= Y.nextAttribute(0); i >= 0 ; i = Y.nextAttribute(i+1)) {
    		Y.remove(i);    			
    		JD_Y = mineAllJDsWithLHS(AttX, AttY, Y,1,mostSpecificJD(Y));
    		if(JD_Y.isEmpty()) { //equivalent to: if nonempty, then remove
    			Y.add(i);   	
    		}    	
    		else {
    			toReturn = JD_Y.iterator().next();
    		}
    	}
    	return toReturn;
    }
    
    
    private boolean canBeReducedToMinSep(IAttributeSet sep, Set<IAttributeSet> minSeps) {
    	for(IAttributeSet minSep: minSeps) {
    		if(sep.contains(minSep))
    			return false;
    	}
    	return true;
    }
    
    private void initSetOfMinSeps(Set<IAttributeSet> reducedLHSSet){
    	long startTime = System.currentTimeMillis();
    	reducedLHSSet.clear();
    	DependencySet mostSpecificMVDs = new DependencySet();
    	MultivaluedDependency.addMostSpecificDependencies(
    			new RelationSchema(numAttribtues), mostSpecificMVDs);
    	
    	HashMap<IAttributeSet, Double> values = new HashMap<IAttributeSet, Double>();
    	Iterator<DataDependency> mostSpecificIT = mostSpecificMVDs.iterator();
    	while(mostSpecificIT.hasNext()) {
    		DataDependency MVD = mostSpecificIT.next();
    		values.put(MVD.lhs,0.0);
    		IAttributeSet rhs1 = MVD.rhs.union(MVD.lhs);
    		values.put(rhs1,0.0);
    		IAttributeSet rhs2 = MVD.rhs.complement();
    		values.put(rhs2,0.0);
    	}
    	queryModule.computeEntropies(values);
    	
    	long specificMVDCalculation = (System.currentTimeMillis()-startTime);
    	System.out.println("specificMVDCalculation: " + specificMVDCalculation);
    	
    	Iterator<DataDependency> it = mostSpecificMVDs.iterator();
    	while(it.hasNext()) {
    		DataDependency dd = it.next();    				
    		IAttributeSet ddlhs = dd.lhs;   
    		//get the appropriate pair
    		IAttributeSet attPair = ddlhs.complement();
    		int AttX = attPair.nextAttribute(0);
    		int AttY = attPair.nextAttribute(AttX+1);
    		AttributePair AP = new AttributePair(AttX,AttY);
    		
    		if(minPairwiseSeps.containsKey(AP)) {
    			Set<IAttributeSet> XYMinSeps = minPairwiseSeps.get(AP);
    			if(!XYMinSeps.isEmpty())
    				continue; //we already have one min separator
    		}
    		
    		
    		
    		double ddMeasure=this.calcuateIMeasure(dd);
    		dd.setMeasure(ddMeasure);
    		if(dd.getMeasure() > this.epsilon) {    			
    			continue; //do not place in the data structure
    		}
    		//initialize
    		IAttributeSet pairLHS = ddlhs;
    		
    		JoinDependency reduceToMinJDReturnJD = reduceToMinJDReturnJD(AttX, AttY, ddlhs);
    		IAttributeSet reducedLHS = reduceToMinJDReturnJD.getlhs();
    		//IAttributeSet reducedLHS = reduceToMinJD(AttX, AttY, ddlhs);
    		if(reducedLHS != null) {	    	
    			reducedLHSSet.add(reducedLHS);
    			pairLHS = reducedLHS;
    		}
    		
    		
    		Set<IAttributeSet> XYMinSeps;
    		if(minPairwiseSeps.containsKey(AP)) {
    			XYMinSeps = minPairwiseSeps.get(AP);
    		}
    		else {
    			XYMinSeps = new HashSet<IAttributeSet>();
    		}
    		XYMinSeps.add(pairLHS);
    		//minPairwiseSeps.put(AP, XYMinSeps);
    		updateMinSepsBasedOnMinedJD(AP, reduceToMinJDReturnJD);
    		minedMinSeps.add(pairLHS);
    		//retVal.put(AP, XYMinSeps);
    	}
    	//return retVal;
    	this.totalRunningTime+=(System.currentTimeMillis()-startTime);
    }
    
    private void mineAllMinSeps(int AttX, int AttY) {
    	AttributePair XYPair = new AttributePair(AttX, AttY);
    	
    	if(!minPairwiseSeps.containsKey(XYPair)) { //no separator
    		minPairwiseSeps.put(XYPair, new HashSet<IAttributeSet>());
    		return;
    	}
    	Set<IAttributeSet> minXYSeps= minPairwiseSeps.get(XYPair);
    	IAttributeSet firstXYMinSep = minXYSeps.iterator().next();
    	if(minXYSeps.size() == 1 && firstXYMinSep.cardinality() == (numAttribtues-2)) {
    		//no other minimal separators are possible, just return
    		return;
    	}
    	
    	boolean done = false;
    	//should never return a set with either X or Y, since it only
    	//return minimal transversals, and neither X or Y can be
    	//part of a minimal transversal
    	Transversals minTransversals = new Transversals(minXYSeps, numAttribtues);
    	while(!done) {
    		JoinDependency CtrJD = null;
    		while(minTransversals.hasNext()) {
    			IAttributeSet minTransversal = minTransversals.next();
    			assert minTransversal.contains(AttX)==false;
    			assert minTransversal.contains(AttY)==false;
    			
    			minTransversal.flip(0, numAttribtues); //take the complement
    			minTransversal.remove(AttX);
    			minTransversal.remove(AttY);
    			
    			Set<JoinDependency> minTransversalJDs = 
    					mineAllJDsWithLHS(AttX, AttY, minTransversal,
    							1,mostSpecificJD(minTransversal));
    			if(!minTransversalJDs.isEmpty()) {
    				CtrJD = minTransversalJDs.iterator().next();
    				break;
    			}    			
    		}
    		if(CtrJD == null) {
    			done = true;    			
    		}
    		else {
    			IAttributeSet minLHS = reduceToMinJD(AttX, AttY,CtrJD.getlhs());
    			Set<JoinDependency> minTransversalJDs = mineAllJDsWithLHS(AttX, AttY, minLHS,
    					1,mostSpecificJD(minLHS));
    			assert (!minTransversalJDs.isEmpty());
    			JoinDependency newJD = minTransversalJDs.iterator().next();
    			updateMinSepsBasedOnMinedJD(XYPair, newJD);
    			minTransversals = new Transversals(minXYSeps, numAttribtues);
    			//minTransversals.addHyperedge(minLHS);
    		}  			
    		
    	}
    	
    }
    
    private void addMinSepForPair(AttributePair XYPair, IAttributeSet minXYSep) {
    	Set<IAttributeSet> minXYSeps;
    	if(minPairwiseSeps.containsKey(XYPair)) {
    		minXYSeps = minPairwiseSeps.get(XYPair);
    	}
    	else {
    		minXYSeps = new HashSet<IAttributeSet>();
    	}
    	minXYSeps.add(minXYSep);
    	minPairwiseSeps.put(XYPair,minXYSeps);
    }
    
    private void updateMinSepsBasedOnMinedJD(AttributePair XYPair, JoinDependency JD) {
    	IAttributeSet XComponent = null;
    	IAttributeSet YComponent = null;
    	Iterator<IAttributeSet> componentIt = JD.componentIterator();
    	while(componentIt.hasNext()) {
    		IAttributeSet component = componentIt.next();
    		if(component.contains(XYPair.AttX)) {
    			XComponent = component;
    		}
    		if(component.contains(XYPair.AttY)) {
    			YComponent = component;
    		}
    		if(XComponent!= null && YComponent != null) {
    			break;
    		}
    	}
    	for(int i = XComponent.nextAttribute(0) ; i >= 0 ; i = XComponent.nextAttribute(i+1)) {
    		for(int j = YComponent.nextAttribute(0) ; j >= 0 ; j = YComponent.nextAttribute(j+1)) {
    			AttributePair ijPair = new AttributePair(i, j);
    			addMinSepForPair(ijPair, JD.getlhs());
    		}
    	}
    	
    }
   
    private Set<IAttributeSet> mineAllMinSeps() {
    	long startTime = System.currentTimeMillis();
    	Set<AttributePair> seperablePairs = minPairwiseSeps.keySet();
    	for(AttributePair XYPair: seperablePairs) {
    		mineAllMinSeps(XYPair.AttX, XYPair.AttY);    		
    	}
    	
    	Set<IAttributeSet> retVal = new HashSet<IAttributeSet>();
    	Collection<Set<IAttributeSet>> minimalSeparators = minPairwiseSeps.values();
    	for(Set<IAttributeSet> SomePairMinSeps: minimalSeparators) {
    		retVal.addAll(SomePairMinSeps);
    	}
    	this.totalRunningTime+=(System.currentTimeMillis()-startTime);
    	return retVal;
    	
    	
    	/*
    	 * if minSeps = 0 then clearly, there are no separators in the relation.
    	 * If minSeps = 1 then there can be no other minseps.
    	 * Assume otherwise and let Y be the minimal separator that is doifferent from
    	 * X (but was not mined).
    	 * Since Y \not \supseteq X then X must contain a variable x that is not in Y.
    	 * So, Y must separate x from some other variable z. Therefore, Y should have been returned 
    	 * as a minimal separator derived from I(x;z|REST).
    	 * Since it wasn;t generated, we conclude that REST is not a separator, and therefore
    	 * neother is Y.
    	 * 
    	 */
    	/*
    	Set<IAttributeSet> minSeps = initSetOfMinSeps();
    	if(minSeps.size() < 2) {
    		return minSeps;  
    	}
    	boolean done = false;
    	Transversals minTransversals = new Transversals(minSeps, numAttribtues);
    	while(!done) {
    		JoinDependency CtrJD = null;
    		while(minTransversals.hasNext()) {
    			IAttributeSet minTransversal = minTransversals.next();
    			minTransversal.flip(0, numAttribtues); //take the complement
    			Set<JoinDependency> minTransversalJDs = mineAllJDsWithLHS(minTransversal,1);
    			if(!minTransversalJDs.isEmpty()) {
    				CtrJD = minTransversalJDs.iterator().next();
    				break;
    			}    			
    		}
    		if(CtrJD == null) {
    			done = true;    			
    		}
    		else {
    			IAttributeSet minLHS = reduceToMinJD(CtrJD.getlhs());
    			minSeps.add(minLHS);
    		}  			
    		
    	}
    	return minSeps;    		
    	*/
    }
    
    public static JDGeneratorGivenLHS executeTest(String dataSetPath, int numAttribtues, 
    		double threshold) {
    	RelationSchema schema = new RelationSchema(numAttribtues);        
        ExternalFileDataSet dataSet = new ExternalFileDataSet(dataSetPath, schema);     
        JDGeneratorGivenLHS miner = new JDGeneratorGivenLHS(dataSet,threshold,numAttribtues);
        miner.mineAllMinSeps();
        miner.printRuntimeCharacteristics();        
        return miner;
    }
    
    public static void executeTestsSingleDataset(String dataSetPath, 
    		int numAttribtues, double[] thresholds, String outputDirPath, long timeoutInSec) {
    	
    	
    	Path inputPath = Paths.get(dataSetPath);
    	String inputFilename = inputPath.getFileName().toString();
    	String outputFileName = inputFilename+".out.csv";
    	Path outputPath= Paths.get(outputDirPath, outputFileName);    	
    	
    	System.out.println("Executing tests of " + dataSetPath + " with runWithTimeout!");
    	
    	//We have to create the CSVPrinter class object 
        Writer writer;
		try {
			writer = Files.newBufferedWriter(outputPath);
			CSVPrinter csvPrinter = 
					new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader
							("#Attribtues", "#Rows", "Threshold", "#Minimal MVDs", 
									"#File Scans","total file scan time",
									"total Running time","%scanning"));
			for(int i=0; i < thresholds.length ; i++) {
				double thresh = thresholds[i];				
				Boolean completed = false;
				Callable<JDGeneratorGivenLHS> execution = new Callable<JDGeneratorGivenLHS>() {
					 @Override
				        public JDGeneratorGivenLHS call() throws Exception
				        {
						 JDGeneratorGivenLHS retVal = executeTest(dataSetPath, numAttribtues, thresh);						    
						 	return retVal;
				        }
				};
				RunnableFuture future = new FutureTask(execution);
			    ExecutorService service = Executors.newSingleThreadExecutor();
			    service.execute(future);
			    JDGeneratorGivenLHS miningResult = null;
			    try
			    {
			    	miningResult = (JDGeneratorGivenLHS) future.get(timeoutInSec, TimeUnit.SECONDS);    // wait
			    	completed = true;
			    }
			    catch (TimeoutException ex)
			    {
			        // timed out. Try to stop the code if possible.
			        future.cancel(true);			        
			    } catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    service.shutdown();
			   			   
				if(completed) {
					IDataset DatasetObj = miningResult.getDatasetObject();
					double percentScan = (double)DatasetObj.getTotalScanTime()/(double)miningResult.totalRunningTime;
					percentScan = percentScan*100.0;
					//Writing records in the generated CSV file
		            csvPrinter.printRecord(DatasetObj.getNumAttributes(), DatasetObj.getNumRows(),
		            		thresh, miningResult.getDiscoveredDataDependencies().size(),
		            		DatasetObj.getNumDBScans(), DatasetObj.getTotalScanTime(),
		            		miningResult.totalRunningTime, percentScan);
				}
				else {
					 csvPrinter.printRecord(numAttribtues, "NaN",
			            		thresh, "NaN",
			            		"NaN", "NaN",
			            		">"+timeoutInSec, "NaN");
				}
	            csvPrinter.flush();
			}
			csvPrinter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}       

    }
    
    public static void main(String[] args) {
    //	testMinSeps(args);
    	
    	String inDirectory = args[0];
    	String outDir = args[1];
    	File inDir = new File(inDirectory);    	
    	double[] thresholds = new double[] {0,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,
        		0.55,0.6,0.65,0.7,0.75,0.8,0.9,1.0,1.1,1.15,1.3,1.4,1.5,1.6,2.0,2.5,3,24};
        
    	File[] inFiles = inDir.listFiles();
    	Arrays.sort(inFiles, new Comparator<File>(){
    	    public int compare(File f1, File f2)
    	    {
    	        return Long.valueOf(f1.length()).compareTo(f2.length());
    	    } });
    	
        for(File inFile: inFiles) {
        		int numAttributes = getNumAtts(inFile);
                executeTestsSingleDataset(inFile.getAbsolutePath(), 
        		numAttributes, thresholds,outDir, 12000);
        }
    }
    
    /*
    public static void main(String[] args) {
    	testMinSeps(args);
    }*/
    public static void testMinSeps(String[] args) {
    	double alpha = 0.76;
    	DependencySet minedMVDs = mineMVDs(args,alpha);
    	Iterator<DataDependency> MVDIt = minedMVDs.iterator();
    /*	while(MVDIt.hasNext())
    		System.out.println(MVDIt.next());*/
    	
    	JoinDependency.resetMergeTime();
    	long startTime = System.currentTimeMillis();
    	String filePath = args[0];
        int numAttributes = Integer.valueOf(args[1]);
        RelationSchema schema = new RelationSchema(numAttributes);
        ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);  
        
        AttributeSet.resetConeTime();
        JDGeneratorGivenLHS JDGenLHS = new JDGeneratorGivenLHS(dataSet,alpha, numAttributes);
        System.out.println("initialization time " + (System.currentTimeMillis()-startTime));
        Set<IAttributeSet> JDminSeps = JDGenLHS.mineAllMinSeps();
        
        long runtime = System.currentTimeMillis()-startTime;
         
        //get ground truth minimal separators
        Iterator<IAttributeSet> lhsIterator = minedMVDs.determinantIterator();
        Set<IAttributeSet> determinantSet = new HashSet<IAttributeSet>();
        HashSet<IAttributeSet> minSepsGT = new HashSet<IAttributeSet>();
        while(lhsIterator.hasNext()) {
        	IAttributeSet test = lhsIterator.next();
        	determinantSet.add(test);
        }
    	
        for(int i = 0 ; i< numAttributes ; i++)
        	for(int j=i+1; j < numAttributes ; j++) {
        		AttributePair ijPair = JDGenLHS.new AttributePair(i,j);
        		Set<IAttributeSet> XYMinSeps = AllXYMinSepsFromCollection(ijPair,minedMVDs );
        		minSepsGT.addAll(XYMinSeps);
        	}
      
        /*
        for(IAttributeSet test : determinantSet) {
    		if(isMinimal(test, minedMVDs))
    			minSepsGT.add(test);
    	}
    	*/
    	
        
        System.out.println("Ground truth:");
        for(IAttributeSet test : minSepsGT) {
        	System.out.println(test.toString());
        }
        
        System.out.println("Mined minimal separators:");
        for(IAttributeSet test : JDminSeps) {
        	System.out.println(test.toString());
        }
        
        
        boolean passed = true;
      //compare the sets
		Iterator<IAttributeSet> JDminSepsIT = JDminSeps.iterator();
		while(JDminSepsIT.hasNext()) {
			IAttributeSet CurrJDMinSep = JDminSepsIT.next();
			if(!minSepsGT.contains(CurrJDMinSep)) {    				
				System.out.println("Error: Separator " + CurrJDMinSep.toString() + " should not have been mined");
				passed = false;
			}
		}
		
		Iterator<IAttributeSet> minSepsGTIT = minSepsGT.iterator();
		while(minSepsGTIT.hasNext()) {
			IAttributeSet CurrGTSep = minSepsGTIT.next();
			if(!JDminSeps.contains(CurrGTSep) && !(CurrGTSep.cardinality()==CurrGTSep.length()-1)) {    				
				System.out.println("Error: Separator " + CurrGTSep.toString() + " should have been mined, but wasn't");
				passed = false;
			}
		}
		
		int numSaturatedFDs=0;
		for(IAttributeSet s: minSepsGT) {
			if(s.cardinality() == (numAttributes-1))
				numSaturatedFDs++;
		}
		System.out.println("Number of ground truth minimal separators: " + minSepsGT.size());
		System.out.println("Number of saturated FDs: " + numSaturatedFDs);
		System.out.println("Number of ground truth minimal separators without"
				+ " saturated FDS: " + (minSepsGT.size()-numSaturatedFDs));
		System.out.println("Number of mined minimal separators: " + JDminSeps.size());
		
		System.out.println("passed="+passed);
		JDGenLHS.printRuntimeCharacteristics();
		System.out.println("runtime: " + runtime);
    }
    
    private static boolean isMinimalSep(IAttributeSet test, Collection<IAttributeSet> collection) {
    	for(IAttributeSet set: collection) {
    		if(test.contains(set) && test.cardinality()>set.cardinality()) {
    			return false;
    		}    					
    	}
    	return true;
    }
    
    private static Set<IAttributeSet> AllXYMinSepsFromCollection(AttributePair XYPair, 
    		DependencySet allMVDs){
    	Set<IAttributeSet> XYSeps = new HashSet<IAttributeSet>();
    	Set<IAttributeSet> retVal = new HashSet<IAttributeSet>();
    	Iterator<DataDependency> it = allMVDs.iterator();
    	while(it.hasNext()) {
    		DataDependency curr = it.next();
    		if(curr.lhs.contains(XYPair.AttX) || curr.lhs.contains(XYPair.AttY))
    			continue;
    		
    		if((curr.rhs.contains(XYPair.AttX) && !curr.rhs.contains(XYPair.AttY))||
    				(curr.rhs.contains(XYPair.AttY) && !curr.rhs.contains(XYPair.AttX))) {
    			XYSeps.add(curr.lhs);
    		}
    	}
    	
    	
    	for(IAttributeSet test: XYSeps) {
    		if(isMinimalSep(test,XYSeps)) {    			
    			retVal.add(test);    		
    		}
    	}
    	return retVal;
    	
    }
    /*
    public static void testMinedJDsWithLHS(String[] args) {
    	double alpha = 1.2;
    	DependencySet minedMVDs = mineMVDs(args,alpha);
    	
    	
    	
    	String filePath = args[0];
        int numAttributes = Integer.valueOf(args[1]);
        RelationSchema schema = new RelationSchema(numAttributes);
        
        ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);  
        JDGeneratorGivenLHS JDGenLHS = new JDGeneratorGivenLHS(dataSet,alpha, numAttributes);
    	Iterator<IAttributeSet> lhsIterator = minedMVDs.determinantIterator();
    	
    	DependencySet SaturatedFDs = new DependencySet();
    	FunctionalDependency.addMostSpecificDependencies(schema, SaturatedFDs);
    	boolean passed = true;
    	while(lhsIterator.hasNext()) {
    		DependencySet minedMVDsFromJDs = new DependencySet();
    		IAttributeSet lhs = lhsIterator.next();
    		Set<JoinDependency> JDOut = JDGenLHS.mineAllJDsWithLHS(lhs, -1);
    		for(JoinDependency JD : JDOut) {
    			DependencySet JDToMVDs = JoinDependency.translateToMVDs(JD);
    			minedMVDsFromJDs.add(JDToMVDs);
    		}
    		Iterator<DataDependency> lhsMVDIt = minedMVDs.iteratorOfDeterminant(lhs);
    		DependencySet lhsMVDSet = new DependencySet();
    		while(lhsMVDIt.hasNext()) {
    			lhsMVDSet.add(lhsMVDIt.next());
    		}
    		
    		//compare the sets
    		Iterator<DataDependency> JDMinedMVDsIt = minedMVDsFromJDs.iterator();
    		while(JDMinedMVDsIt.hasNext()) {
    			DataDependency JDMVD = JDMinedMVDsIt.next();
    			IAttributeSet equivRHS = JDMVD.lhs.union(JDMVD.rhs);
    			equivRHS = equivRHS.complement();
    			if(!lhsMVDSet.contains(JDMVD) && !lhsMVDSet.contains(JDMVD.lhs, equivRHS)) {    				
    				System.out.println("Error: MVD " + JDMVD.toString() + " should not have been mined");
    				passed = false;
    			}
    		}
    		
    		Iterator<DataDependency> lhsMVDSetIt = lhsMVDSet.iterator();
    		while(lhsMVDSetIt.hasNext()) {
    			DataDependency JDMVD = lhsMVDSetIt.next();
    			IAttributeSet equivRHS = JDMVD.lhs.union(JDMVD.rhs);
    			equivRHS = equivRHS.complement();
    			if(!minedMVDsFromJDs.contains(JDMVD) && !SaturatedFDs.contains(JDMVD) &&
    					!minedMVDsFromJDs.contains(JDMVD.lhs,equivRHS)) {
    				System.out.println("Error: MVD " + JDMVD.toString() + " should have been mined but wasn't");
    				passed = false;
    			}
    		}
    		
    	}
    	if(passed)
    		System.out.println("Test passed!!");
    }
    
    */
    
  //new code
    public static DependencySet mineMVDs(String[] args,double alpha)
    {
        String filePath = args[0];
        int numAttributes = Integer.valueOf(args[1]);
        RelationSchema schema = new RelationSchema(numAttributes);        
        ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);   
        TopDownInductiveSearch search = new TopDownInductiveSearch(DependencyType.MULTIVALUED_DEPENDENCY, 
        		dataSet, null, alpha);
        DependencySet minedMVDs = search.mineMVDs();      
        search.printRuntimeCharacteristics();
        return minedMVDs;
    }
    
    public void printRuntimeCharacteristics()
    {
        System.out.println("Runtime Characteristics:");
        System.out.printf("Num Attributes: %d\n", queryModule.getNumAttributes());
        System.out.printf("Num Rows: %d\n", queryModule.getNumRows());
        System.out.printf("Threshold: %f\n", epsilon);
        System.out.printf("Num File Scans: %d\n", queryModule.getNumDBScans());
        System.out.printf("File Scan Time: %d\n", queryModule.getTotalScanTime());
        System.out.printf("TIme preparing the entropies for caching: %d\n", 
        		((ExternalFileDataSet)queryModule).preparingMapForComputingEntropies);
        System.out.printf("Time preparing entropies for calc: %d\n", timePreparingEntropies);
        System.out.printf("Time finding consistent JD: %d\n", timeToGetConsistentJD);
        System.out.printf("Time spend calculating I-measure: %d\n", calculatingIMeasure);
        System.out.printf("Time spend cloning attributeSets: %d\n", AttributeSet.cloneTime);
        
        System.out.printf("Total time to mine JDs with LHS: %d\n", timeToMineJDsWithLHS);
        System.out.printf("Time to mine JDs with LHS without preparing entropies and scanning: %d\n", 
        		(timeToMineJDsWithLHS-(timeToGetConsistentJD+timePreparingEntropies)));
        System.out.printf("Time spent merging JDs: %d\n", JoinDependency.timeSpentMerging);
        
    }
    
    private static int getNumAtts(File csvFile) {
    	int retVal = 0;
    	try {
    		
    		BufferedReader  reader = Files.newBufferedReader(csvFile.toPath());
			String line = reader.readLine();
			String[] atts = line.split(",");
			return atts.length;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return retVal;
    }
   
}
