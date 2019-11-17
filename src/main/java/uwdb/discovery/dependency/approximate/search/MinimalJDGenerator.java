package uwdb.discovery.dependency.approximate.search;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
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
import uwdb.discovery.dependency.approximate.entropy.CompressedDB;
import uwdb.discovery.dependency.approximate.entropy.ExternalFileDataSet;
import uwdb.discovery.dependency.approximate.entropy.IDataset;
import uwdb.discovery.dependency.approximate.entropy.MasterCompressedDB;
//import uwdb.discovery.dependency.approximate.search.MinimalJDGenerator.AttributePair;

public class MinimalJDGenerator {

	
	protected double epsilon;    
    protected MasterCompressedDB mcDB;

    protected DependencySet queue;
    protected DependencySet discoveredDependencies;
    protected int numAttribtues;
    //protected Map<IAttributeSet, Set<JoinDependency>> MinedJDs;

    public int scanCount;    
    public long totalScanningTime;
    private Map<AttributePair, Set<IAttributeSet>> minPairwiseSeps;
    //private Set<IAttributeSet> initialSetOfReducedLHS;
    private Set<IAttributeSet> minedMinSeps;
    private static MinimalJDGenerator currentlyExecuting;
    private Set<JoinDependency> fullMVDsOfMinSeps;
    
    public Set<JoinDependency> MinedJDsFromMinSeps;
    
    public long timePreparingEntropies=0;
    public long timeToGetConsistentJD=0;
    public long calculatingIMeasure =0;
    public long timeToMineJDsWithLHS =0;
    public long timeInitMinSeps =0;
    public long totalRunningTime=0;
    
    public static int TIMEOUT = 20; //3600; //14400;
    
    public static volatile boolean STOP=false;
    
    private IAttributeSet allAttSet;
    boolean completedMiningAllMinSeps;
    boolean completedMiningAllFullMVDs;
    
    
    public static MinimalJDGenerator getCurrentExecution() {
    	return currentlyExecuting;
    }
    public static void setCurrentExecution(MinimalJDGenerator MJD) {
    	MinimalJDGenerator.currentlyExecuting=MJD;
    }
    //private IAttributeSet[] SingleAttSets;
     
    
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
    
    public MasterCompressedDB getDatasetObject() {
    	return mcDB;
    }
    
    public int getNumAttributes() {
    	return numAttribtues;
    }
    
    public Collection<IAttributeSet> getDiscoveredDataDependencies(){
    	return minedMinSeps;
    }
    public MinimalJDGenerator(MasterCompressedDB mcDB, double epsilon, int numAtts) {
    	this.epsilon=epsilon;
    	this.mcDB = mcDB;
    	this.numAttribtues = numAtts;
    	this.totalRunningTime = 0;
    //	MinedJDs = new HashMap<IAttributeSet, Set<JoinDependency>>();
    //	initialSetOfReducedLHS = new HashSet<IAttributeSet>();
    	minPairwiseSeps = new HashMap<AttributePair, Set<IAttributeSet>>();
    	minedMinSeps=new HashSet<IAttributeSet>();    	
    	allAttSet = new AttributeSet(numAtts);
    	allAttSet.add(0, numAtts);
    	MinedJDsFromMinSeps = new HashSet<JoinDependency>();
    	fullMVDsOfMinSeps = new HashSet<JoinDependency>();
    	completedMiningAllMinSeps = false;;
        completedMiningAllFullMVDs = false;
   
    }
    
    public boolean completedMiningAllMinSeps() {
    	return completedMiningAllMinSeps;
    }
    
    public boolean completedMiningAllFullMVDs() {
    	return completedMiningAllFullMVDs;
    }
    public void initMinSeps() {
    	this.initSetOfMinSeps();
    }
    public static double calculateJDMeasure(JoinDependency JD, MasterCompressedDB mcDB) {
    		
    	Iterator<IAttributeSet> it = JD.componentIterator();
    	
    	double componentParts = 0;
    	IAttributeSet toCalc = JD.getlhs().clone();
    	int numComponents=0;
    	while(it.hasNext()) {    		
    		toCalc.or(it.next());
    		componentParts+=mcDB.getEntropy(toCalc);
    		toCalc.intersectNonConst(JD.getlhs());
    		numComponents++;
    	}
    	
    	double lhsPart = mcDB.getEntropy(JD.getlhs());
    	double totalEntropy = mcDB.getTotalEntropy();
    	    	
    	double JDMeasure = componentParts-(numComponents-1.0)*lhsPart-totalEntropy;
    	if(JDMeasure < 0.0) {
    		assert Math.abs(JDMeasure)<0.0001;
    		JDMeasure=0.0;
    	}
    	JD.setMeasure(JDMeasure);
    	return JDMeasure;
    }
    
    private static double ACCURACY = 0.00001; 
    public static boolean isGreaterThanEpsilon(double imeasure, double alpha) {
    	double diff = imeasure-alpha;
    	return (diff>ACCURACY);
    }
    
    public double calculateElementalMVD(int X, int Y, IAttributeSet rest) {
    	long startTime = System.currentTimeMillis();
    	double lhse = mcDB.getEntropy(rest); //entropy of lhs
    	rest.add(X);
    	double firste = mcDB.getEntropy(rest); 
    	rest.remove(X);
    	rest.add(Y);
    	double seconde = mcDB.getEntropy(rest); 
    	rest.remove(Y);
    	double alle = mcDB.getEntropy(allAttSet);
    	double retVal = firste+seconde-lhse-alle;    	
    	calculatingIMeasure+=(System.currentTimeMillis()-startTime);
    	return retVal;
    	
    }
    public double calcuateIMeasure(DataDependency MVD) {
    	long startTime = System.currentTimeMillis();
    	IAttributeSet first = MVD.rhs;
    	IAttributeSet rhslhs = first.union(MVD.lhs);
    	IAttributeSet second = rhslhs.complement();
    	double retVal = calcuateIMeasure(first,second,MVD.lhs, this.mcDB );
    	calculatingIMeasure+=(System.currentTimeMillis()-startTime);
    	return retVal;
    }
    		
    public double calcuateIMeasure(IAttributeSet first, IAttributeSet second, 
    			IAttributeSet lhs,MasterCompressedDB mcDB) {
    	long startTime = System.currentTimeMillis();
    	IAttributeSet lhsClone = lhs.clone();
    	double lhse = mcDB.getEntropy(lhs); //entropy of lhs
    	lhsClone.or(first);  //here, lhsClone = lhs\cup first
    	double firste = mcDB.getEntropy(lhsClone); 
    	lhsClone.or(second); //here, lhsClone = lhs\cup first \ cup second
    	double alle = mcDB.getEntropy(lhsClone);
    	lhsClone.intersectNonConst(lhs); //here lhsClone = lhs
    	lhsClone.or(second); //here lhsClone = lhs \cup second
    	double seconde = mcDB.getEntropy(lhsClone);
    	
    	/*
    	IAttributeSet firstlhs = first.union(lhs);
    	IAttributeSet secondlhs = second.union(lhs);
    	IAttributeSet allVars = secondlhs.union(first);
    	*/
    	
    	double retVal = firste+seconde-lhse-alle;
    	calculatingIMeasure+=(System.currentTimeMillis()-startTime);
    	
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
    
    private JoinDependency mostSpecificJD(Collection<IAttributeSet> components, IAttributeSet lhs) {
    	
    	IAttributeSet allComponents = new AttributeSet(this.numAttribtues);
    	for(IAttributeSet component : components) {
    		allComponents.or(component);
    	}
    	allComponents.or(lhs);
    	IAttributeSet left = allComponents.complement();
    	JoinDependency JD = new JoinDependency(lhs);
    	for(int i = left.nextAttribute(0); i >=0 ; i = left.nextAttribute(i+1)) {
    		IAttributeSet singleiAttSet = new AttributeSet(lhs.length());
    		singleiAttSet.add(i);
    		JD.addComponent(singleiAttSet);
    	}
    	for(IAttributeSet component : components) {
    		JD.addComponent(component);
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
    
    private class MergedJD{
    	public JoinDependency JD;
    	public int i;
    	public int j;
    	public MergedJD(JoinDependency JD, int i, int j) {
    		this.JD = JD;
    		this.i = i;
    		this.j=j;
    	}
    }
    public Set<JoinDependency> mineAllJDsWithLHSDFS(int AttX, int AttY, 
    		IAttributeSet lhs, int limit, JoinDependency JDToStart) {
    	long startTime = System.currentTimeMillis();
    	
    	JoinDependency JD0 = JDToStart;
    	if(JD0==null)
    		return new HashSet<JoinDependency>(1); //return empty set
    	
    	double jdMeasure = MinimalJDGenerator.calculateJDMeasure(JDToStart, mcDB);
    	//Every component (above 2) can reduce at most epsilon if eliminated.
    	//So, if removing all components does not lead to a good bound then abort.
    	int componentFactor = JD0.getComponents().size()-1;    	
    	if(isGreaterThanEpsilon(jdMeasure, componentFactor*this.epsilon))
    		return new HashSet<JoinDependency>(1); //return empty set
    	
    	Stack<MergedJD> Q = new Stack<MergedJD>();
    	Q.push(new MergedJD(JD0,0,0));
    	Set<JoinDependency> P = new HashSet<JoinDependency>();
    	while(!Q.isEmpty() && !STOP) {
    		MergedJD mergedJD = Q.pop();
    		double iJD = MinimalJDGenerator.calculateJDMeasure(mergedJD.JD,mcDB);
    		if(!isGreaterThanEpsilon(iJD, this.epsilon)) {    		    	
    			P.add(mergedJD.JD);
    			if(limit>0 && P.size() >= limit) {
    				break;
    			}
    		}
    		else { //try to traverse one of the merges
    			int numComponents = mergedJD.JD.numComponents();    			
    			if(numComponents > 2) { //if it is <= 2 then we know that there is no JD    				
    				IAttributeSet[] JDArray = mergedJD.JD.toArray();    				
    				int newi=0;
    				int newj=0;
    				if(mergedJD.j < numComponents-1) {
    					newi = mergedJD.i;
    					newj = mergedJD.j+1;
    				}
    				else if(mergedJD.i < numComponents-2) {
    					newi = mergedJD.i+1;
    					newj = newi+1;
    				}
    				if((newi < newj) && 
    				   (JDArray[newi].contains(AttX) && JDArray[newj].contains(AttY)) ||
					   (JDArray[newi].contains(AttY) && JDArray[newj].contains(AttX))) {
    					if(newj < numComponents-1)
    						newj++;
    					else if(newi+1 < numComponents-2){
    						newi=newi+1;
    						newj=newi+1;
    					}
    					else {
    						newi=0;
    						newj=0;
    					}
    				}
    				if(newi < newj) {
    					mergedJD.i=newi;
    					mergedJD.j = newj;
    					Q.push(mergedJD);
    					JoinDependency mergedij = mergedJD.JD.mergeComponents(newi, newj);	
    					JoinDependency mergedijConsistent = getConsistentJDCandidate(AttX, AttY, lhs,epsilon, mergedij,false);
    					if(mergedijConsistent!= null) {
    						double mergedJDMeasure = MinimalJDGenerator.calculateJDMeasure(mergedijConsistent, mcDB);
    				    	if(!isGreaterThanEpsilon(mergedJDMeasure, (mergedijConsistent.getComponents().size()-2)*this.epsilon))    				    		
    				    		Q.push(new MergedJD(mergedijConsistent,0,0));
    					}
    				}
    			}
    		}
    	}
    	timeToMineJDsWithLHS+=(System.currentTimeMillis()-startTime);
    	return P;
    }
    
    
    public Set<JoinDependency> mineAllJDsWithLHSBFS(int AttX, int AttY, 
    		IAttributeSet lhs, int limit, JoinDependency JDToStart) {
    	long startTime = System.currentTimeMillis();
    	
    	//JoinDependency JD0 = getConsistentJDCandidate(AttX, AttY, lhs,epsilon, JDToStart,false);
    	JoinDependency JD0 = JDToStart;
    	if(JD0==null)
    		return new HashSet<JoinDependency>(1); //return empty set
    	
    	double jdMeasure = MinimalJDGenerator.calculateJDMeasure(JDToStart, mcDB);
    	if(isGreaterThanEpsilon(jdMeasure, (JD0.getComponents().size()-2)*this.epsilon))
    		return new HashSet<JoinDependency>(1); //return empty set
    	
    	Queue<JoinDependency> Q = new LinkedList<JoinDependency>();
    	Q.add(JD0);
    	Set<JoinDependency> P = new HashSet<JoinDependency>();
    	while(!Q.isEmpty()) {
    		JoinDependency JD = Q.poll();
    		double iJD = MinimalJDGenerator.calculateJDMeasure(JD,mcDB);
    		if(!isGreaterThanEpsilon(iJD, this.epsilon)) {
    		//if(iJD <= epsilon) {    	
    			P.add(JD);
    			if(limit>0 && P.size() >= limit) {
    				break;
    			}
    		}
    		else {
    			int numComponents = JD.numComponents();    			
    			if(numComponents > 2) { //if it is <= 2 then we know that there is no JD
    				IAttributeSet[] JDArray = JD.toArray();    				
	    			for(int i=0; i < numComponents ; i++) {
	    				for(int j=i+1 ; j < numComponents; j++) {
	    					if((JDArray[i].contains(AttX) && JDArray[j].contains(AttY)) ||
	    							(JDArray[i].contains(AttY) && JDArray[j].contains(AttX)))
	    						continue; //we do not want to merge AttX and AttY
	    					
	    					JoinDependency mergedij = JD.mergeComponents(i, j);	    					
	    					//changed
	    					//Q.add(mergedij);
	    					
	    					JoinDependency mergedijConsistent = getConsistentJDCandidate(AttX, AttY, lhs,epsilon, mergedij,false);
	    					if(mergedijConsistent != null) {
	    						Q.add(mergedijConsistent);	    	
	    						break;
	    						    					
	    					}
	    				}
	    			}
	    			
    			}
    		}
    	}
    	timeToMineJDsWithLHS+=(System.currentTimeMillis()-startTime);
    	return P;
    }
    
    
    
    public Set<JoinDependency> mineAllJDsWithLHS(IAttributeSet lhs, int limit) {
    	long startTime = System.currentTimeMillis();
    	
    	//JoinDependency JD0 = mostSpecificJD(lhs);
    	JoinDependency mostSpecific = mostSpecificJD(lhs);    	
    	JoinDependency JD0 = getConsistentJDCandidate(lhs, epsilon, mostSpecific);
    	if(JD0==null)
    		return new HashSet<JoinDependency>(1); //return empty set    	
    	
    	
    	Queue<JoinDependency> Q = new LinkedList<JoinDependency>();
    	Q.add(JD0);
    	Set<JoinDependency> P = new HashSet<JoinDependency>();
    	while(!Q.isEmpty()) {
    		JoinDependency JD = Q.poll();
    		double iJD = MinimalJDGenerator.calculateJDMeasure(JD,mcDB);
    		if(!isGreaterThanEpsilon(iJD,this.epsilon)) {    		
    			P.add(JD);
    			if(limit>0 && P.size() >= limit) {
    				break;
    			}
    		}
    		else {
    			int numComponents = JD.numComponents();    			
    			if(numComponents > 2) { //if it is <= 2 then we know that there is no JD    				    				
	    			for(int i=0; i < numComponents ; i++) {
	    				for(int j=i+1 ; j < numComponents; j++) {
	    					JoinDependency mergedij = JD.mergeComponents(i, j);
	    					//Q.add(mergedij);
	    					
	    					JoinDependency mergedijConsistent = getConsistentJDCandidate(lhs,epsilon, mergedij);
	    					if(mergedijConsistent != null) {
	    						Q.add(mergedijConsistent);
	    					}	    					
	    					
	    				}
	    			}
	    			
    			}
    		}
    	}
    	timeToMineJDsWithLHS+=(System.currentTimeMillis()-startTime);
    	return P;
    }
    
    private double[] getComponentEntropies(JoinDependency JD) {
    	IAttributeSet[] JDCOmponents = JD.toArray();    	
    	double[] retVal = new double[JD.getComponents().size()];
    	for(int i=0 ; i < retVal.length ; i++) { //compute the entropy of JDCOmponents[i]\cup JD.getLHS()
    		IAttributeSet AS = JDCOmponents[i];
    		AS.or(JD.getlhs());
    		retVal[i] = mcDB.getEntropy(AS);    		
    	}
    	for(int att = JD.getlhs().nextAttribute(0) ; att >= 0 ; att = JD.getlhs().nextAttribute(att+1)) {
    		for(int i=0 ; i < JD.getComponents().size() ; i++) {
    			JDCOmponents[i].remove(att); //return to previous situation without allocating memory
    		}
		}
    	return retVal;
    }
    
    private IAttributeSet mergeIfNeeded(JoinDependency JD, 
    		double[] componentEntropies, double lhsEntropy) {
    	IAttributeSet[] JDCOmponents = JD.getComponentArray();
    	
    	//int length = componentEntropies.length - numMerges;
    	int length = JD.numOfComponents();
    	for(int i=0 ; i < length ; i++) {		
    			IAttributeSet first = JDCOmponents[i];
    			double firste = componentEntropies[i];
			for(int j=i+1 ; j < length ; j++) {
    			IAttributeSet second = JDCOmponents[j];
				double seconde = componentEntropies[j];
				IAttributeSet firstSecondCombined = first.union(second);
				firstSecondCombined.or(JD.getlhs());
				double mergedEntropy = mcDB.getEntropy(firstSecondCombined);
    			//double imeasure = calcuateIMeasure(first,second, JD.getlhs(),mcDB);
				double imeasure = firste+seconde-(mergedEntropy+lhsEntropy);
    			if(isGreaterThanEpsilon(imeasure, this.epsilon)) {    		
    				//i and j must appear together in any constalation
    				IAttributeSet merged = JD.mergeComponentsNonConst(i,j);    				
    				int lastIndex = length - 1;    				
    				componentEntropies[i] = mergedEntropy;
    				componentEntropies[j] = componentEntropies[lastIndex];    				
    				return merged;
    			}
    		}
		}
    	return null;
    }
    
      
    private JoinDependency getConsistentJDCandidate(int AttX, int AttY, 
    		IAttributeSet lhs, double epsilon, JoinDependency JDtoStart, boolean ignoreXYPair) {
    	//JoinDependency JD = mostSpecificJD(lhs); //starting point
    	long startTime = System.currentTimeMillis();
    	JoinDependency JD = JDtoStart;
    	double lhse = mcDB.getEntropy(JD.getlhs());
    	
    	
    	//check if even possible to have them separated.    	
    	IAttributeSet localLHS = lhs;
    	localLHS.add(AttX);
    	double lhsXe = mcDB.getEntropy(localLHS);
    	lhs.add(AttY);
    	double XYLHSEntropy = mcDB.getEntropy(localLHS);
    	lhs.remove(AttX);
    	double lhsYe = mcDB.getEntropy(localLHS);    
    	lhs.remove(AttY);
    	double basicIMeasure = lhsXe+lhsYe-lhse-XYLHSEntropy;
    	if(isGreaterThanEpsilon(basicIMeasure, this.epsilon)) {
    		return null;
    	}
    	
    	//IAttributeSet[] JDComponents = JD.toArray();    	
    	double[] componentEntropies = getComponentEntropies(JD);
    	
    	IAttributeSet mergedComponent = mergeIfNeeded(JD, componentEntropies, lhse );    	 
    	while(mergedComponent != null) {    		
    		if((mergedComponent.contains(AttX) && mergedComponent.contains(AttY))) {
    			timeToGetConsistentJD+=(System.currentTimeMillis()-startTime);
				return null;	
    		}
    		mergedComponent = mergeIfNeeded(JD, componentEntropies, lhse);    	 
    	}
    	timeToGetConsistentJD+=(System.currentTimeMillis()-startTime);
    	if(JD.numComponents() > 1)
    		return JD;
    	return null;    	
    }
    
    
    private JoinDependency getConsistentJDCandidate(IAttributeSet lhs, double epsilon, JoinDependency startingJD) {
    	//JoinDependency JD = mostSpecificJD(lhs); //starting point
    	long startTime = System.currentTimeMillis();
    	JoinDependency JD = startingJD;
    	double lhse = mcDB.getEntropy(JD.getlhs());    	
    	  	
    	double[] componentEntropies = getComponentEntropies(JD);    	
    	IAttributeSet mergedComponent = mergeIfNeeded(JD, componentEntropies, lhse );    	 
    	while(mergedComponent != null) {
    		mergedComponent = mergeIfNeeded(JD, componentEntropies, lhse);    	 
    	}
    	timeToGetConsistentJD+=(System.currentTimeMillis()-startTime);
    	if(JD.numComponents() > 1)
    		return JD;
    	return null;    	
    }
    
    
     
    private IAttributeSet reduceToMinJD(int AttX, int AttY, IAttributeSet X) {    	
    	IAttributeSet Y = X.clone();
    	Set<JoinDependency> JD_Y = mineAllJDsWithLHSDFS(AttX, AttY, Y,1, mostSpecificJD(X));    	
    	if(JD_Y.isEmpty()) return null;
    	
    	for(int i= Y.nextAttribute(0); i >= 0 ; i = Y.nextAttribute(i+1)) {
    		Y.remove(i);    			
    		JD_Y = mineAllJDsWithLHSDFS(AttX, AttY, Y,1,mostSpecificJD(Y));
    		if(JD_Y.isEmpty()) { //equivalent to: if nonempty, then remove
    			Y.add(i);   	
    		}    		
    	}
    	return Y;
    }
    
    private JoinDependency reduceToMinJDReturnJD(int AttX, int AttY, IAttributeSet X) {    	
    	IAttributeSet Y = X.clone();    	
    	JoinDependency consistentJD = getConsistentJDCandidate(AttX, AttY, Y, epsilon, mostSpecificJD(Y), false);
    	if(consistentJD == null) return null;
    	Set<JoinDependency> JD_Y = mineAllJDsWithLHSDFS(AttX, AttY, Y,1, consistentJD);    	
    	if(JD_Y.isEmpty()) return null;
    	
    	
    	JoinDependency toReturn = JD_Y.iterator().next();
    	for(int i= Y.nextAttribute(0); i >= 0 ; i = Y.nextAttribute(i+1)) {
    		Y.remove(i);
    		//JD_Y = mineAllJDsWithLHS(AttX, AttY, Y,1,mostSpecificJD(Y));
    		//JoinDependency mostSpecificToStart = mostSpecificJD(consistentJD.getComponents(),Y);
    		JoinDependency mostSpecificToStart = mostSpecificJD(Y);
    		consistentJD = getConsistentJDCandidate(AttX, AttY, Y, epsilon, mostSpecificToStart, false);
    		if(consistentJD == null) {
    			Y.add(i);    
    			continue;
    		}
    		JD_Y = mineAllJDsWithLHSDFS(AttX, AttY, Y,1,consistentJD);
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
    private void initSetOfMinSeps() {
    	long startTime = System.currentTimeMillis();
   /* 	HashMap<AttributePair, IAttributeSet> startingPoints = 
    			new HashMap<MinimalJDGenerator.AttributePair, IAttributeSet>();
    */			
    	IAttributeSet AS = new AttributeSet(this.numAttribtues);
    	AS.add(0, numAttribtues);
    	IAttributeSet startingPoint;
    	for(int i=0 ; i < numAttribtues && !STOP; i++) {
    		AS.remove(i);
    		for(int j=i+1 ; j < numAttribtues && !STOP ; j++) {
    			AttributePair XY = new AttributePair(i, j);
				if(minPairwiseSeps.containsKey(XY)) {					
					continue; //already have some initial minimal separator
				}			
				AS.remove(j);
		   		double imeasure = calculateElementalMVD(i,j,AS);	    			
		   		if(isGreaterThanEpsilon(imeasure,this.epsilon)) {
		   			AS.add(j);
		   			continue;    			
		   		}			
		   		startingPoint = AS;
		
				
    			HashSet<IAttributeSet> XYMinSps = new HashSet<IAttributeSet>();        			
				JoinDependency JD = reduceToMinJDReturnJD(i, j, startingPoint);
				if(JD == null) {
					AS.add(j);
    				continue;   
				}				
				XYMinSps.add(JD.getlhs());
				minPairwiseSeps.put(XY, XYMinSps);
			//	updateMinSepsBasedOnMinedJD(XY,JD);
			//	updateStartingPoints(JD, startingPoints);
				minedMinSeps.add(JD.getlhs()); 
				MinedJDsFromMinSeps.add(JD);
    			AS.add(j);
    		}
    		AS.add(i);
    	}
    	timeInitMinSeps += (System.currentTimeMillis()-startTime);	
    }
    
    private void updateStartingPoints(JoinDependency JD, 
    		Map<AttributePair, IAttributeSet> startingPoints) {
    	
    	int lhsCardinality = JD.getlhs().cardinality();
    	IAttributeSet cloned = JD.getlhs().clone();
    	for(IAttributeSet component: JD.getComponents()) {
    		cloned.or(component);
    		IAttributeSet rest = cloned.complement();
    		for(int i=rest.nextAttribute(0) ; i <= 0 ; i = rest.nextAttribute(i+1)) {
    			for(int j=component.nextAttribute(0); j <=0 ; j =component.nextAttribute(j+1)) {
    				AttributePair AP = new AttributePair(i, j);
    				if(startingPoints.containsKey(AP)) {
    					IAttributeSet APSP = startingPoints.get(AP);
    					if(APSP.cardinality() > lhsCardinality) {
    						startingPoints.put(AP,JD.getlhs().clone());
    					}
    				}
    				else {
    					startingPoints.put(AP,JD.getlhs().clone());
    				}
    			}
    		}
    		cloned.intersectNonConst(JD.getlhs());
    	}
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
    	while(!done && !STOP) {
    		JoinDependency CtrJD = null;
    		while(minTransversals.hasNext()) {
    			IAttributeSet minTransversal = minTransversals.next();
    			assert minTransversal.contains(AttX)==false;
    			assert minTransversal.contains(AttY)==false;
    			
    			minTransversal.flip(0, numAttribtues); //take the complement
    			minTransversal.remove(AttX);
    			minTransversal.remove(AttY);
    			
    			JoinDependency JD0 = getConsistentJDCandidate(AttX, AttY, minTransversal, 
    					epsilon, mostSpecificJD(minTransversal), false);
    			if(JD0!= null) {
	    			Set<JoinDependency> minTransversalJDs = 
	    					mineAllJDsWithLHSDFS(AttX, AttY, minTransversal,
	    							1,JD0);
	    			if(!minTransversalJDs.isEmpty()) {
	    				CtrJD = minTransversalJDs.iterator().next();
	    				break;
	    			}
    			}    			
    		}
    		if(CtrJD == null) {
    			done = true;    			
    		}
    		else {
    			//IAttributeSet minLHS = reduceToMinJD(AttX, AttY,CtrJD.getlhs());
    			JoinDependency newJD = reduceToMinJDReturnJD(AttX, AttY, CtrJD.getlhs());
    			
    			/*
    			Set<JoinDependency> minTransversalJDs = mineAllJDsWithLHS(AttX, AttY, minLHS,
    					1,mostSpecificJD(minLHS));
    			assert (!minTransversalJDs.isEmpty());
    			JoinDependency newJD = minTransversalJDs.iterator().next();   
    			 		*/	
    			minedMinSeps.add(newJD.getlhs());
    			MinedJDsFromMinSeps.add(newJD);
    			//updateMinSepsBasedOnMinedJD(XYPair, newJD);
    			//minTransversals = new Transversals(minXYSeps, numAttribtues);
    			minTransversals.addHyperedge(newJD.getlhs());
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
    	if(XComponent.intersects(YComponent)){
    		System.out.println("Error");
    	}
    	for(int i = XComponent.nextAttribute(0) ; i >= 0 ; i = XComponent.nextAttribute(i+1)) {
    		for(int j = YComponent.nextAttribute(0) ; j >= 0 ; j = YComponent.nextAttribute(j+1)) {
    			AttributePair ijPair = new AttributePair(i, j);
    			addMinSepForPair(ijPair, JD.getlhs());
    		}
    	}
    	
    }
   
    private Set<IAttributeSet> mineAllMinSeps() {
    	    	
    //	Set<AttributePair> seperablePairs = minPairwiseSeps.keySet();
    	for(int i=0 ; i < numAttribtues  && !STOP; i++)
    		for(int j=i+1 ; j< numAttribtues  && !STOP ; j++)
    			mineAllMinSeps(i, j);
    /*	for(AttributePair XYPair: seperablePairs) {
    		mineAllMinSeps(XYPair.AttX, XYPair.AttY);    		
    	}*/
    	this.completedMiningAllMinSeps=true;
      	
    	Set<IAttributeSet> retVal = new HashSet<IAttributeSet>();
    	Collection<Set<IAttributeSet>> minimalSeparators = minPairwiseSeps.values();
    	for(Set<IAttributeSet> SomePairMinSeps: minimalSeparators) {
    		retVal.addAll(SomePairMinSeps);
    	}    	
    	return retVal;   	
    	
    }
    
    private void mineAllFullMVDs() {
    	
    	fullMVDsOfMinSeps.addAll(this.MinedJDsFromMinSeps);
    	for(Entry<AttributePair, Set<IAttributeSet>> minSepsForPair: minPairwiseSeps.entrySet()) {
    		AttributePair pair = minSepsForPair.getKey();
    		Set<IAttributeSet> pairMinSep=minSepsForPair.getValue();
    		for(IAttributeSet minSep : pairMinSep) {    			
    			JoinDependency mostSpecificJD = mostSpecificJD(minSep);
    			JoinDependency consistentJD = getConsistentJDCandidate(pair.AttX, pair.AttY, minSep,epsilon, mostSpecificJD,false);
    			Set<JoinDependency> fullMVDsMinSep = mineAllJDsWithLHSDFS(pair.AttX, pair.AttY, minSep,
					Integer.MAX_VALUE,consistentJD);
    			fullMVDsOfMinSeps.addAll(fullMVDsMinSep);
    			if(STOP) {
    				break;
    			}
    		}
    		if(STOP) {
				break;
			}
    	}
    	if(!STOP)
    		this.completedMiningAllFullMVDs=true;
    }
    
    public static void printJDsToFile(Collection<JoinDependency> minJDs, String JDFilePath, int numAttributes) {
    	File fout = new File(JDFilePath);
    	FileOutputStream fos=null;
		try {
			fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			for(JoinDependency JD : minJDs) {
				StringBuilder sb = new StringBuilder();
				sb.append(numAttributes);
				sb.append(",").append(JD.toString());
				bw.write(sb.toString());
				bw.newLine();
			}
			bw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}      
    	
    	
    }
    
    /*
    public void getJDsFromMinSeps(int limit) {
    	for(IAttributeSet separator : minedMinSeps) {
    		Set<JoinDependency> sepJDs = mineAllJDsWithLHS(separator, limit);
    		if(sepJDs != null) {
    			MinedJDsFromMinSeps.addAll(sepJDs);
    		}
    	}    	
    }*/
    
    private static int LIMIT_JDS=10;
    public static MinimalJDGenerator executeTest(String dataSetPath, int numAttribtues, 
    		int rangeSize, double threshold, boolean mineAllFullMVDs) {    	
    	long startTime = System.currentTimeMillis();    	
    	MinimalJDGenerator.STOP=false;
    	System.out.println(Thread.currentThread().getId() +  ":Starting Execution " + dataSetPath + 
    			" threshold=" + threshold);
    	//RelationSchema schema = new RelationSchema(numAttribtues);
        //ExternalFileDataSet dataSet = new ExternalFileDataSet(dataSetPath, schema);        
        MasterCompressedDB mcDB = new MasterCompressedDB(dataSetPath, numAttribtues, rangeSize, false);
        mcDB.initDBs();
        System.out.println(Thread.currentThread().getId()+ ": after initDBs");
//        mcDB.getTotalEntropy();
        //mcDB.precomputeMostSpecificSeps(dataSet);
        //System.out.println("after precomputeMostSpecificSeps");
        MinimalJDGenerator miner = new MinimalJDGenerator(mcDB,threshold,numAttribtues);
        MinimalJDGenerator.setCurrentExecution(miner);
        miner.initMinSeps();
        System.out.println(Thread.currentThread().getId()+ ": calling mineAllMinSeps...");
        miner.mineAllMinSeps();
        System.out.println(Thread.currentThread().getId() + ": Completed mining all minSeps...");
        if(mineAllFullMVDs) {
        	miner.mineAllFullMVDs();
        	System.out.println(Thread.currentThread().getId() + ": Completed mining all full MVDs...");
        }
        miner.totalRunningTime+=(System.currentTimeMillis()-startTime);
        miner.printRuntimeCharacteristics();        
        MasterCompressedDB.shutdown();        
        return miner;
    }
    
    public static void executeTestsSingleDataset(String dataSetPath, 
    		int numAttribtues, int[] rangeSizes, double[] thresholds, 
    		String outputDirPath, long[] timeouts, boolean mineAllFullMVDs) {
    	//timeoutInSec
    	
    	Path inputPath = Paths.get(dataSetPath);
    	String inputFilename = inputPath.getFileName().toString();
    	String outputFileName = inputFilename+".out.csv";
    	Path outputPath= Paths.get(outputDirPath, outputFileName);    	
    	
    	
    	
    	System.out.println("Executing tests of " + dataSetPath + "with " + 
    	numAttribtues + " attributes, with runWithTimeout!");
    	
    	//We have to create the CSVPrinter class object 
        Writer writer;
        CSVPrinter csvPrinter = null;
		try {
			if(Files.exists(outputPath, LinkOption.NOFOLLOW_LINKS)) {
				writer =Files.newBufferedWriter(outputPath,StandardOpenOption.APPEND,StandardOpenOption.SYNC);
				csvPrinter = 
						new CSVPrinter(writer, CSVFormat.DEFAULT);
			}
			else {
				writer = Files.newBufferedWriter(outputPath,StandardOpenOption.CREATE,StandardOpenOption.SYNC);
				csvPrinter = 
					new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader
							("#Attribtues", "#Rows", "Range Size", 
									"Threshold", "Timeout (sec)", "Completed MinSeps", "#Minimal Separators",
									"Completed FullMVDs", "#Full MVDs",
									"Time Building range Tbls",
									"#In-Memory Queries Issued","Query Time",									
									"total Running time","%querying",
									"Cached Entropy Objects",
									"Number of tuples processed during Entropy Computation"));
			}
			for(long timeoutInSec : timeouts) {				
				for(int j=0 ; j < rangeSizes.length ; j++) {
					int rangeSize = rangeSizes[j];
					for(int i=0; i < thresholds.length ; i++) {
						String minSepFileName = inputFilename+".TO."+timeoutInSec+".RANGE."+rangeSizes[j]+
								".THRESH."+thresholds[i]+".sep";
						Path minSepOutPath= Paths.get(outputDirPath, minSepFileName);
						
						double thresh = thresholds[i];				
						Boolean completed = true;
						Callable<MinimalJDGenerator> execution = new Callable<MinimalJDGenerator>() {
							 @Override
						        public MinimalJDGenerator call() throws Exception
						        {
								 	MinimalJDGenerator retVal = executeTest(dataSetPath, numAttribtues, 
								 					rangeSize,thresh,mineAllFullMVDs);						    
								 	return retVal;
						        }
						};
						RunnableFuture<MinimalJDGenerator> future = new FutureTask<MinimalJDGenerator>(execution);
					    ExecutorService service = Executors.newSingleThreadExecutor();				    
					    service.execute(future);
					    MinimalJDGenerator miningResult = null;
					    boolean cancelled=false;
					    try
					    {
					    	MinimalJDGenerator.setCurrentExecution(null);
					    	miningResult = (MinimalJDGenerator) future.get(timeoutInSec, TimeUnit.SECONDS);    // wait
					    	
					    }
					    catch (TimeoutException ex)
					    {
					    	MinimalJDGenerator.STOP=true;
					    	completed = false;
					    	System.out.println(Thread.currentThread().getId() + ":TIMEOUT!, stopping execution");		
					    	cancelled = future.cancel(true);		
					    	Thread.sleep(5000);
					    	//fetch mining result
					    	miningResult = MinimalJDGenerator.getCurrentExecution();
					    	
					    	
					    	//MasterCompressedDB.shutdown();
					        // timed out. Try to stop the code if possible.
					        //cancelled = future.cancel(true);
					        
					        
					    } catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ExecutionException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					    service.shutdown();
					   			   
				//		if(completed) {
					    if(miningResult!= null) {
							MasterCompressedDB DatasetObj = miningResult.getDatasetObject();
							long totalRunTime = miningResult.totalRunningTime;
							double percentQuerying = (double)DatasetObj.totalTimeSpentQuerying()/(double)totalRunTime;
							percentQuerying = percentQuerying*100.0;
							//Writing records in the generated CSV file
				            csvPrinter.printRecord(DatasetObj.getNumAttributes(), DatasetObj.getNumRows(), rangeSize,
				            		thresh, timeoutInSec, Boolean.toString(miningResult.completedMiningAllMinSeps), 
				            		miningResult.getDiscoveredDataDependencies().size(),
				            		Boolean.toString((miningResult.completedMiningAllFullMVDs || !mineAllFullMVDs)),
				            		miningResult.fullMVDsOfMinSeps.size(),
				            		DatasetObj.totalTimeBuildingRangeTables(),
				            		DatasetObj.totalNumQueriesIsuues(), DatasetObj.totalTimeSpentQuerying(),			            	
				            		totalRunTime, percentQuerying,
				            		DatasetObj.numberOfCachedEntropies(),
				            		DatasetObj.numOfTuplesProcessedDuringEntropyComputation());
				            csvPrinter.flush();			            				            
				            //miningResult.getJDsFromMinSeps(LIMIT_JDS);
				            //MinimalJDGenerator.printJDsToFile(miningResult.MinedJDsFromMinSeps,
				            //		minSepOutPath.toString(), numAttribtues);
				            miningResult.fullMVDsOfMinSeps.addAll(miningResult.MinedJDsFromMinSeps);
				            MinimalJDGenerator.printJDsToFile(miningResult.fullMVDsOfMinSeps,
				            		minSepOutPath.toString(), numAttribtues);
				            miningResult.printRuntimeCharacteristics();
					    }
					    else {
					    	System.out.println(Thread.currentThread().getId() + ": Did not fully create MinimalJDGenerator object within timeout");
					        MasterCompressedDB.shutdown();
					    }
				//		}
				//		else {						
													
							
				//		}
			    //        csvPrinter.flush();
			        //    MasterCompressedDB.shutdown();
					}
				}
			}
			csvPrinter.close();
		} catch (IOException e) {			
			// TODO Auto-generated catch block
			e.printStackTrace();
		}       
		catch (Exception e) {			
			MasterCompressedDB.shutdown();
		}       

    }
    
    public static void main(String[] args) {
    	
    //	testMinSeps2(args[0],Integer.parseInt(args[1]),0.4);
    	
    	String inDirectory = args[0];
    	String outDir = args[1];
    	String[] thresholds =  args[2].split(",");
    	double[] dblThresholds = new double[thresholds.length];
    	for(int i=0 ; i < thresholds.length ; i++) {
    		dblThresholds[i] = Double.valueOf(thresholds[i]);
    	}
    	String[] ranges =  args[3].split(",");    	
    	int[] intRanges = new int[ranges.length];
    	for(int i=0 ; i < ranges.length ; i++) {
    		intRanges[i] = Integer.valueOf(ranges[i]);
    	}
    	String[] timeouts =  args[4].split(",");
    	long[] dblTimeout = new long[timeouts.length];
    	for(int i=0 ; i < timeouts.length ; i++) {
    		dblTimeout[i] = Long.valueOf(timeouts[i]);
    	}
    	
    	Integer range = new Integer(args[3]);
    	Long timeout = new Long(args[4]);
    	
    	boolean mineFullMVDs=false; 
    	if(args.length > 5){
    		if(!args[5].isEmpty()) {
    			mineFullMVDs = Boolean.valueOf(args[5]);    			
    		}
    	}
    	File inDir = new File(inDirectory);    	
       /* double[] thresholds = new double[] {0,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,
        		0.55,0.6,0.65,0.7,0.75,0.8,0.9,1.0,1.1,1.15,1.3,1.4,1.5,1.6,2.0,2.5,3,24};
        */
//        double[] thresholds = new double[] {0.0,0.001,0.005,0.01,0.015,0.02,0.05,0.1};
    // 	double[] thresholds = new double[] {1.0,2.0,3.0,24};
   //     long[] timeouts = new long[] {1800,3600,7200,14400};
       // long[] timeouts = new long[] {5,10,15,20,30};
      //  long[] timeouts = new long[] {60, 300, 500, 1500};
        
    	File[] inFiles = inDir.listFiles();
    	Arrays.sort(inFiles, new Comparator<File>(){
    	    public int compare(File f1, File f2)
    	    {
    	        return Long.valueOf(f1.length()).compareTo(f2.length());
    	    } });
    	
        for(File inFile: inFiles) {        	
    		int numAttributes = getNumAtts(inFile);        		
            executeTestsSingleDataset(inFile.getAbsolutePath(), 
    		numAttributes, intRanges, dblThresholds,outDir, dblTimeout,mineFullMVDs);        	
        }
    }
    
    private static int[] getRangeSizes(int numOfAttributes) {
    	//assume at most 15 joins
    	Set<Integer> ranges = new HashSet<Integer>();
    	for(int i=1 ; i <= 15 ; i++) {
    		int numTbls = i+1;
    		int rangeSize = (numOfAttributes/numTbls);
    		if(rangeSize <= 0) {
    			break;
    		}
    		ranges.add(rangeSize);
    		
    	}
    	int[] retVal = new int[ranges.size()+1];
    	int i=0;
    	for(Integer rangeSize : ranges) {
    		retVal[i++]=rangeSize;
    	}
    	int maxRange = Math.min(16, numOfAttributes);
    	retVal[i] = maxRange;
    	return retVal;
    }
    /*
    public static void main(String[] args) {
    	testMinSeps(args);
    }*/
    public static boolean testMinSeps(String[] args, double alpha) {
    	
    	DependencySet minedMVDs = mineMVDs(args,alpha);
    	Iterator<DataDependency> MVDIt = minedMVDs.iterator();
    	
    	    	
    	JoinDependency.resetMergeTime();
    	long startTime = System.currentTimeMillis();
    	String filePath = args[0];
        int numAttributes = Integer.valueOf(args[1]);
        RelationSchema schema = new RelationSchema(numAttributes);
        ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);  
        /*
         * MasterCompressedDB mcDB = new MasterCompressedDB(dataSetPath, numAttribtues, rangeSize, false);
        mcDB.initDBs();        
        MinimalJDGenerator miner = new MinimalJDGenerator(mcDB,threshold,numAttribtues);     
        miner.initMinSeps();        
        miner.mineAllMinSeps();
        
         */
        
        
        MasterCompressedDB mcDB = new MasterCompressedDB(filePath, numAttributes, 5, false);
        mcDB.initDBs();
    //    mcDB.precomputeMostSpecificSeps(dataSet);        
        MinimalJDGenerator JDGenLHS = new MinimalJDGenerator(mcDB,alpha, numAttributes);
        JDGenLHS.initMinSeps();
        Set<IAttributeSet> JDminSeps = JDGenLHS.mineAllMinSeps();
        System.out.println("initialization time " + (System.currentTimeMillis()-startTime));
        
        
        long runtime = System.currentTimeMillis()-startTime;
         
        //get ground truth minimal separators
        Iterator<IAttributeSet> lhsIterator = minedMVDs.determinantIterator();
        Set<IAttributeSet> determinantSet = new HashSet<IAttributeSet>();
        HashSet<IAttributeSet> minSepsGT = new HashSet<IAttributeSet>();
        while(lhsIterator.hasNext()) {
        	IAttributeSet test = lhsIterator.next();
        	determinantSet.add(test);
        }
    	
        //ignore due to small entropy measurement errors
        DependencySet GTtoIgnore = new DependencySet();
        while(MVDIt.hasNext())
    	{
    		DataDependency dd = MVDIt.next();
    		double ddImeasure = JDGenLHS.calcuateIMeasure(dd);
    		if(ddImeasure > alpha) {
    			GTtoIgnore.add(dd);
    			assert(ddImeasure-alpha < 0.0001);
    		}
    	}
        Iterator<DataDependency> ignoreIt = GTtoIgnore.iterator();
        while(ignoreIt.hasNext()) {
        	minedMVDs.remove(ignoreIt.next());
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
				System.out.println("Error: Separator " + 
						CurrJDMinSep.toString() + " should not have been mined");		
				MinimalJDGenerator.testMinSeparator(JDGenLHS, CurrJDMinSep, alpha, dataSet);
				passed = false;				
			}
		}
		
		Iterator<IAttributeSet> minSepsGTIT = minSepsGT.iterator();
		while(minSepsGTIT.hasNext()) {
			IAttributeSet CurrGTSep = minSepsGTIT.next();
			if(!JDminSeps.contains(CurrGTSep) && !(CurrGTSep.cardinality()==CurrGTSep.length()-1)) {    				
				System.out.println("Error: Separator " + 
						CurrGTSep.toString() + " should have been mined, but wasn't");
				MinimalJDGenerator.testMinSeparator(JDGenLHS, CurrGTSep, alpha, dataSet);
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
		mcDB.shutdown();
		return passed;
    }
    
   
public static boolean testMinSeps2(String filePath, int numAttributes, double alpha) {
    	
    	System.out.println("Starting test: " + filePath + " num atts: " + numAttributes + " thresh: " + alpha);   	    	
    	JoinDependency.resetMergeTime();
    	long startTime = System.currentTimeMillis();
    //	String filePath = args[0];
     //   int numAttributes = Integer.valueOf(args[1]);
   //     RelationSchema schema = new RelationSchema(numAttributes);
    //    ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);  
    	MinimalJDGenerator.STOP=false;
    	MinimalJDGenerator.currentlyExecuting=null;
        MasterCompressedDB mcDB = new MasterCompressedDB(filePath, numAttributes, 10, false);
        mcDB.initDBs();
 //       mcDB.precomputeMostSpecificSeps(dataSet);
        AttributeSet.resetConeTime();
        MinimalJDGenerator JDGenLHS = new MinimalJDGenerator(mcDB,alpha, numAttributes);
        JDGenLHS.initMinSeps();
        System.out.println("initialization time " + (System.currentTimeMillis()-startTime));
        Set<IAttributeSet> JDminSeps = JDGenLHS.mineAllMinSeps();
        
        long runtime = System.currentTimeMillis()-startTime;
        int totalSepsMined=0;
        int totalInconsistentByGetConsistentJD=0;
        int totalInconsistentByEmptyJDsWithLHS=0;
        int totalNonMinimal=0;
        System.out.println("threshold: " + alpha + ", num mined: " + JDminSeps.size());
        Map<AttributePair, Set<IAttributeSet>> pairwiseSeps = JDGenLHS.minPairwiseSeps;
        for(Entry<AttributePair,Set<IAttributeSet>> entry:  pairwiseSeps.entrySet()) {
        	AttributePair XY = entry.getKey();
        	Set<IAttributeSet> pairSeps = entry.getValue();
        	totalSepsMined+=pairSeps.size();
        	for(IAttributeSet sep : pairSeps) {
        		JoinDependency consistentJD = JDGenLHS.getConsistentJDCandidate(XY.AttX, XY.AttY, sep, alpha, JDGenLHS.mostSpecificJD(sep), false);
        		if(consistentJD == null) {
        			//System.out.println("Mined an inconsistent JD");
        			totalInconsistentByGetConsistentJD++;
        			continue;
        		}
        		Set<JoinDependency> JD_sep = JDGenLHS.mineAllJDsWithLHSDFS(XY.AttX, XY.AttY, sep, 1, consistentJD);
        		if(JD_sep == null || JD_sep.isEmpty()) {
        			//System.out.println("Mined an inconsistent JD");
        			totalInconsistentByEmptyJDsWithLHS++;
        			continue;
        		}
        		//now, make sure it's minimal
        		JoinDependency reducedJD = JDGenLHS.reduceToMinJDReturnJD(XY.AttX, XY.AttY, sep);
        		if(sep.contains(reducedJD.getlhs()) && sep.cardinality() > reducedJD.getlhs().cardinality()) {
        			totalNonMinimal++;
        			//System.out.println("Mined a non-minimal separator JD");        			
        		}
        	}
        }
        boolean passed = (totalInconsistentByGetConsistentJD==0 && 
        		totalInconsistentByEmptyJDsWithLHS==0 && totalNonMinimal==0);        
        if(passed) {
        	System.out.println("test passed");
        	System.out.println("totalSepsMined: " + totalSepsMined);
        }
        else {
        	System.out.println("totalSepsMined: " + totalSepsMined);
        	System.out.println("totalInconsistentByGetConsistentJD: " + totalInconsistentByGetConsistentJD);
        	System.out.println("totalInconsistentByEmptyJDsWithLHS: " + totalInconsistentByEmptyJDsWithLHS);
        	System.out.println("totalNonMinimal: " + totalNonMinimal);
        }
        MasterCompressedDB.shutdown();
		return passed;
    }

    private static void testMinSeparator(MinimalJDGenerator MJG, 
    		IAttributeSet lhs, double alpha, AbstractDataset DatasetToCompare) {    	
    	Set<JoinDependency> JDs = MJG.mineAllJDsWithLHS(lhs, 1000);
    	if(JDs.isEmpty()) {
    		System.out.println(lhs.toString() + " cannot be a minimal separator ");    		
    	}
    	//verify that all returned JDs have the appropriate measure
    	for(JoinDependency JD : JDs) {
    		double JDMeasure = JD.getMeasure();
    		double calculatedMeasure = MJG.calculateJDMeasure(JD, MJG.mcDB);
    		if(calculatedMeasure > alpha) {
    			System.out.println("mined bad JD");
    		}
    		double OldDatasetComputation = DatasetToCompare.computeJD(JD);
    		if(OldDatasetComputation > alpha) {
    			System.out.println("Old algorithm mined bad JD");
    		}
    	}
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
        System.out.println(Thread.currentThread().getId() + ": Runtime Characteristics:");
        System.out.printf(Thread.currentThread().getId() + ": Num Attributes: %d\n", mcDB.getNumAttributes());
        System.out.printf(Thread.currentThread().getId() +  ": Num Rows: %d\n", mcDB.getNumRows());
        System.out.printf(Thread.currentThread().getId() +  ": Threshold: %f\n", epsilon);        
        System.out.printf(Thread.currentThread().getId() +  ": Num In-memory Queries issued: %d\n", mcDB.totalNumQueriesIsuues());
        System.out.printf(Thread.currentThread().getId() + ": Time spend processing queries: %d\n", mcDB.totalTimeSpentQuerying());
        System.out.printf(Thread.currentThread().getId() +" Time finding consistent JD: %d\n", timeToGetConsistentJD);
        System.out.printf(Thread.currentThread().getId() +" Time spend calculating I-measure: %d\n", calculatingIMeasure);
        System.out.printf(Thread.currentThread().getId() +" Time spend initializing minimal separators: %d\n", timeInitMinSeps);
        System.out.printf(Thread.currentThread().getId() +" Number of separators mined: %d\n", this.minedMinSeps.size());
        System.out.printf(Thread.currentThread().getId() +" Number of JDs corresponding to minSeps: %d\n", this.MinedJDsFromMinSeps.size());
        System.out.printf(Thread.currentThread().getId() +" Number of full MVDs corresponding to minSeps: %d\n", this.fullMVDsOfMinSeps.size());
        System.out.printf(Thread.currentThread().getId() +" Avg. JDs per minsep: %f\n", 
        						(double)this.MinedJDsFromMinSeps.size()/(double)minedMinSeps.size());
        
       
        
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
