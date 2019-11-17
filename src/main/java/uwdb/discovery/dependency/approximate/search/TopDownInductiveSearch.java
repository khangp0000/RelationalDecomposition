package uwdb.discovery.dependency.approximate.search;

import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.dependency.DependencyType;
import uwdb.discovery.dependency.approximate.common.dependency.FunctionalDependency;
import uwdb.discovery.dependency.approximate.common.dependency.MultivaluedDependency;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IDependencySet;
import uwdb.discovery.dependency.approximate.entropy.IDataset;
import uwdb.discovery.dependency.approximate.inference.IInferenceModule;
import uwdb.discovery.dependency.approximate.inference.IInferenceModuleFactory;
import uwdb.discovery.dependency.approximate.inference.LatticeOrderingInference;

import java.util.HashSet;
import java.util.Iterator;

public class TopDownInductiveSearch implements ISearchAlgorithm {

    protected double alpha;
    protected RelationSchema schema;
    protected DependencyType type;

    protected IInferenceModule orderInferenceModule;
    protected IInferenceModule inferenceModule;
    protected IDataset queryModule;

    protected DependencySet queue;
    protected DependencySet specializeList;
    protected DependencySet discoveredDependencies;
    protected DependencySet borderMVDs;

    public int inferredCount;
    public int scanCount;
    public long totalInferenceTime;
    public long totalRunningTime;
    public long totalScanningTime;

    public TopDownInductiveSearch(DependencyType type, IDataset queryModule, IInferenceModuleFactory inferenceModuleFactory, double alpha)
    {
        this.alpha = alpha;
        this.type = type;
        this.inferredCount = 0;
        this.scanCount = 0;
        this.queryModule = queryModule;

        this.schema = queryModule.getSchema();
        this.discoveredDependencies = new DependencySet();
        if(inferenceModuleFactory!= null)
        	this.inferenceModule = inferenceModuleFactory.getInferenceModule(discoveredDependencies);
        this.orderInferenceModule = new LatticeOrderingInference(schema, discoveredDependencies, alpha);
        this.queue = new DependencySet();
        this.totalInferenceTime = 0;
        this.totalRunningTime = 0;
        this.totalScanningTime = 0;
    }

    protected void initialize()
    {
        switch (type)
        {
            case FUNCTIONAL_DEPENDENCY:
                FunctionalDependency.addMostGeneralDependencies(schema, queue);
                break;
            case MULTIVALUED_DEPENDENCY:
                MultivaluedDependency.addMostGeneralDependencies(schema, queue);
                break;
        }
    }

    protected void processQueue()
    {
        //infer bounds from already discovered dependencies using Beeri's algorithm
        if(type == DependencyType.MULTIVALUED_DEPENDENCY && !discoveredDependencies.isEmpty())
        {
            long startTime = System.currentTimeMillis();
            //inferenceModule.doBatchInference(queue);
            long endTime = System.currentTimeMillis();
            totalInferenceTime += (endTime - startTime);
        }

        //prepare the list to check from data
        IDependencySet checkFromData = new DependencySet();
        Iterator<DataDependency> iterator = queue.iterator();
        while (iterator.hasNext())
        {
            DataDependency dep = iterator.next();
            /*
            if(dep.isApproximate(alpha) == Status.UNKNOWN)
            {
                checkFromData.add(dep);
            }
            */
            if(dep.getMeasure() < 0)
            {
                checkFromData.add(dep);
            }
            else
            {
                inferredCount++;
            }
        }

        //compute the value from data
        if(checkFromData.size() > 0)
        {
            scanCount++;
            long startTime = System.currentTimeMillis();
            queryModule.computeMeasures(checkFromData);
            long endTime = System.currentTimeMillis();
            totalScanningTime += (endTime - startTime);
        }
    }

    protected void createQueue()
    {
        queue = new DependencySet();
        Iterator<DataDependency> iterator = specializeList.iterator();
        while (iterator.hasNext()) {
            DataDependency parent = iterator.next();
            parent.addSpecializations(orderInferenceModule, queue);
        }
    }

    public DependencySet getDiscoveredDataDependencies()
    {
        return discoveredDependencies;
    }

    public void search()
    {
        long startTime = System.currentTimeMillis();
        initialize();
        int level = 0;
        while(!queue.isEmpty())
        {
            level++;
            processQueue();
            specializeList = new DependencySet();

            Iterator<DataDependency> iterator = queue.iterator();
            while (iterator.hasNext())
            {
                DataDependency dependency = iterator.next();
                if(dependency.getMeasure() <= alpha) {
                	discoveredDependencies.add(dependency);
                }
                else {
                	specializeList.add(dependency);
                }
                /*
                switch (dependency.isApproximate(alpha))
                {
                    case TRUE:
                        discoveredDependencies.add(dependency);
                        break;
                    case FALSE:
                        specializeList.add(dependency);
                        break;
                    case UNKNOWN:
                        throw new RuntimeException("Must be decided by now!");
                }
                */
            }

            //printDiscoveredDependencies();

            createQueue();
        }

        long endTime = System.currentTimeMillis();
        totalRunningTime = endTime - startTime;
    }
    
    public DependencySet bruteForce() {
    	long startTime = System.currentTimeMillis();
    	DependencySet allPossibleMVDs = CandidateGenerator.getAllMVDCandidates(schema.getNumAttributes());
    	FunctionalDependency.addMostSpecificDependencies(schema, allPossibleMVDs);
    	
    	Iterator<DataDependency> mvdIterator = allPossibleMVDs.iterator();
    	while(mvdIterator.hasNext()) {
    		DataDependency mvd = mvdIterator.next();
    		queryModule.computeMVD(mvd);
    		scanCount++;
    		if(mvd.getMeasure() <= alpha) {
    			discoveredDependencies.add(mvd);
    		}
    	}
    	long endTime = System.currentTimeMillis();
        totalRunningTime = endTime - startTime;
    	return discoveredDependencies;
    }
    private void getAccurate(double alpha, DependencySet dependencies, DependencySet accurateDependencies) {
    	Iterator<DataDependency> it = dependencies.iterator(); 
    	while(it.hasNext()) {
    		DataDependency MVDToCheck = it.next();
    		queryModule.computeMVD(MVDToCheck);
			scanCount++;
			if(MVDToCheck.getMeasure() <= alpha) {
				accurateDependencies.add(MVDToCheck);
			}			
    	}
    }
    
    public DependencySet mineMVDs() {
    	DependencySet Q=new DependencySet();
    	DependencySet P=new DependencySet();
    	DependencySet SaturatedFDs=new DependencySet();
    	DependencySet mostSepcificMVDs=new DependencySet();
    	
    	Boolean foundMVD=false;
    	long startTime = System.currentTimeMillis();
    	MultivaluedDependency.addMostSpecificDependencies(schema, mostSepcificMVDs);
    	FunctionalDependency.addMostSpecificDependencies(schema, SaturatedFDs);
    	
    	getAccurate(alpha, mostSepcificMVDs, Q);
    	getAccurate(alpha, SaturatedFDs, discoveredDependencies);
    	discoveredDependencies.add(Q);
    	
    	HashSet<IAttributeSet> determinantsProcessed = new HashSet<IAttributeSet>();
    	
    	for(int k = schema.getNumAttributes()-3 ; k >= 0; k--) {
    		foundMVD=false;
    		Iterator<IAttributeSet> QdeterminantSet=Q.determinantIterator();
    		while(QdeterminantSet.hasNext()) {
    			IAttributeSet determinant = QdeterminantSet.next();
    			for(int i = 1 ; i<=k+1 ; i++ ) {
    				IAttributeSet newDeterminant = determinant.clone();
    				newDeterminant.removeItemIdx(i);
    				if(!determinantsProcessed.contains(newDeterminant)) {
    					CandidateGenerator CS = new CandidateGenerator(newDeterminant);
    					while(CS.hasNext()) {
    						DataDependency mvdToCheck = CS.next();
    						if(!prune(mvdToCheck,Q)) {
    							queryModule.computeMVD(mvdToCheck);
    							scanCount++;
    							if(mvdToCheck.getMeasure() <= alpha) {
    								P.add(mvdToCheck);
    								discoveredDependencies.add(mvdToCheck);
    								foundMVD = true;
    							}
    						}    						
    					}
    					determinantsProcessed.add(newDeterminant);
    				}    				
    			}    			
    		}
    		if(!foundMVD) break;
    		foundMVD=false;    		
    		Q = P;
    		P=new DependencySet();
    		
    	}
    	borderMVDs=Q;
    	long endTime = System.currentTimeMillis();
        totalRunningTime = endTime - startTime;
        
    	return discoveredDependencies;
    	
    }
   
    private Boolean prune(DataDependency mvdToCheck, 
    	DependencySet prevIteration) {
    	
    	IAttributeSet mvdRHS1= mvdToCheck.rhs.clone();
    	IAttributeSet mvdLHS= mvdToCheck.lhs.clone();
    	IAttributeSet mvdRHS2= mvdRHS1.union(mvdLHS);
    	mvdRHS2 = mvdRHS2.complement();
    	//special hack for saturated FDs
    	if(mvdRHS2.cardinality() ==0) {
    		mvdRHS2= mvdRHS1.clone();
    	}
    	
    	if(mvdRHS1.cardinality() > 1) {
	    	while(mvdRHS1.hasNext()) {
	    		Integer RHSVar=mvdRHS1.next();
	    		mvdRHS1.flip(RHSVar);
	    		mvdLHS.flip(RHSVar);
	    		if(!prevIteration.contains(mvdLHS, mvdRHS1) & !prevIteration.contains(mvdLHS, mvdRHS2))    				
	    			return true;
	    		mvdRHS1.flip(RHSVar);
	    		mvdLHS.flip(RHSVar);
	    	}
    	}
    	
    	if(mvdRHS2.cardinality() > 1) {
	    	while(mvdRHS2.hasNext()) {
	    		Integer RHSVar=mvdRHS2.next();
	    		mvdRHS2.flip(RHSVar);
	    		mvdLHS.flip(RHSVar);
	    		if(!prevIteration.contains(mvdLHS, mvdRHS2) & !prevIteration.contains(mvdLHS, mvdRHS1) )    				
	    			return true;
	    		mvdRHS2.flip(RHSVar);
	    		mvdLHS.flip(RHSVar);
	    	}
    	}
    	
    	return false;
    }

    public void printDiscoveredDependencies()
    {
        Iterator<DataDependency> iterator = discoveredDependencies.iterator();
        while(iterator.hasNext())
        {
            System.out.println(iterator.next());
        }
    }
    
    public IDataset getDatasetObject() {
    	return queryModule;
    }

    public void printRuntimeCharacteristics()
    {
        System.out.println("Runtime Characteristics:");
        System.out.printf("Dependency Type: %s\n", type.toString());
        System.out.printf("Num Attributes: %d\n", queryModule.getNumAttributes());
        System.out.printf("Num Rows: %d\n", queryModule.getNumRows());
        System.out.printf("Threshold: %f\n", alpha);
        System.out.printf("Total Discovered: %d\n", discoveredDependencies.size());
        System.out.printf("Total Border MVDs: %d\n", (borderMVDs!= null) ? borderMVDs.size(): 0);
        System.out.printf("Num Inferred: %d\n", inferredCount);
        System.out.printf("Num File Scans: %d\n", queryModule.getNumDBScans());
        System.out.printf("Running Time: %d\n", totalRunningTime);
        System.out.printf("File Scan Time: %d\n", queryModule.getTotalScanTime());
        System.out.printf("Inference Time: %d\n", totalInferenceTime);
    }
}
