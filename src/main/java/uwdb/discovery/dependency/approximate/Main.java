package uwdb.discovery.dependency.approximate;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.*;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.dependency.DependencyType;
import uwdb.discovery.dependency.approximate.common.dependency.FunctionalDependency;
import uwdb.discovery.dependency.approximate.common.sets.DependencySet;
import uwdb.discovery.dependency.approximate.entropy.ExternalFileDataSet;
import uwdb.discovery.dependency.approximate.entropy.IDataset;
import uwdb.discovery.dependency.approximate.inference.BeeriAlgorithmInference;
import uwdb.discovery.dependency.approximate.inference.IInferenceModuleFactory;
import uwdb.discovery.dependency.approximate.search.TopDownInductiveSearch;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVParser;

public class Main {

    public static void printLattice()
    {
        RelationSchema schema = new RelationSchema(4);
        FunctionalDependency.printLattice(schema, System.out);
    }

    public static void performSearch(String[] args)
    {
        String filePath = args[0];
        int numAttributes = Integer.valueOf(args[1]);
        RelationSchema schema = new RelationSchema(numAttributes);
        double alpha = 1E-4;
        ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);
        IInferenceModuleFactory inferenceModuleFactory = new BeeriAlgorithmInference.Factory(schema, alpha);
        TopDownInductiveSearch searchMVDs = new TopDownInductiveSearch(DependencyType.MULTIVALUED_DEPENDENCY, 
        		dataSet, inferenceModuleFactory, alpha);
        searchMVDs.search();        
        TopDownInductiveSearch searchFDs = new TopDownInductiveSearch(DependencyType.FUNCTIONAL_DEPENDENCY, 
        		dataSet, inferenceModuleFactory, alpha);
        searchFDs.search();        
                
        searchMVDs.printDiscoveredDependencies();
        searchMVDs.printRuntimeCharacteristics();
        
        searchFDs.printDiscoveredDependencies();
        searchFDs.printRuntimeCharacteristics();
    }
    
    
    public static void executeTestsSingleDataset(String dataSetPath, 
    		int numAttribtues, double[] thresholds, String outputDirPath, long timeoutInSec) {
    	
    	
    	Path inputPath = Paths.get(dataSetPath);
    	String inputFilename = inputPath.getFileName().toString();
    	String outputFileName = inputFilename+".out.csv";
    	Path outputPath= Paths.get(outputDirPath, outputFileName);    	
    	
    	System.out.println("Executing stepwise tests of " + dataSetPath + " with runWithTimeout!");
    	
    	//We have to create the CSVPrinter class object 
        Writer writer;
		try {
			writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE);
			CSVPrinter csvPrinter = 
					new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader
							("#Attribtues", "#Rows", "Threshold", "#MVDs", 
									"#File Scans","total file scan time",
									"total Running time","%scanning"));
			for(int i=0; i < thresholds.length ; i++) {
				double thresh = thresholds[i];				
				Boolean completed = false;
				Callable<TopDownInductiveSearch> execution = new Callable<TopDownInductiveSearch>() {
					 @Override
				        public TopDownInductiveSearch call() throws Exception
				        {
						    TopDownInductiveSearch retVal = executeTest(dataSetPath, numAttribtues, thresh);						    
						 	return retVal;
				        }
				};
				RunnableFuture future = new FutureTask(execution);
			    ExecutorService service = Executors.newSingleThreadExecutor();
			    service.execute(future);
			    TopDownInductiveSearch miningResult = null;
			    try
			    {
			    	miningResult = (TopDownInductiveSearch) future.get(timeoutInSec, TimeUnit.SECONDS);    // wait
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
    //execute the test and return all information
    public static TopDownInductiveSearch executeTest(String dataSetPath, int numAttribtues, 
    		double threshold) {
    	RelationSchema schema = new RelationSchema(numAttribtues);        
        ExternalFileDataSet dataSet = new ExternalFileDataSet(dataSetPath, schema);      
        TopDownInductiveSearch search = new TopDownInductiveSearch(DependencyType.MULTIVALUED_DEPENDENCY, 
        		dataSet, null, threshold);
        DependencySet minedMVDs = search.mineMVDs();
       // printMVDs(minedMVDs);    
        search.printRuntimeCharacteristics();
        return search;
    }
    
    //new code
    public static DependencySet mineMVDs(String[] args)
    {
        String filePath = args[0];
        int numAttributes = Integer.valueOf(args[1]);
        RelationSchema schema = new RelationSchema(numAttributes);
        double alpha = 2;
        ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);   
        TopDownInductiveSearch search = new TopDownInductiveSearch(DependencyType.MULTIVALUED_DEPENDENCY, 
        		dataSet, null, alpha);
        DependencySet minedMVDs = search.mineMVDs();
        printMVDs(minedMVDs);    
        search.printRuntimeCharacteristics();
        return minedMVDs;
    }
   
    public static DependencySet bruteForceMVDs(String[] args) {
    	String filePath = args[0];
        int numAttributes = Integer.valueOf(args[1]);
        RelationSchema schema = new RelationSchema(numAttributes);
        double alpha = 2;
        ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);
        IInferenceModuleFactory inferenceModuleFactory = new BeeriAlgorithmInference.Factory(schema, alpha);
        TopDownInductiveSearch search = new TopDownInductiveSearch(DependencyType.MULTIVALUED_DEPENDENCY, 
        		dataSet, inferenceModuleFactory, alpha);
        DependencySet bruteForceMVDs = search.bruteForce();
        printMVDs(bruteForceMVDs);
    //  search.printDiscoveredDependencies();
        search.printRuntimeCharacteristics();
        return bruteForceMVDs;
    }
    public static void printMVDs(DependencySet MVDs){
    	 Iterator<DataDependency> iterator = MVDs.iterator();
         while(iterator.hasNext())
         {
        	 DataDependency mvd = iterator.next();
             System.out.println(mvd.toString());
         }
    }

    
    public static void main(String[] args) {
    	String inDirectory = args[0];
    	String outDir = args[1];
    	File inDir = new File(inDirectory);    	
    	double[] thresholds = new double[] {0,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.5,0.6,0.7,
        		0.75,0.8, 0.9,1.0,1.1,1.15,1.3,1.4,1.5,1.6,2.0,2.5,3};
        
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
    public static void singleDatasetTest(String[] args)
    {
    	Boolean failed = false;
    	DependencySet miningAlg = mineMVDs(args);
    	DependencySet bruteForce = bruteForceMVDs(args);
    	if(bruteForce.size() != miningAlg.size()) {
    		System.out.println("wrong!");
    		failed = true;
    	}
    	Iterator<DataDependency> miningit = miningAlg.iterator();
    	while(miningit.hasNext()) {
    		DataDependency dd = miningit.next();
    		if(!bruteForce.contains(dd)) {
    			System.out.println("false positive:");
    			System.out.println(dd.toString());
    			failed = true;
    		}
    	}
    	Iterator<DataDependency> bruteForceit = bruteForce.iterator();
    	while(bruteForceit.hasNext()) {
    		DataDependency dd = bruteForceit.next();
    		if(!miningAlg.contains(dd)) {
    			System.out.println("false negative:");
    			System.out.println(dd.toString());
    			failed = true;
    		}
    	}
    	
    	if(!failed)
    		System.out.println("Passed test!");
    	
        
        
        //printLattice();
    }
}
