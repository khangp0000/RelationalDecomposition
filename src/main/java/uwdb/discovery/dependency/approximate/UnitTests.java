package uwdb.discovery.dependency.approximate;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;

import uwdb.discovery.dependency.approximate.search.MinimalJDGenerator;

public class UnitTests {

	public static void main(String[] args) {
		String inDirectory = args[0];    	
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
        		System.out.println("testing dataset: " + inFile.getName());
        		for(double alpha : thresholds) {        			
        			boolean passed = MinimalJDGenerator.testMinSeps2(inFile.getAbsolutePath(),
        					numAttributes, alpha);
        			System.out.println("threshold: " + alpha + ":" + (passed ? "Passed" : "Failed"));
        		}
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
}
