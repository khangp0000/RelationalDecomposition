package uwdb.discovery.dependency.approximate.entropy;

//import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class ExternalFileDataSet extends AbstractDataset {
    protected boolean hasHeader;
    protected String fileName;
    protected BufferedReader reader;
    protected HashMap<IAttributeSet, Double> cachedEntropies;
    protected long numScans;
    protected long totalScanTime;
    public long preparingMapForComputingEntropies;

    public ExternalFileDataSet(String fileName, RelationSchema schema) {
        this(fileName, schema, false);
        
        
    }

    public ExternalFileDataSet(String fileName, RelationSchema schema, boolean hasHeader) {
        super(schema);
        this.fileName = fileName;
        this.hasHeader = hasHeader;
        try {
            this.reader = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        numScans = 0;
        totalScanTime=0;
        cachedEntropies = new HashMap<IAttributeSet, Double>();
    }

    public RelationSchema getSchema() {
        return schema;
    }

    public double computeEntropy(IAttributeSet subset) {
    	//TODO: check if this caching helps...
    	if(cachedEntropies.containsKey(subset)) {
    		return cachedEntropies.get(subset);
    	}
    	long startTime = System.currentTimeMillis();
        
        long numLines = 0;
        String[] tempData = new String[numAttributes];
        String line;
        HashMap<Integer, Long> counts = new HashMap<Integer, Long>();
        try {
            reader = new BufferedReader(new FileReader(fileName));
            if(hasHeader)
            {
                line = reader.readLine();
            }
            while((line  = reader.readLine()) != null)
            {
                parseLine(line, tempData);
                addCount(counts, tempData, subset);
                numLines += 1;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        double retVal = computeEntropyValue(counts, numLines);
        cachedEntropies.put(subset, retVal);
        numScans++;
        if(numRows == 0) {
            numRows = numLines;
        }
        long endTime = System.currentTimeMillis();
        totalScanTime += (endTime - startTime);
        
        return retVal;
    }

    protected void parseLine(String line, String[] destinationArray)
    {
        String[] parts = line.split(",");
        for(int i = 0; i < numAttributes; i++) {
            destinationArray[i] = parts[i].trim();
        }
    }

    private void updateMapBasedOnCachedEntropies(Map<IAttributeSet, Double> values) {
    	   	
    	for (IAttributeSet attributeSet : values.keySet()) {
    		if(cachedEntropies.containsKey(attributeSet)) {
    			values.replace(attributeSet, cachedEntropies.get(attributeSet));
    		}
    		else {
    			if(attributeSet.cardinality() == attributeSet.length()) {
    				values.replace(attributeSet, getTotalEntropy());
    			}
    			else if(attributeSet.cardinality() == 0){
    				values.replace(attributeSet, 0.0);
    			}
    			else {
    				values.replace(attributeSet, -1.0);
    			}
    		}
    	}
    	
    }
    public void computeEntropies(HashMap<IAttributeSet, Double> values) {
        long numLines = 0;
        String[] lineData = new String[numAttributes];
        String line;
        long startTime = System.currentTimeMillis();
        updateMapBasedOnCachedEntropies(values);
        
        HashMap<IAttributeSet, HashMap<Integer, Long>> counts = new HashMap<IAttributeSet, HashMap<Integer, Long>>();
        for(Entry<IAttributeSet, Double> entry: values.entrySet()) {
        	if(entry.getValue() < 0) {
        		counts.put(entry.getKey(), new HashMap<Integer, Long>());
        	}
        }
        if(counts.isEmpty())
        	return; //all entropies were cached.
        preparingMapForComputingEntropies = (System.currentTimeMillis()-startTime);
        /*
        for(IAttributeSet attributeSet : values.keySet()) {
            counts.put(attributeSet, new HashMap<Integer, Long>());
        }*/

        startTime = System.currentTimeMillis();
        
        try {
            reader = new BufferedReader(new FileReader(fileName));
            while((line  = reader.readLine()) != null) {
                parseLine(line, lineData);
                for(Entry<IAttributeSet, HashMap<Integer, Long>> countEntry: counts.entrySet()) {
                	addCount(countEntry.getValue(), lineData, countEntry.getKey());
                }
                /*
                for (IAttributeSet attributeSet : values.keySet()) {
                    addCount(counts.get(attributeSet), lineData, attributeSet);
                }*/
                numLines += 1;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        totalScanTime += (endTime - startTime);

        for (IAttributeSet attributeSet : counts.keySet()) {
            HashMap<Integer, Long> countsMap = counts.get(attributeSet);
            double entropyValue = computeEntropyValue(countsMap, numLines);
            values.put(attributeSet, entropyValue);
            cachedEntropies.put(attributeSet, entropyValue);
        }

        if(numRows == 0) {
            numRows = numLines;
        }
        numScans++;
    }

   
	@Override
	public long getNumDBScans() {
		return numScans;
	}

	@Override
	public long getTotalScanTime() {
		return totalScanTime;
	}
}
