package uwdb.discovery.dependency.approximate.entropy;

//import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

import java.util.*;

public class InMemoryDataSet extends AbstractDataset {
    protected RelationSchema schema;
    protected int numAttributes;
    protected long[][] data;
    protected long numScans;
    protected long totalScanTime;


    public InMemoryDataSet(String fileName, RelationSchema schema)
    {
        super(schema);
        this.numAttributes = schema.getNumAttributes();
        numScans=0;
        totalScanTime=0;
    }
    public InMemoryDataSet(long[][] data, RelationSchema schema)
    {
        super(schema);
        this.numAttributes = schema.getNumAttributes();
        this.data = data;
    }

    public RelationSchema getSchema() {
        return schema;
    }

    public double computeEntropy(IAttributeSet subset) {
    	long startTime = System.currentTimeMillis();
        HashMap<Integer, Long> counts = new HashMap<Integer, Long>();
        for(int i = 0; i < data.length; i++)
        {
            addCount(counts, data[i], subset);
        }
        numScans++;
        long endTime = System.currentTimeMillis();
        totalScanTime += (endTime - startTime);
        
        return computeEntropyValue(counts, data.length);
    }

    public void computeEntropies(HashMap<IAttributeSet, Double> values) {
        HashMap<IAttributeSet, HashMap<Integer, Long>> counts = new HashMap<IAttributeSet, HashMap<Integer, Long>>();
        for(IAttributeSet attributeSet : values.keySet()) {
            counts.put(attributeSet, new HashMap<Integer, Long>());
        }

        for(int i = 0; i < data.length; i++)
        {
            for (IAttributeSet attributeSet : values.keySet())
            {
                addCount(counts.get(attributeSet), data[i], attributeSet);
            }
        }

        for (IAttributeSet attributeSet : values.keySet()) {
            HashMap<Integer, Long> countsMap = counts.get(attributeSet);
            double entropyValue = computeEntropyValue(countsMap, data.length);
            values.put(attributeSet, entropyValue);
        }
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
