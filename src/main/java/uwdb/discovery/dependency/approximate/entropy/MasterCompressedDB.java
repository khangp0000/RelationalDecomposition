package uwdb.discovery.dependency.approximate.entropy;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

public class MasterCompressedDB {
	
	//public static final int MAX_ATTS = 2;
	private int numOfAttribtues;
	private Map<IAttributeSet, CompressedDB> rangeAttMap;
	private Map<IAttributeSet,Double> EntropyMap;	  
	Connection DBConnection;
	long numLines;
	long numQueriesIssued=0;
	long totalTimeQuerying =0;
	long totalTimeFetchingEntropy=0;
	long totalTimeBuildingRangeTables=0;
	long tuplesReadDuringEntropyComputation=0;
	private IAttributeSet allVars;
	int RangeSize;
	private double totalEntropy = -1;
	
	public MasterCompressedDB(String fileName, int numOfAttribtues,  int RangeSize, boolean hasHeader){
		this.numOfAttribtues=numOfAttribtues;
		this.RangeSize=RangeSize;
		int numOfRanges = numOfAttribtues/RangeSize+1;
		rangeAttMap = new HashMap<IAttributeSet, CompressedDB>(numOfRanges);
		EntropyMap = new HashMap<IAttributeSet, Double>();
		EntropyMap.put(new AttributeSet(numOfAttribtues), 0.0);
		
		for(int j=0 ; j < numOfAttribtues; j+= RangeSize) {
			int start = j;
			int end = Math.min(j+RangeSize,numOfAttribtues) ;
			IAttributeSet jRange = new AttributeSet(numOfAttribtues);
			jRange.add(start, end); //sets the bits from start (inclusive) to end (exclusive)
			CompressedDB cDB = new CompressedDB(fileName, numOfAttribtues, hasHeader, start, end);
			rangeAttMap.put(jRange,cDB);
		}
		DBConnection = CompressedDB.getDBConnection();
		allVars = new AttributeSet(numOfAttribtues);
		allVars.add(0, numOfAttribtues);
	}
	
	public void precomputeMostSpecificSeps(AbstractDataset IDS) {
		HashMap<IAttributeSet, Double> values = new HashMap<IAttributeSet, Double>(numOfAttribtues*(numOfAttribtues+1));
		IAttributeSet AS = new AttributeSet(numOfAttribtues);
		AS.add(0, numOfAttribtues);
		for(int i=0 ; i < numOfAttribtues ; i++) {
			AS.remove(i);
			values.put(AS.clone(), -1.0);
			for(int j= i+1 ; j < numOfAttribtues ; j++) {
				AS.remove(j);
				values.put(AS.clone(), -1.0);
				AS.add(j);
			}
			AS.add(i);
		}
		IDS.computeEntropies(values);
		EntropyMap.putAll(values);		
	}
	
	public void initDBs() {
		long startTime = System.currentTimeMillis();
		for(CompressedDB cDB : rangeAttMap.values()) {
			cDB.init();
			cDB.computeAllEntropies();
			numLines = cDB.numLines;
		}
		totalTimeBuildingRangeTables = System.currentTimeMillis()-startTime;
	}
	
	public double getTotalEntropy() {
		//return CompressedDB.log2(numLines);
		totalEntropy = CompressedDB.log2(numLines);
		return totalEntropy;
		/*
		if(totalEntropy < 0)
			totalEntropy = getEntropy(allVars);
		return totalEntropy;
		*/
	}
	
	 public int getNumAttributes() {
	    	return numOfAttribtues;
	 }
	 
	 public long getNumRows() {
		 return numLines;
	 }
	
	 public long totalTimeFetchingEntropy() {
		 return totalTimeFetchingEntropy;
	 }
	 public long totalNumQueriesIsuues() {
		 return numQueriesIssued;
	 }
	 
	 public long totalTimeSpentQuerying() {
		 return totalTimeQuerying;
	 }
	 
	 public long totalTimeBuildingRangeTables() {
		 return totalTimeBuildingRangeTables;
	 }
	 
	public long numberOfCachedEntropies() {
		return EntropyMap.size();
	}
	
	public long numOfTuplesProcessedDuringEntropyComputation() {
		return tuplesReadDuringEntropyComputation;
	}
	public double getEntropy(IAttributeSet attSet) {
		long startTime = System.currentTimeMillis();
		double retVal=0;
		
		if(EntropyMap.containsKey(attSet)) {
			retVal = EntropyMap.get(attSet);
			totalTimeFetchingEntropy+=(System.currentTimeMillis()-startTime);			
			return retVal;
		}
		IAttributeSet cloned = attSet.clone();
		//collect the table names to join
		HashSet<String> tblNames = new HashSet<String>(rangeAttMap.size());		
		for(IAttributeSet rangeSet: rangeAttMap.keySet()) {
			cloned.or(attSet); //return to normal
			if(!cloned.intersects(rangeSet)) {				
				continue; //does not contain any attribute from this range
			}
			CompressedDB cDB = rangeAttMap.get(rangeSet);
			if(rangeSet.contains(cloned)) {				
				retVal = cDB.getEntropy(cloned);
				totalTimeFetchingEntropy+=(System.currentTimeMillis()-startTime);
				return 	retVal;			
			}			
			cloned.intersectNonConst(rangeSet);
			String cDBTbleName = CompressedDB.tblNameForAttSet(cloned);
			if(!cDB.doesTableExist(cDBTbleName) /*||
					cDB.isTblEmpty(cDBTbleName)*/) {
				//EntropyMap.put(attSet,cDB.totalEntropy());
				totalTimeFetchingEntropy+=(System.currentTimeMillis()-startTime);				 
				return cDB.totalEntropy();				
			}			
			tblNames.add(cDBTbleName);			
		}
		totalTimeFetchingEntropy+=(System.currentTimeMillis()-startTime);
		startTime = System.currentTimeMillis();
		//now, execute the query
		//generate the CONCAT(tbl1.val,...,tblk.val) string
		StringBuilder cnctsb = new StringBuilder("CONCAT( ");
		StringBuilder fromClause = new StringBuilder(" FROM ");
		int i=1;
		for(String tblName : tblNames) {		
			cnctsb.append(tblName);
			fromClause.append(tblName);
			if(i < tblNames.size()) {
				cnctsb.append(".val, ");
				fromClause.append(", ");
			}
			else {
				cnctsb.append(".val");
			}
			i++;
		}
		cnctsb.append(")");
		StringBuilder whereClause = new StringBuilder(" WHERE ");
		//need to read table names into array
		String[] tblnamesArr = new String[tblNames.size()];
		i=0;
		for(String tblName : tblNames) {
			tblnamesArr[i++]=tblName;
		}
		
		for(i=0 ; i < tblnamesArr.length-2 ; i++) {
			whereClause.append(tblnamesArr[i]);
			whereClause.append(".tid=");
			whereClause.append(tblnamesArr[i+1]);
			whereClause.append(".tid AND ");
		}
		
		whereClause.append(tblnamesArr[tblnamesArr.length-2]);
		whereClause.append(".tid=");
		whereClause.append(tblnamesArr[tblnamesArr.length-1]);
		whereClause.append(".tid");
		
		
		String cntTblName = CompressedDB.tblCNTNameForAttSet(attSet);
		
		String sql = "CREATE TABLE " + cntTblName +
				  " AS (SELECT HASH(" + CompressedDB.HASH_FUNCTION + "," + cnctsb.toString() + ") as cval, "+ 
				" Count(" + tblnamesArr[0] + ".tid)"+ " as CNT " + 
				  fromClause.toString() + whereClause.toString() +
				  " GROUP BY cval HAVING CNT>1)"; //table for <val,Cnt> pairs
		
		
		/*
		String sql = "CREATE TABLE " + cntTblName +
				  " AS (SELECT "+ cnctsb.toString() + " as cval, "+ 
				" Count(" + tblnamesArr[0] + ".tid)"+ " as CNT " + 
				  fromClause.toString() + whereClause.toString() +
				  " GROUP BY cval HAVING CNT>1)"; //table for <val,Cnt> pairs
		*/
		try {
			Statement stmt = DBConnection.createStatement();
			stmt.executeUpdate(sql);
			DBConnection.commit();
			numQueriesIssued++;
			
			retVal= computeEntropy(attSet);
			dropTblByName(cntTblName);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		totalTimeQuerying+=(System.currentTimeMillis()-startTime);
		return retVal;
		
	}
	
	//delete all DB tables
	public static void shutdown() {
		System.out.println(Thread.currentThread().getId() + ": calling Master shutdown");
		CompressedDB.shutdown();
		//  try {			  
		// 	 Connection DBConnection = CompressedDB.getDBConnection();
		// 	  ResultSet rs = DBConnection.getMetaData().getTables(null, null,
		// 			  "TBL_%", null);		
		// 	  ResultSet rsTemp = DBConnection.getMetaData().getTables(null, null,
		// 			  "TEMP%", null);	
			  
		// 	  Statement stmtDeleteTEMPTables = DBConnection.createStatement();
		// 	  Statement stmtDeleteTables = DBConnection.createStatement();
		// 	  int numTblsRemoved =0 ; 
		// 	  int numTempTblsRemoved=0;
		// 	  while(rs.next()) {
		// 		  String tblName = rs.getString(3);
		// 		  String sql = "DROP TABLE " + tblName;
		// 		  stmtDeleteTables.addBatch(sql);
		// 		  numTblsRemoved++;
		// 	  }
		// 	  while(rsTemp.next()) {
		// 		  String tblName = rs.getString(3);
		// 		  String sql = "DROP TABLE " + tblName;
		// 		  stmtDeleteTEMPTables.addBatch(sql);
		// 		  numTempTblsRemoved++;
		// 	  }
		// 	  if(numTempTblsRemoved > 0)
		// 		  stmtDeleteTEMPTables.executeBatch();
		// 	  DBConnection.commit();
			  
		// 	  if(numTblsRemoved > 0)
		// 		  stmtDeleteTables.executeBatch();
			  
		// 	  DBConnection.commit();
		//   }
		//   catch (SQLException e) {
		// 	  // TODO Auto-generated catch block
		// 	  e.printStackTrace();			  
		//   }		
	}
	private void dropTblByName(String tblName) {
		  try {
			  Statement stmt = DBConnection.createStatement();
			  String sql = "DROP TABLE " + tblName;
			  stmt.executeUpdate(sql);			  
			  DBConnection.commit();
		  }
		  catch (SQLException e) {
			  // TODO Auto-generated catch block
			  //e.printStackTrace();			  
		  }		
	  }
	
	private double computeEntropy(IAttributeSet attSet) {
		   //First, retrieve the table CNT name
		  String cntTbleName = CompressedDB.tblCNTNameForAttSet(attSet);
		  	  
		  String sql = "Select CNT from " + cntTbleName;
		  ResultSet rset;
		  Statement stmt;
		  double entropy =0;
		  double tuplesAccountedFor = 0;
			try {
				stmt = DBConnection.createStatement();
				rset = stmt.executeQuery(sql);				
				while(rset.next()) {
					double cntVal = ((double)rset.getInt("CNT"));
					//number of transactions in which it appears divided by #lines
					double valProb=cntVal/((double)numLines); 
					entropy+=(-1.0)*(valProb* CompressedDB.log2(valProb));
					tuplesAccountedFor+=cntVal;
					tuplesReadDuringEntropyComputation++;
				}
				//account for the singleton lines
				double singletonTuples = ((double)numLines-tuplesAccountedFor);
				double uniformProb = 1/((double)numLines);
				entropy+=singletonTuples*((-1.0)*uniformProb*CompressedDB.log2(uniformProb));
				EntropyMap.put(attSet.clone(), entropy);
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}	
			return entropy;
	   }
	
	
	public static void main(String[] args) { 
		   
		   int numAtts = CompressedDB.getNumAtts(new File(args[0]));
		   long startTime =  System.currentTimeMillis();
		   MasterCompressedDB mcDB = new MasterCompressedDB(args[0], numAtts, 5, false);
		   long timeToConstruct = System.currentTimeMillis()-startTime;
		   startTime =  System.currentTimeMillis();
		   mcDB.initDBs();
		   RelationSchema schema = new RelationSchema(numAtts);
	       ExternalFileDataSet dataSet = new ExternalFileDataSet(args[0], schema);     
	        mcDB.precomputeMostSpecificSeps(dataSet);
		   long timeToInit = System.currentTimeMillis()-startTime;
		   long timeToRun = System.currentTimeMillis()-startTime;
		   
		   
		   MasterCompressedDB.compareTest(args[0],  numAtts, mcDB);
		   
	   }
	
	private static void compareTest(String filePath, int numAtts,MasterCompressedDB mcDB) {
		   	//now test   
		   
	       int numAttributes = numAtts;
	       RelationSchema schema = new RelationSchema(numAttributes);	       
	       ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);
	       //iterate over all subsets and test the entropy value
	       double numPossibleSets = Math.pow(2, numAttributes)-1; 
		   long curr =0;
		   HashMap<IAttributeSet, Double> entropyVals = new HashMap<IAttributeSet,Double>();
		   while(curr < numPossibleSets) {
				AttributeSet attSet = new AttributeSet(numAttributes);
				for(int j = 0; j < numAttributes; j++) 
		        {
					/* Check if jth bit in the  
		            counter is set */
		            if((curr & (1 << j)) > 0) 
		            	attSet.add(j);	                
		        }
				entropyVals.put(attSet,0.0);
				curr++;
			}		
		   dataSet.computeEntropies(entropyVals);
		   boolean passed = true;
		   //now compare
		   for(IAttributeSet attSet: entropyVals.keySet()) {
			   double toCompareentropy = entropyVals.get(attSet);
			   double entropy = mcDB.getEntropy(attSet);			   			   
			   
			   if(Math.abs(entropy-toCompareentropy)>0.0000000001) {
				   System.out.println("Wrong entropy value for attributeSet " + 
						   attSet.toString() + " correct value: " + 
						   entropyVals.get(attSet) + " while mcDB computed: "+ entropy);
				   passed = false;
			   }
		   }
		   System.out.println("test: " + (passed ? "passed" : "failed"));
	   }
}
