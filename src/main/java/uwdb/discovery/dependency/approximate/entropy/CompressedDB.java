package uwdb.discovery.dependency.approximate.entropy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;

import org.apache.commons.lang3.tuple.Pair;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;



public class CompressedDB {

	static final String SEP = ",";
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "org.h2.Driver";
	static final String DB_URL = "jdbc:h2:mem:db;LOCK_TIMEOUT=10000";

	static final String HASH_FUNCTION = "'SHA256'"; // "'SHA2_256'";

	// Database credentials
	static final String USER = "sa";
	static final String PASS = "";

	static Connection DBConnection;
	int numOfAttributes;
	int[] domainSizes; // for dictionary encoding
	AttributeSet[] singleColumnAtts;

	Map<Integer, Map<Integer, List<Long>>> attributeMaps;
	Map<Integer, Map<String, Integer>> dictionaryEncodingMaps;
	private String fileName;
	long numLines = 0;
	boolean hasHeader;
	int rangeBegin;
	int rangeEnd;

	private Map<IAttributeSet, String> FirstLevelAttsToTblName;

	private Map<IAttributeSet, Double> EntropyMap;
	int numEntropiesImplied = 0;

	static long timeInCreatePrelimCnt = 0;
	static long timeInCreatePrelimTid = 0;
	static long timeInDictionaryEncoding = 0;
	static long timeSpentComputingEntropy = 0;
	static long timeSpentGeneratingFirstLevelTbls = 0;
	public static long timeFetchingEntropyFromMap = 0;

	private Set<String> generatedTblNames;

	public CompressedDB(String fileName, int numOfAttribtues, boolean hasHeader) {
		this(fileName, numOfAttribtues, hasHeader, 0, numOfAttribtues);
	}


	public CompressedDB(String fileName, int numOfAttribtues, boolean hasHeader, int rangeBegin,
			int rangeEnd) {

		this.numOfAttributes = numOfAttribtues;
		this.rangeBegin = rangeBegin;
		this.rangeEnd = rangeEnd;

		getDBConnection();
		domainSizes = new int[numOfAttribtues];
		attributeMaps = new HashMap<Integer, Map<Integer, List<Long>>>(numOfAttribtues);
		this.fileName = fileName;
		numLines = 0;
		this.hasHeader = hasHeader;
		FirstLevelAttsToTblName = new HashMap<IAttributeSet, String>(numOfAttribtues);
		dictionaryEncodingMaps = new HashMap<Integer, Map<String, Integer>>();
		// CurrLevelAttsToTblName = new HashMap<IAttributeSet, String>();

		singleColumnAtts = new AttributeSet[numOfAttribtues];
		for (int i = rangeBegin; i < rangeEnd; i++) {
			domainSizes[i] = 1; // start dictionary encoding from 1
			Map<Integer, List<Long>> map_i = new HashMap<Integer, List<Long>>(); // inverted index
																					// for attribute
																					// i
			attributeMaps.put(i, map_i);
			Map<String, Integer> dictionaryEncoding_i = new HashMap<String, Integer>(); // from the
																						// string to
																						// encoded
																						// val
			dictionaryEncodingMaps.put(i, dictionaryEncoding_i);
			singleColumnAtts[i] = new AttributeSet(numOfAttribtues);
			singleColumnAtts[i].add(i);
		}

		EntropyMap = new HashMap<IAttributeSet, Double>();
		EntropyMap.put(new AttributeSet(numOfAttribtues), 0.0); // put the empty attribute set
		generatedTblNames = new HashSet<String>();
	}

	public double getEntropy(IAttributeSet attset) {
		long startTime = System.currentTimeMillis();
		double retVal = totalEntropy();
		if (EntropyMap.containsKey(attset)) {
			// return EntropyMap.get(attset);
			retVal = EntropyMap.get(attset);
		}
		// return totalEntropy();
		timeFetchingEntropyFromMap += System.currentTimeMillis() - startTime;
		return retVal;
	}

	public void init() {

		String line;
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(fileName));
			if (hasHeader) {
				line = reader.readLine();
			}
			while ((line = reader.readLine()) != null) {
				numLines += 1;
				processLine(line, numLines);

			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		generateFirstLevelTables();
		// CurrLevelAttsToTblName = this.FirstLevelAttsToTblName;
	}

	private static String EMPTY_STRING_REP = "EMPTY";

	protected void processLine(String line, long tupleIndex) {
		String[] parts = line.split(SEP);
		for (int i = rangeBegin; i < rangeEnd; i++) {
			String i_val = EMPTY_STRING_REP;
			if (i < parts.length) // for the case where the last field is empty
				i_val = parts[i].trim(); // get string value
			Map<String, Integer> dicEncodingMap_i = dictionaryEncodingMaps.get(i);
			Map<Integer, List<Long>> i_map = attributeMaps.get(i);
			int dictValue;
			if (dicEncodingMap_i.containsKey(i_val)) { // have seen this value before
				dictValue = dicEncodingMap_i.get(i_val); // get dictionary encoding
				List<Long> tupleIndices = i_map.get(dictValue); // get tuple indices that contain
																// this value
				tupleIndices.add(tupleIndex);
			} else {
				dictValue = domainSizes[i]++; // define encoding for this value
				dicEncodingMap_i.put(i_val, dictValue);
				List<Long> tupleIndices = new LinkedList<Long>();
				tupleIndices.add(tupleIndex);
				i_map.put(dictValue, tupleIndices);
			}
		}
	}

	private void insertValueAndTuples(Integer value, List<Long> tuples, String tblName) {

		if (tuples.size() <= 1)
			return; // ignore singletons

		Statement stmt = null;
		try {
			stmt = DBConnection.createStatement();

			for (Long tid : tuples) {
				String sql = "INSERT INTO " + tblName + " VALUES " + "(" + value + "," + tid + ")";
				stmt.addBatch(sql);
			}

			stmt.executeBatch();
			DBConnection.commit();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	public static String tblNameForAttSet(IAttributeSet attSet) {
		Integer hc = attSet.hashCode();
		StringBuilder sb = new StringBuilder();
		sb.append((hc >= 0 ? "0" : "1"));
		hc = (hc >= 0 ? hc : (0 - hc));
		sb.append(hc);

		for (int i = attSet.nextAttribute(0); i >= 0; i = attSet.nextAttribute(i + 1)) {
			sb.append(i).append('_');
		}
		return "TBL_" + sb.toString();
	}

	public static String tblCNTNameForAttSet(IAttributeSet attSet) {
		Integer hc = attSet.hashCode();
		StringBuilder sb = new StringBuilder();
		sb.append((hc >= 0 ? "0" : "1"));
		hc = (hc >= 0 ? hc : (0 - hc));
		sb.append(hc);

		for (int i = attSet.nextAttribute(0); i >= 0; i = attSet.nextAttribute(i + 1)) {
			sb.append(i).append('_');
		}
		return "TBL_CNT_" + sb.toString();
	}

	private double uniformProb() {
		return 1 / ((double) numLines);
	}

	public double totalEntropy() {
		// double uniformProb = uniformProb();
		// double retVal = numLines*((-1.0)*uniformProb*log2(uniformProb));
		return log2(numLines);
		// return retVal;
	}

	private void generateFirstLevelTables() {
		long startTime = System.currentTimeMillis();
		Statement stmt = null;
		try {
			stmt = DBConnection.createStatement();
			for (int i = rangeBegin; i < rangeEnd; i++) {
				AttributeSet iSet = new AttributeSet(numOfAttributes);
				iSet.add(i);
				String iTblName = tblNameForAttSet(iSet);
				dropIndexByName(iTblName + "_tidIdx");
				dropIndexByName(iTblName + "_valIdx");
				dropTblByName(iTblName);

				/*
				 * String sql = "CREATE TABLE " + iTblName +
				 * " (val INTEGER not NULL, tid INTEGER not NULL)"; //table for <val,tid> pairs
				 */

				String sql = "CREATE TABLE " + iTblName
						+ " (val VARBINARY(260) not NULL, tid INTEGER not NULL)"; // table for
																					// <val,tid>
																					// pairs
				String idxName = iTblName + "_tidIdx";
				String sqlIdx = "CREATE INDEX " + idxName + " ON " + iTblName + " (tid)";
				String idxValName = iTblName + "_valIdx";
				String sqlValIdx = "CREATE INDEX " + idxValName + " ON " + iTblName + " (val)";
				stmt.addBatch(sql);
				stmt.addBatch(sqlIdx);
				stmt.addBatch(sqlValIdx);
				FirstLevelAttsToTblName.put(iSet, iTblName);
				generatedTblNames.add(iTblName);
			}
			stmt.executeBatch(); // create raw tables
			Collection<IAttributeSet> singleColumns = FirstLevelAttsToTblName.keySet();
			for (IAttributeSet singleCol : singleColumns) {
				String tbleName = FirstLevelAttsToTblName.get(singleCol); // get table name
																			// corresponding to the
																			// (single) attribute
				int getColIdx = singleCol.nextAttribute(0);
				Map<Integer, List<Long>> valToTuplesMap = attributeMaps.get(getColIdx);
				Collection<Integer> colDictionaryVals = valToTuplesMap.keySet();
				for (Integer val : colDictionaryVals) {
					List<Long> valIdxs = valToTuplesMap.get(val);
					insertValueAndTuples(val, valIdxs, tbleName);
				}
			}
			attributeMaps = null; // not needed anymore - free memory
			// now create the CNT tbls for the attributes
			stmt = DBConnection.createStatement();
			for (IAttributeSet singleCol : singleColumns) {
				String cntTblName = tblCNTNameForAttSet(singleCol);
				String attTbleName = tblNameForAttSet(singleCol);
				String sql =
						"CREATE TABLE " + cntTblName + " AS (Select val, Count(tid) as CNT FROM "
								+ attTbleName + " GROUP BY  val HAVING CNT>1 )"; // table for
																					// <val,Cnt>
																					// pairs
				String idxName = cntTblName + "_valIdx";
				String sqlIdx = "CREATE UNIQUE INDEX " + idxName + " ON " + cntTblName + " (val)";
				stmt.addBatch(sql);
				stmt.addBatch(sqlIdx);
			}
			stmt.executeBatch();

			// compute entropies for basic columns

			for (int i = rangeBegin; i < rangeEnd; i++) {
				IAttributeSet singleColumnAttSet = new AttributeSet(this.numOfAttributes);
				singleColumnAttSet.add(i);
				computeEntropy(singleColumnAttSet);
				this.dropTblByName(tblCNTNameForAttSet(singleColumnAttSet)); // not needed anymore
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		timeSpentGeneratingFirstLevelTbls += System.currentTimeMillis() - startTime;
	}

	private static double log_10_2 = Math.log10(2.0);

	public static double log2(double val) {
		return (Math.log10(val) / log_10_2);
	}

	/*
	 * Here, we assume that we have the first level tables Steps: 1. Compute the entropies from the
	 * current k-level table 2. Use current level table to generate the next level table
	 */
	private void computeEntropy(IAttributeSet attSet) {
		long startTime = System.currentTimeMillis();
		// First, retrieve the table CNT name
		String cntTbleName = tblCNTNameForAttSet(attSet);
		// should not happen
		// if(!doesTableExist(cntTbleName)) return;

		// String sql = "Select val, count(tid) from " + cntTbleName + " GROUP BY val";
		String sql = "Select CNT from " + cntTbleName;
		ResultSet rset;
		Statement stmt;
		double entropy = 0;
		double tuplesAccountedFor = 0;
		try {
			stmt = DBConnection.createStatement();
			rset = stmt.executeQuery(sql);
			while (rset.next()) {
				double cntVal = ((double) rset.getInt("CNT"));
				// assert(cntVal > 1);
				// number of transactions in which it appears divided by #lines
				double valProb = cntVal / ((double) numLines);
				entropy += (-1.0) * (valProb * log2(valProb));
				tuplesAccountedFor += cntVal;
			}
			// account for the singleton lines
			double singletonTuples = ((double) numLines - tuplesAccountedFor);
			double uniformProb = 1 / ((double) numLines);
			entropy += singletonTuples * ((-1.0) * uniformProb * log2(uniformProb));
			EntropyMap.put(attSet, entropy);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
		timeSpentComputingEntropy += (System.currentTimeMillis() - startTime);
	}

	public boolean doesTableExist(String tblName) {
		return generatedTblNames.contains(tblName);
		/*
		 * ResultSet rset; try { rset = DBConnection.getMetaData().getTables(null, null, tblName,
		 * null); if (rset.next()) { return true; } } catch (SQLException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); } return false;
		 */
	}

	public boolean isTblEmpty(String tblName) {
		try {
			Statement stmt = DBConnection.createStatement();
			String sql = "SELECT * from " + tblName;
			ResultSet rs = stmt.executeQuery(sql);
			return (!rs.next());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			return false;
		}
	}

	private void dropIndexByName(String indexName) {
		try {
			Statement stmt = DBConnection.createStatement();
			String sql = "DROP INDEX IF EXISTS " + indexName;
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	private void dropTblByName(String tblName) {
		try {
			Statement stmt = DBConnection.createStatement();
			String sql = "DROP TABLE " + tblName;
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	public void computeAllEntropies() {
		Set<IAttributeSet> prevLevelLevelCandidates = new HashSet<IAttributeSet>();
		// initialize
		for (int i = rangeBegin; i < rangeEnd; i++) {
			prevLevelLevelCandidates.add(singleColumnAtts[i]);
		}
		Set<IAttributeSet> currLevelLevelCandidates = new HashSet<IAttributeSet>();
		while (!prevLevelLevelCandidates.isEmpty()) {
			// now, generate the candidates
			for (IAttributeSet attSet : prevLevelLevelCandidates) {
				for (int i = rangeBegin; i < rangeEnd; i++) {
					if (attSet.contains(i)) {
						continue; // cannot be extended with i
					}
					IAttributeSet candidate = attSet.clone();
					candidate.add(i);
					if (currLevelLevelCandidates.contains(candidate)) // already processed
						continue;

					createPrelimCntTbl(attSet, i);
					String candidateCNTTblName = tblCNTNameForAttSet(candidate);
					if (isTblEmpty(candidateCNTTblName)) {
						// double entropy = totalEntropy();
						// EntropyMap.put(candidate, entropy);
						dropTblByName(candidateCNTTblName);
						// addDegenerateAttSetsToEntropyMap(candidate,currLevelLevelCandidates);
						continue;
					}
					createPrelimTidTbl(attSet, i);
					// dictionaryEncodePrelimTbls(candidate);
					computeEntropy(candidate);
					dropTblByName(candidateCNTTblName); // no need for it anymore
					currLevelLevelCandidates.add(candidate);
				}
			}
			prevLevelLevelCandidates = currLevelLevelCandidates;
			currLevelLevelCandidates = new HashSet<IAttributeSet>();
		}

	}

	// after finding out that the entropy of attSet = totalEntropy
	// add all of its supersets to the map as well
	private void addDegenerateAttSetsToEntropyMap(IAttributeSet attSet,
			Set<IAttributeSet> toUpdate) {
		for (int i = 0; i < numOfAttributes; i++) {
			if (attSet.contains(i))
				continue;
			attSet.add(i);
			if (!toUpdate.contains(attSet)) {
				numEntropiesImplied++;
				IAttributeSet newAttSet = attSet.clone();
				toUpdate.add(newAttSet);
			}
			attSet.remove(i);
		}
		/*
		 * double curr =0; double numPossibleSets = Math.pow(2, numOfAttributes)-1;
		 * 
		 * for(int i=0 ; i < numOfAttributes ; i++) { if(attSet.contains(i)) { curr+=Math.pow(2, i);
		 * } } long lcurr = (long)curr; while(lcurr <= numPossibleSets) { IAttributeSet dattSet =
		 * attSet.clone(); for(int j = 0; j < numOfAttributes; j++) {
		 * 
		 * if((lcurr & (1 << j)) > 0) dattSet.add(j); } if(!EntropyMap.containsKey(dattSet))
		 * numEntropiesImplied++;
		 * 
		 * EntropyMap.put(dattSet, totalEntropy());
		 * 
		 * lcurr++; } numEntropiesImplied--;
		 */
	}

	private void createPrelimCntTbl(IAttributeSet attSet, int i) {
		long startTime = System.currentTimeMillis();
		String attTbleName = tblNameForAttSet(attSet);
		Statement stmt = null;
		if (attSet.contains(i)) {
			return; // no point in joining
		}
		try {
			attSet.add(i);
			String cntTblName = tblCNTNameForAttSet(attSet);
			attSet.flip(i); // return as before
			String iTblName = tblNameForAttSet(singleColumnAtts[i]);
			/*
			 * String sql = "CREATE TABLE " + cntTblName + " AS (SELECT " +
			 * attTbleName+".val  as kval, " + iTblName+".val as ival, Count(" + attTbleName +
			 * ".tid)"+ " as CNT FROM " + attTbleName + " , " + iTblName +" WHERE " + attTbleName +
			 * ".tid=" + iTblName+".tid"+ " GROUP BY " + attTbleName+".val" + ","+ iTblName
			 * +".val HAVING CNT>1)"; //table for <val,Cnt> pairs
			 */
			// SELECT CONVERT(VARCHAR(1000), varbinary_value, 2);


			String sql = "CREATE TABLE " + cntTblName + " AS (SELECT HASH(" + HASH_FUNCTION
					+ ",CONCAT(" + attTbleName + ".val," + iTblName + ".val)) as cval, " + " Count("
					+ attTbleName + ".tid)" + " as CNT FROM " + attTbleName + " , " + iTblName
					+ " WHERE " + attTbleName + ".tid=" + iTblName + ".tid"
					+ " GROUP BY cval HAVING CNT>1)"; // table for <val,Cnt> pairs

			/*
			 * String cantorPairHashing = "0.5*("+attTbleName+".val+"+
			 * iTblName+".val"+")*("+attTbleName+".val+"+ iTblName+".val+1)+"+iTblName+".val" ;
			 * String sql = "CREATE TABLE " + cntTblName + " AS (SELECT  " + cantorPairHashing+
			 * " as cval, "+ " Count(" + attTbleName + ".tid)"+ " as CNT FROM " + attTbleName +
			 * " , " + iTblName +" WHERE " + attTbleName + ".tid=" + iTblName+".tid"+
			 * " GROUP BY cval HAVING CNT>1)"; //table for <val,Cnt> pairs
			 */
			// CONVERT(VARCHAR(1000), varbinary_value, 2);
			String tableIdx = cntTblName + "_kivalIdx";
			// String sqlIdx = "CREATE UNIQUE INDEX " + tableIdx +" ON "+ cntTblName + " (kval,
			// ival)";
			String sqlIdx = "CREATE UNIQUE INDEX " + tableIdx + " ON " + cntTblName + " (cval)";
			stmt = DBConnection.createStatement();
			stmt.addBatch(sql);
			stmt.addBatch(sqlIdx);
			// stmt.executeUpdate(sql);
			stmt.executeBatch();
			DBConnection.commit();

			timeInCreatePrelimCnt += (System.currentTimeMillis() - startTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}


	private void createPrelimTidTbl(IAttributeSet attSet, int i) {
		long startTime = System.currentTimeMillis();
		if (attSet.contains(i)) {
			return; // no point in joining
		}

		Statement stmt = null;
		try {
			String tidTblName = tblNameForAttSet(attSet);
			attSet.add(i);
			String cntTblName = tblCNTNameForAttSet(attSet);
			String tidNewTblName = tblNameForAttSet(attSet);
			attSet.flip(i); // return as before
			String iTblName = tblNameForAttSet(singleColumnAtts[i]); // this is the temporary name
			/*
			 * String sql = "CREATE TABLE " + tidNewTblName + " AS "+ "(SELECT " +
			 * tidTblName+".val as kval, " + iTblName+".val as ival, " + iTblName +".tid as tid" +
			 * " from " + tidTblName + ", "+ iTblName +", "+ cntTblName + " WHERE " + tidTblName +
			 * ".tid=" + iTblName + ".tid AND "+ tidTblName +".val=" + cntTblName + ".kval AND "+
			 * iTblName + ".val=" + cntTblName + ".ival)";
			 */
			// SELECT CONVERT(VARCHAR(50), HASHBYTES(" + HASH_FUNCTION + ",CONCAT(" +
			// attTbleName+".val,|,"+ iTblName+".val)) as val, "+
			String tempTbl = "TEMP" + tidNewTblName;


			String sql = "CREATE TABLE " + tidNewTblName + " AS( " + "WITH " + tempTbl + " AS ( "
					+ "SELECT HASH(" + HASH_FUNCTION + ",CONCAT(" + tidTblName + ".val," + iTblName
					+ ".val)) as val," + iTblName + ".tid as tid" + " from " + tidTblName + ", "
					+ iTblName + " WHERE " + tidTblName + ".tid=" + iTblName + ".tid) "
					+ " SELECT val, tid " + " FROM " + tempTbl + ", " + cntTblName + " WHERE val = "
					+ cntTblName + ".cval)";


			/*
			 * String cantorPairHashing = "0.5*("+tidTblName+".val+"+
			 * iTblName+".val"+")*("+tidTblName+".val+"+ iTblName+".val+1)+"+iTblName+".val" ;
			 * String sql = "CREATE TABLE " + tidNewTblName + " AS( " + "WITH " + tempTbl + " AS ( "
			 * + "SELECT " + cantorPairHashing + "  as val," + iTblName +".tid as tid" + " from " +
			 * tidTblName + ", "+ iTblName + " WHERE " + tidTblName + ".tid=" + iTblName + ".tid) "
			 * + " SELECT val, tid " + " FROM " + tempTbl +", " + cntTblName + " WHERE val = " +
			 * cntTblName + ".cval)";
			 */

			/*
			 * String sql = "CREATE TABLE " + tidNewTblName + " AS "+ "(SELECT HASH(" +
			 * HASH_FUNCTION + ",CONCAT(" + tidTblName+".val,"+ iTblName+".val)) as val, " +
			 * iTblName +".tid as tid" + " from " + tidTblName + ", "+ iTblName +", "+ cntTblName +
			 * " WHERE " + tidTblName + ".tid=" + iTblName + ".tid AND "+ "HASH(" + HASH_FUNCTION +
			 * ",CONCAT(" + tidTblName+".val,"+ iTblName+".val))=" + cntTblName + ".cval)";
			 */
			stmt = DBConnection.createStatement();
			stmt.executeUpdate(sql);


			String tableIdx = tidNewTblName + "_tidIdx";
			String valIdx = tidNewTblName + "_kivalIdx";
			String sqlIdx = "CREATE INDEX " + tableIdx + " ON " + tidNewTblName + " (tid)";
			// String valSqlIdx = "CREATE INDEX " + valIdx +" ON "+ tidNewTblName + " (kval, ival)";
			String valSqlIdx = "CREATE INDEX " + valIdx + " ON " + tidNewTblName + " (val)";

			stmt.addBatch(valSqlIdx);
			stmt.addBatch(sqlIdx);

			stmt.executeBatch();
			DBConnection.commit();
			dropTblByName(tempTbl);
			generatedTblNames.add(tidNewTblName);
			timeInCreatePrelimTid += (System.currentTimeMillis() - startTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	private void dictionaryEncodePrelimTbls(IAttributeSet attSet) {
		long startTime = System.currentTimeMillis();
		Statement stmt;
		try {
			stmt = DBConnection.createStatement();
			// now recreate the tables using dictionary encoding
			String cntTblName = tblCNTNameForAttSet(attSet);
			String tidTblName = tblNameForAttSet(attSet);
			String sqlCNT = "ALTER TABLE " + cntTblName + " ADD val INTEGER"; // ok, because current
																				// columns in this
																				// table are kval
																				// and ival
			String sqltid = "ALTER TABLE " + tidTblName + " ADD val INTEGER"; // ok, because current
																				// columns in this
																				// table are kval
																				// and ival

			String cntTableIdx = cntTblName + "_valIdx";
			String sqlCntIdx = "CREATE INDEX " + cntTableIdx + " ON " + cntTblName + " (val)";
			// String tidTableIdx = tidTblName+"_tidIdx";
			// String sqlTidIdx = "CREATE INDEX " + tidTableIdx +" ON "+ tidTblName + " (tid)";


			stmt.addBatch(sqlCNT);
			stmt.addBatch(sqltid);
			stmt.addBatch(sqlCntIdx);
			// stmt.addBatch(sqlTidIdx);
			stmt.executeBatch();
			DBConnection.commit();

			// now, dictionary encode both tables
			Statement insertiDictValueSql = DBConnection.createStatement();
			String sql = "SELECT * from " + cntTblName;
			ResultSet rs = stmt.executeQuery(sql);
			// map from <kval,ival> pairs to the appropriate dictionary encoding
			Map<Pair<Integer, Integer>, Integer> idictEncoding =
					new HashMap<Pair<Integer, Integer>, Integer>();
			int dictEncoding = 1;
			int count = 0;
			while (rs.next()) {
				int attVal = rs.getInt("kval");
				int iattVal = rs.getInt("ival");
				// for testing
				int cntVal = rs.getInt("CNT");
				// assert(cntVal > 1);

				int dictVal;
				Pair<Integer, Integer> p = Pair.of(attVal, iattVal);
				if (idictEncoding.containsKey(p)) {
					dictVal = idictEncoding.get(p);
				} else {
					dictVal = dictEncoding++;
				}
				idictEncoding.put(p, dictVal);
				String insertValSqlCNT = "UPDATE " + cntTblName + " SET val=" + dictVal
						+ " WHERE (kval=" + attVal + ") AND (ival=" + iattVal + ")";
				String insertValSqlTID = "UPDATE " + tidTblName + " SET val=" + dictVal
						+ " WHERE (kval=" + attVal + ") AND (ival=" + iattVal + ")";
				insertiDictValueSql.addBatch(insertValSqlCNT);
				insertiDictValueSql.addBatch(insertValSqlTID);
				count += 2;
				if (count == 10) {
					insertiDictValueSql.executeBatch();
					count = 0;
				}
			}
			if (count > 0) {
				insertiDictValueSql.executeBatch();
			}
			DBConnection.commit();

			// now, delete redundant columns kval and ival from the table
			Statement deleteStatement = DBConnection.createStatement();
			String sqlDeleteCNT = "ALTER TABLE " + cntTblName + " DROP COLUMN kval, ival";
			String sqlDeleteTID = "ALTER TABLE " + tidTblName + " DROP COLUMN kval, ival";
			String cntidxName = cntTblName + "_kivalIdx";
			String tididxName = tidTblName + "_kivalIdx";
			String sqlIdxDeleteCnt = "DROP INDEX " + cntidxName + " ON " + cntTblName;
			String sqlIdxDeleteTid = "DROP INDEX " + tididxName + " ON " + tidTblName;
			deleteStatement.addBatch(sqlIdxDeleteCnt);
			deleteStatement.addBatch(sqlIdxDeleteTid);
			deleteStatement.executeBatch();
			deleteStatement.addBatch(sqlDeleteCNT);
			deleteStatement.addBatch(sqlDeleteTID);

			deleteStatement.executeBatch();
			DBConnection.commit();

			timeInDictionaryEncoding += System.currentTimeMillis() - startTime;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	/*
	 * Here we assume that we are creating a level of at least k>=2 That is, we are expanding the
	 * attribute set attSet
	 * 
	 * Change for dictionary encoding
	 */
	private void createCntTables(IAttributeSet attSet) {

		String attTbleName = tblNameForAttSet(attSet);
		Statement stmt = null;
		try {
			for (int i = 0; i < numOfAttributes; i++) {
				if (attSet.contains(i)) {
					continue; // no point in joining
				}

				attSet.add(i);
				String cntTblName = tblCNTNameForAttSet(attSet);
				attSet.flip(i); // return as before
				String iTblName = tblNameForAttSet(singleColumnAtts[i]);
				String sql = "CREATE TABLE " + cntTblName + " AS (SELECT " + attTbleName
						+ ".val  as kval, " + iTblName + ".val as ival, Count(tid) as CNT FROM "
						+ attTbleName + " , " + iTblName + " WHERE " + attTbleName + ".tid="
						+ iTblName + ".tid" + " GROUP BY " + attTbleName + ".val" + "," + iTblName
						+ ".val HAVING CNT>1)"; // table for <val,Cnt> pairs

				stmt = DBConnection.createStatement();
				stmt.addBatch(sql);
			}
			stmt.executeBatch();
			DBConnection.commit();

			stmt = DBConnection.createStatement();
			// now recreate the tables using dictionary encoding
			for (int i = 0; i < numOfAttributes; i++) {
				if (attSet.contains(i)) {
					continue; // no point in joining
				}
				attSet.add(i);
				String cntTblName = tblCNTNameForAttSet(attSet);
				attSet.flip(i); // return as before
				String sql = "ALTER TABLE " + cntTblName + " ADD val INTEGER"; // ok, because
																				// current columns
																				// in this table are
																				// kval and ival
				stmt.addBatch(sql);
			}
			stmt.executeBatch();
			DBConnection.commit();

			// now, dictionary encode
			stmt = DBConnection.createStatement();
			Statement insertiDictValueSql = DBConnection.createStatement();
			for (int i = 0; i < numOfAttributes; i++) {
				if (attSet.contains(i)) {
					continue; // no point in joining
				}

				attSet.add(i);
				String cntTblName = tblCNTNameForAttSet(attSet);
				attSet.flip(i); // return as before
				String sql = "SELECT * from " + cntTblName;
				ResultSet rs = stmt.executeQuery(sql);
				// map from <kval,ival> pairs to the appropriate dictionary encoding
				Map<Pair<Integer, Integer>, Integer> idictEncoding =
						new HashMap<Pair<Integer, Integer>, Integer>();
				int dictEncoding = 1;
				while (rs.next()) {
					int attVal = rs.getInt(1);
					int iattVal = rs.getInt(2);
					int dictVal;
					Pair<Integer, Integer> p = Pair.of(attVal, iattVal);
					if (idictEncoding.containsKey(p)) {
						dictVal = idictEncoding.get(p);
					} else {
						dictVal = dictEncoding++;
					}
					idictEncoding.put(p, dictVal);
					String insertValSql = "UPDATE " + cntTblName + " SET val=" + dictVal
							+ " WHERE (kval=" + attVal + ") AND (ival=" + iattVal + ")";
					insertiDictValueSql.addBatch(insertValSql);
				}
			}
			insertiDictValueSql.executeBatch();
			DBConnection.commit();

			// now, delete redundant columns kval and ival from the table
			Statement deleteStatement = DBConnection.createStatement();
			for (int i = 0; i < numOfAttributes; i++) {
				if (attSet.contains(i)) {
					continue; // no point in joining
				}
				attSet.add(i);
				String cntTblName = tblCNTNameForAttSet(attSet);
				attSet.flip(i); // return as before
				String sqlDelete = "ALTER TABLE " + cntTblName + " DROP COLUMN kval, ival";
				deleteStatement.addBatch(sqlDelete);
			}
			deleteStatement.executeBatch();
			DBConnection.commit();
		}

		catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	public void createIndex(String tblName, String[] clmnNames, boolean unique) {
		Statement stmt = null;
		String opening = unique ? "CREATE UNIQUE INDEX ON " : "CREATE INDEX ON ";
		StringBuilder sb = new StringBuilder();
		sb.append(opening);
		sb.append(tblName);
		sb.append("( ");
		for (int i = 0; i < clmnNames.length; i++) {
			sb.append(clmnNames[i]);
			if (i < (clmnNames.length - 1)) {
				sb.append(", ");
			}
		}
		sb.append(")");

		try {
			stmt = DBConnection.createStatement();
			stmt.executeUpdate(sb.toString());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	public static Connection getDBConnection() {
		if (DBConnection == null) {
			try {
				Class.forName(JDBC_DRIVER);
			} catch (ClassNotFoundException e) {
				System.out.println(e.getMessage());
			}
			try {
				DBConnection = DriverManager.getConnection(DB_URL, USER, PASS);
				return DBConnection;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		return DBConnection;
	}

	private static void compareTest(String filePath, int numAtts,
			Map<IAttributeSet, Double> toCompareMap, double totalEntropy) {
		// now test

		int numAttributes = numAtts;
		RelationSchema schema = new RelationSchema(numAttributes);
		ExternalFileDataSet dataSet = new ExternalFileDataSet(filePath, schema);
		// iterate over all subsets and test the entropy value
		double numPossibleSets = Math.pow(2, numAttributes) - 1;
		long curr = 0;
		HashMap<IAttributeSet, Double> entropyVals = new HashMap<IAttributeSet, Double>();
		while (curr < numPossibleSets) {
			AttributeSet attSet = new AttributeSet(numAttributes);
			for (int j = 0; j < numAttributes; j++) {
				/*
				 * Check if jth bit in the counter is set
				 */
				if ((curr & (1 << j)) > 0)
					attSet.add(j);
			}
			entropyVals.put(attSet, 0.0);
			curr++;
		}
		dataSet.computeEntropies(entropyVals);
		boolean passed = true;
		// now compare
		for (IAttributeSet attSet : entropyVals.keySet()) {
			double toCompareentropy = entropyVals.get(attSet);
			if (!toCompareMap.containsKey(attSet)) {
				if (Math.abs(toCompareentropy - totalEntropy) > 0.001) {
					System.out.println("Entropy for " + attSet.toString()
							+ "not computed even though it's entropy is " + toCompareentropy + " < "
							+ totalEntropy);
					passed = false;
				}
				continue;
			}
			double entropy = toCompareMap.get(attSet);

			if (Math.abs(entropy - toCompareentropy) > 0.001) {
				System.out.println("Wrong entropy value for attributeSet " + attSet.toString()
						+ " correct value: " + entropyVals.get(attSet) + " while cDB computed: "
						+ toCompareMap.get(attSet));
				passed = false;
			}
		}
		System.out.println("test: " + (passed ? "passed" : "failed"));
	}

	public static void main(String[] args) {

		int numAtts = CompressedDB.getNumAtts(new File(args[0]));
		long startTime = System.currentTimeMillis();
		CompressedDB cDB = new CompressedDB(args[0], numAtts, false);
		cDB.init();
		cDB.computeAllEntropies();
		long timeToRun = System.currentTimeMillis() - startTime;
		System.out.println(
				"total time in msec for : " + numAtts + " attributes: " + timeToRun + " msec");
		System.out.println("Total number of entropies computed: " + cDB.EntropyMap.size()
				+ " out of " + (Math.pow(2, cDB.numOfAttributes) - 1));
		System.out.println("From those, the number of implied is: " + cDB.numEntropiesImplied);
		System.out.println("Time spent creating prelim cnt tables: " + cDB.timeInCreatePrelimCnt);
		System.out.println("Time spent creating prelim tid tables: " + cDB.timeInCreatePrelimTid);
		System.out.println("Time spent deictionary encoding: " + cDB.timeInDictionaryEncoding);
		System.out.println("Time spent computing entropy: " + cDB.timeSpentComputingEntropy);
		System.out.println("Time spent generating first level tables: "
				+ cDB.timeSpentGeneratingFirstLevelTbls);
		compareTest(args[0], cDB.numOfAttributes, cDB.EntropyMap, cDB.totalEntropy());

	}

	public static int getNumAtts(File csvFile) {
		int retVal = 0;
		try {

			BufferedReader reader = Files.newBufferedReader(csvFile.toPath());
			String line = reader.readLine();
			String[] atts = line.split(",");
			return atts.length;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retVal;
	}

	public static void shutdown() {
		if (DBConnection != null) {
			try {
				DBConnection.close();
				DBConnection = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
