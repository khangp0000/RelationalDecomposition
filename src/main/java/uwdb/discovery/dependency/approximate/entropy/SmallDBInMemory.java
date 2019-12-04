package uwdb.discovery.dependency.approximate.entropy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

public class SmallDBInMemory {

	static final String SEP = ",";
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "org.h2.Driver";
	static final String DB_URL = "jdbc:h2:mem:db;LOCK_TIMEOUT=10000";

	static final String HASH_FUNCTION = "'SHA256'"; // "'SHA2_256'";

	// Database credentials
	static final String USER = "sa";
	static final String PASS = "";

	Connection DBConnection;
	int numOfAttributes;
	int[] domainSizes; // for dictionary encoding
	AttributeSet[] singleColumnAtts;

	Map<Integer, Map<Integer, List<Long>>> attributeMaps;
	Map<Integer, Map<String, Integer>> dictionaryEncodingMaps;
	private String fileName;
	int numLines = 0;
	boolean hasHeader;

	public static String TBL_NAME = "CSVTblEncoding";

	public SmallDBInMemory(String fileName, int numOfAttribtues, boolean hasHeader) {
		this.numOfAttributes = numOfAttribtues;

		DBConnection = getDBConnection();
		domainSizes = new int[numOfAttribtues];
		attributeMaps = new HashMap<Integer, Map<Integer, List<Long>>>(numOfAttribtues);
		this.fileName = fileName;
		numLines = 0;
		this.hasHeader = hasHeader;
		dictionaryEncodingMaps = new HashMap<Integer, Map<String, Integer>>();
		// CurrLevelAttsToTblName = new HashMap<IAttributeSet, String>();

		singleColumnAtts = new AttributeSet[numOfAttribtues];
		for (int i = 0; i < numOfAttribtues; i++) {
			domainSizes[i] = 1; // start dictionary encoding from 1
			Map<String, Integer> dictionaryEncoding_i = new HashMap<String, Integer>(); // from the string to encoded
																						// val
			dictionaryEncodingMaps.put(i, dictionaryEncoding_i);
			singleColumnAtts[i] = new AttributeSet(numOfAttribtues);
			singleColumnAtts[i].add(i);
		}
		this.dropTblByName(TBL_NAME);
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ").append(TBL_NAME).append(" (");
		for (int i = 0; i < numOfAttribtues; i++) {
			sb.append("att").append(i).append(" INTEGER not NULL ");
			if (i < (numOfAttribtues - 1)) {
				sb.append(",");
			}
		}
		sb.append(")");

		Statement stmt = null;

		try {
			stmt = DBConnection.createStatement();
			String sql = sb.toString();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static String EMPTY_STRING_REP = "EMPTY";

	protected void processLine(String line, long tupleIndex) {
		StringBuilder sb = new StringBuilder("INSERT INTO ").append(TBL_NAME).append(" VALUES (");

		String[] parts = line.split(SEP);
		for (int i = 0; i < numOfAttributes; i++) {
			String i_val = EMPTY_STRING_REP;
			if (i < parts.length) // for the case where the last field is empty
				i_val = parts[i].trim(); // get string value
			Map<String, Integer> dicEncodingMap_i = dictionaryEncodingMaps.get(i);
			int dictValue;
			if (dicEncodingMap_i.containsKey(i_val)) { // have seen this value before
				dictValue = dicEncodingMap_i.get(i_val); // get dictionary encoding
			} else {
				dictValue = domainSizes[i]++; // define encoding for this value
				dicEncodingMap_i.put(i_val, dictValue);
			}
			sb.append(dictValue);
			if (i < numOfAttributes - 1) {
				sb.append(", ");
			}
		}
		sb.append(")");

		Statement stmt = null;

		try {
			stmt = DBConnection.createStatement();
			String sql = sb.toString();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int generateProjectionTable(IAttributeSet cluster) {

		String clusterTableName = tblClusterNameForAttSetNew(cluster);
		StringBuilder sb = new StringBuilder();
		// sb.append("CREATE TABLE ").append(clusterTableName).append(" AS (Select
		// DISTINCT ");
		// sb.append("CREATE TABLE ").append(clusterTblName).append(" AS (Select ");
		sb.append("CREATE TABLE ").append(clusterTableName).append(" AS (Select ");
		StringBuilder attNames = new StringBuilder();
		int numAdded = 0;
		for (int i = cluster.nextAttribute(0); i >= 0; i = cluster.nextAttribute(i + 1)) {
			numAdded++;
			attNames.append("att").append(i); // .append(" AS ").append("att").append(i);
			if (numAdded < cluster.cardinality()) {
				attNames.append(", ");
			}
		}
		// sb.append(") ");
		sb.append(attNames);
		sb.append(" FROM ").append(TBL_NAME);// .append(")");
		sb.append(" GROUP BY ").append(attNames).append(" HAVING count(*) > 0 )");
		Statement stmt = null;
		int projectionSize = 0;
		try {
			stmt = DBConnection.createStatement();
			String sql = sb.toString();
			stmt.executeUpdate(sql);

			// some tests
			/*
			 * String sqlAll = " select * from " + clusterTableName; String sqlDistinct =
			 * " select DISTINCT * from " + clusterTableName; String sqlCount =
			 * " select Count(*) from " + clusterTableName; String sqlDistinctCount =
			 * " select Count(DISTINCT ) from " + clusterTableName; ResultSet rs1 =
			 * stmt.executeQuery(sqlAll); boolean hasnext = rs1.next(); rs1.last(); int
			 * numbersqlAll = rs1.getRow();
			 * 
			 * ResultSet rs2 = stmt.executeQuery(sqlDistinct); hasnext = rs2.next();
			 * rs2.last(); int numbersqlDistinct = rs2.getRow();
			 * 
			 * ResultSet rs3 = stmt.executeQuery(sqlCount); hasnext = rs3.next(); int answer
			 * = rs3.getInt(1);
			 */

			// now calculate the number of (distinct) tuples in this table
			String sqlCountTuples = " select count(*) from " + clusterTableName;
			// String sqlCountTuples = "Select * from " + clusterTableName;
			// String sqlCountTuples = "Select Count(*) AS CNT from (Select DISTINCT * FROM
			// " + clusterTableName + ")";
			// Statement cntStmt =
			// DBConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
			// ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = stmt.executeQuery(sqlCountTuples);
			boolean hasFirst = rs.first();
			if (!hasFirst)
				System.out.println("problem");
			/*
			 * boolean hasLast = rs.last(); if(hasLast) projectionSize = rs.getRow(); else {
			 * System.out.println("problem"); }
			 */
			projectionSize = rs.getInt(1);
			// projectionSize = rs.getInt("CNT");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return projectionSize;
	}

	public static class decompositionSize {
		public int smallestRelation;
		public int largestRelation;
		public int totalTuplesInDecomposition;
		public int totalCellsInDecomposition;
	}

	public int testDecomposition(Set<IAttributeSet> clusters, Set<IAttributeSet> separators, decompositionSize ds) {
		SmallDBInMemory.shutdown(); // delete all previously created projections

		// map every separator to the set of clusters that contain it
		HashMap<IAttributeSet, Set<IAttributeSet>> sepsToClusters = new HashMap<IAttributeSet, Set<IAttributeSet>>();
		HashMap<IAttributeSet, Integer> clustersToProjectionSizes = new HashMap<IAttributeSet, Integer>();
		for (IAttributeSet separator : separators) {
			Set<IAttributeSet> clustersForSep = new HashSet<IAttributeSet>();
			for (IAttributeSet cluster : clusters) {
				if (cluster.contains(separator)) {
					clustersForSep.add(cluster);
				}
			}
			sepsToClusters.put(separator, clustersForSep);
		}
		// create a decomposition for the clusters
		StringBuilder FROMClause = new StringBuilder(" FROM ");
		int numProcessed = 0;
		int smallestRelation = Integer.MAX_VALUE;
		int largestRelation = 0;
		int totalTuples = 0;
		int totalCells = 0;
		for (IAttributeSet cluster : clusters) {
			numProcessed++;
			int clusterProjectionSize = generateProjectionTable(cluster);
			smallestRelation = (clusterProjectionSize < smallestRelation) ? clusterProjectionSize : smallestRelation;
			largestRelation = (clusterProjectionSize > largestRelation) ? clusterProjectionSize : largestRelation;
			totalTuples += clusterProjectionSize;
			totalCells += (clusterProjectionSize * cluster.cardinality());
			clustersToProjectionSizes.put(cluster, clusterProjectionSize);
			String clusterTblName = tblClusterNameForAttSetNew(cluster);

//			Statement stmt;
//			try {
//				stmt = DBConnection.createStatement();
//				ResultSet rs = stmt.executeQuery("SELECT * FROM " + clusterTblName);
//				int columnsNumber = rs.getMetaData().getColumnCount();
//				while (rs.next()) {
//					for (int i = 1; i <= columnsNumber; i++) {
//						if (i > 1)
//							System.out.print(",  ");
//						String columnValue = rs.getString(i);
//						System.out.print(columnValue + " " + rs.getMetaData().getColumnName(i));
//					}
//					System.out.println("");
//				}
//			} catch (SQLException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			FROMClause.append(clusterTblName);
			if (numProcessed < clusters.size())
				FROMClause.append(", ");
		}
		ds.largestRelation = largestRelation;
		ds.smallestRelation = smallestRelation;
		ds.totalTuplesInDecomposition = totalTuples;
		ds.totalCellsInDecomposition = totalCells;

		StringBuilder WHEREClauseContents = new StringBuilder();
		numProcessed = 0;
		for (Entry<IAttributeSet, Set<IAttributeSet>> sepClusters : sepsToClusters.entrySet()) {
			numProcessed++;
			String sepWhereClause = generateWhereClause(sepClusters.getKey(), sepClusters.getValue());
			WHEREClauseContents.append(sepWhereClause);
			if (numProcessed < sepsToClusters.size())
				WHEREClauseContents.append(" AND ");
		}
		StringBuilder WHEREClause = new StringBuilder();
		if (WHEREClauseContents.length() > 0) {
			WHEREClause.append(" WHERE ").append(WHEREClauseContents);
		}
		StringBuilder sqlQuery = new StringBuilder();
//		   sqlQuery.append("Select Count(*) AS CNT").
//		   sqlQuery.append("Select Count(*) FROM (SELECT DISTINCT * ").
		sqlQuery.append("SELECT DISTINCT * ").append(FROMClause).append(WHEREClause);// .append(") AS CNT") ;

		Statement stmt = null;
		int spuriousTuples = 0;
		try {
			stmt = DBConnection.createStatement();
			String sql = sqlQuery.toString();
			ResultSet rs = stmt.executeQuery(sql);
			// rs.next();
			rs.last();
			// int numTuplesInJoin = rs.getInt("CNT");
			int numTuplesInJoin = rs.getRow();
			spuriousTuples = numTuplesInJoin - numLines;
			SmallDBInMemory.shutdown();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return spuriousTuples;
	}

	private String generateWhereClause(IAttributeSet separator, Set<IAttributeSet> clusters) {
		// copy to array
		IAttributeSet clusterArr[] = new IAttributeSet[clusters.size()];
		int j = 0;
		for (IAttributeSet cluster : clusters) {
			clusterArr[j++] = cluster;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < clusterArr.length - 1; i++) {
			IAttributeSet prev = clusterArr[i];
			IAttributeSet curr = clusterArr[i + 1];
			String prevName = tblClusterNameForAttSetNew(prev);
			String currName = tblClusterNameForAttSetNew(curr);
			for (int k = separator.nextAttribute(0); k >= 0; k = separator.nextAttribute(k + 1)) {
				sb.append(prevName).append(".att").append(k).append("=").append(currName).append(".att").append(k)
						.append(" AND ");
			}
		}

		int numClusters = clusterArr.length;
		int numProcessed = 0;
		String prevName = tblClusterNameForAttSetNew(clusterArr[numClusters - 1]);
		String currName = tblClusterNameForAttSetNew(clusterArr[0]);
		for (int k = separator.nextAttribute(0); k >= 0; k = separator.nextAttribute(k + 1)) {
			numProcessed++;
			sb.append(prevName).append(".att").append(k).append("=").append(currName).append(".att").append(k);
			if (numProcessed < separator.cardinality()) {
				sb.append(" AND ");
			}
		}
		return sb.toString();
	}

	public String tblClusterNameForAttSet1(IAttributeSet attSet) {
		Integer hc = attSet.hashCode();
		StringBuilder sb = new StringBuilder();
		sb.append((hc >= 0 ? "0" : "1"));
		hc = (hc >= 0 ? hc : (0 - hc));
		sb.append(hc);
		String retVal = "TBL_CLUSTER_" + sb.toString();
		return retVal;
	}

	public static String tblClusterNameForAttSetNew(IAttributeSet attSet) {
		return nameTableOnAttSet(attSet, "TBL_CLUSTER_");
	}

	public static String nameTableOnAttSet(IAttributeSet attSet, String prefix) {
		StringBuilder sb = new StringBuilder();
		for (int i = attSet.nextAttribute(0); i >= 0; i = attSet.nextAttribute(i + 1)) {
			sb.append(i).append('_');
		}
		String retVal = prefix + sb.toString();
		return retVal;
	}

	public void init() {
		SmallDBInMemory.shutdown();
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
			// select distinct count as num of lines
			Statement stmt = null;
			String sqlQuery = "Select Count(*) AS CNT from (Select DISTINCT * FROM " + TBL_NAME + ") ";
			try {
				stmt = DBConnection.createStatement();
				ResultSet rs = stmt.executeQuery(sqlQuery);
				rs.next();
				int numDistinctLines = rs.getInt("CNT");
				numLines = numDistinctLines;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Connection getDBConnection() {
		Connection dbConnection = null;
		try {
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		}
		try {
			dbConnection = DriverManager.getConnection(DB_URL, USER, PASS);
			return dbConnection;
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return dbConnection;
	}

	// delete all DB tables
	public static void shutdown() {
//			System.out.println(Thread.currentThread().getId() + ": calling Master shutdown");
		try {
			Connection DBConnection = CompressedDB.getDBConnection();
			ResultSet rs = DBConnection.getMetaData().getTables(null, null, "TBL_CLUSTER%", null);

			Statement stmtDeleteTables = DBConnection.createStatement();
			int numTblsRemoved = 0;
			while (rs.next()) {
				String tblName = rs.getString(3);
				String sql = "DROP TABLE " + tblName;
				stmtDeleteTables.addBatch(sql);
				numTblsRemoved++;
			}

			if (numTblsRemoved > 0)
				stmtDeleteTables.executeBatch();

			DBConnection.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void dropTblByName(String tblName) {
		try {
			Statement stmt = DBConnection.createStatement();
			String sql = "DROP TABLE " + tblName;
			stmt.executeUpdate(sql);
			DBConnection.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		SmallDBInMemory db = new SmallDBInMemory("data/chess/chess.csv", 7, false);
		Set<IAttributeSet> sep = new HashSet<>();
		sep.add(new AttributeSet(new int[] { 0, 1 }, 7));
		sep.add(new AttributeSet(new int[] { 1, 4 }, 7));
		// sep.add(new AttributeSet(new int[] {3}, 7));
		Set<IAttributeSet> clus = new HashSet<>();
		clus.add(new AttributeSet(new int[] { 0, 1, 2}, 7));
//			clus.add(new AttributeSet(new int[] {3,2}, 7));
		clus.add(new AttributeSet(new int[] { 0, 1, 3, 4 }, 7));
		clus.add(new AttributeSet(new int[] { 1,4,5,6 }, 7));
		HashMap<IAttributeSet, Set<IAttributeSet>> sepsToClusters = new HashMap<IAttributeSet, Set<IAttributeSet>>();
		HashMap<IAttributeSet, Set<IAttributeSet>> clustersToSeps = new HashMap<IAttributeSet, Set<IAttributeSet>>();
		for (IAttributeSet separator : sep) {
			if (separator.nextAttribute(0) == -1) {
				continue;
			}
			Set<IAttributeSet> clustersForSep = new HashSet<IAttributeSet>();
			for (IAttributeSet cluster : clus) {
				if (cluster.contains(separator)) {
					clustersForSep.add(cluster);
				}
				Set<IAttributeSet> sepForCluster = null;
				if ((sepForCluster = clustersToSeps.get(cluster)) == null) {
					sepForCluster = new HashSet<IAttributeSet>();
					clustersToSeps.put(cluster, sepForCluster);
				}
				sepForCluster.add(separator);
			}
			sepsToClusters.put(separator, clustersForSep);
		}
		db.init();
		decompositionSize ds = new decompositionSize();
		long start;
		start = System.currentTimeMillis();
		System.out.println(db.testDecomposition(clus, sep, ds));
		System.out.println("old: " + (System.currentTimeMillis() - start));
		start = System.currentTimeMillis();
		System.out.println(db.spuritousTuples(clus, sep, ds));
		System.out.println("new: " + (System.currentTimeMillis() - start));
//		StringBuilder b = new StringBuilder();
//		System.out.println(sepsToClusters);
//
//		System.out.println(clustersToSeps);
//		exploreCluster(new HashSet<>(), new HashSet<>(), sepsToClusters, clustersToSeps,
//				new AttributeSet(new int[] { 1, 6, 5 }, 7), new AttributeSet(7), b);
//		System.out.println(b.toString());
	}

	private int spuritousTuples(Set<IAttributeSet> clusters, Set<IAttributeSet> separators, decompositionSize ds) {
		SmallDBInMemory.shutdown(); // delete all previously created projections

		// map every separator to the set of clusters that contain it
		HashMap<IAttributeSet, Set<IAttributeSet>> sepsToClusters = new HashMap<IAttributeSet, Set<IAttributeSet>>();
		HashMap<IAttributeSet, Set<IAttributeSet>> clustersToSeps = new HashMap<IAttributeSet, Set<IAttributeSet>>();
		for (IAttributeSet separator : separators) {
			if (separator.nextAttribute(0) == -1) {
				continue;
			}
			Set<IAttributeSet> clustersForSep = new HashSet<IAttributeSet>();
			for (IAttributeSet cluster : clusters) {
				if (cluster.contains(separator)) {
					clustersForSep.add(cluster);
					Set<IAttributeSet> sepForCluster;
					if ((sepForCluster = clustersToSeps.get(cluster)) == null) {
						sepForCluster = new HashSet<IAttributeSet>();
						clustersToSeps.put(cluster, sepForCluster);
					}
					sepForCluster.add(separator);
				}
			}
			sepsToClusters.put(separator, clustersForSep);
		}
		// create a decomposition for the clusters
		int smallestRelation = Integer.MAX_VALUE;
		int largestRelation = 0;
		int totalTuples = 0;
		int totalCells = 0;
		for (IAttributeSet cluster : clusters) {
			int clusterProjectionSize = generateProjectionTable(cluster);
			smallestRelation = (clusterProjectionSize < smallestRelation) ? clusterProjectionSize : smallestRelation;
			largestRelation = (clusterProjectionSize > largestRelation) ? clusterProjectionSize : largestRelation;
			totalTuples += clusterProjectionSize;
			totalCells += (clusterProjectionSize * cluster.cardinality());
		}
		ds.largestRelation = largestRelation;
		ds.smallestRelation = smallestRelation;
		ds.totalTuplesInDecomposition = totalTuples;
		ds.totalCellsInDecomposition = totalCells;
		
		Set<IAttributeSet> visitedSep = new HashSet<>();
		Set<IAttributeSet> visitedCluster = new HashSet<>();
		StringBuilder b = new StringBuilder();
		List<String> tableList = new ArrayList<>();
		for (IAttributeSet cluster : clusters) {
			IAttributeSet groupby = new AttributeSet(cluster.length());
			if (!visitedCluster.contains(cluster)) {
				exploreCluster(visitedSep, visitedCluster, sepsToClusters, clustersToSeps, cluster, groupby, b);
				tableList.add(nameTableOnAttSet(cluster, "GR_CLUS_"));
			}
		}
		if (tableList.isEmpty()) {
			throw new IllegalStateException("No cluster?");
		}
		b.append(" SELECT ").append(tableList.get(0)).append(".cnt");
		for (int i = 1; i < tableList.size(); ++i) {
			b.append('*').append(tableList.get(i)).append(".cnt");
		}		

		b.append(" AS cnt FROM ").append(tableList.get(0));
		for (int i = 1; i < tableList.size(); ++i) {
			b.append(", ").append(tableList.get(i));
		}		
		
		int spuritous = 0;
		try {
			Statement s = DBConnection.createStatement();
			ResultSet rs = s.executeQuery(b.toString());
			if (rs.next()) {
				spuritous = rs.getInt("cnt") - numLines;
			}
		} catch (SQLException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		SmallDBInMemory.shutdown(); // delete all previously created projections
		return spuritous;
	}

	public static void exploreCluster(Set<IAttributeSet> visitedSep, Set<IAttributeSet> visitedCluster,
			Map<IAttributeSet, Set<IAttributeSet>> sepsToClusters, Map<IAttributeSet, Set<IAttributeSet>> clusterToSeps,
			IAttributeSet cluster, IAttributeSet groupby, StringBuilder b) {

		visitedCluster.add(cluster);
		String clusterTableName = tblClusterNameForAttSetNew(cluster);
		StringBuilder from = new StringBuilder(" FROM ").append(clusterTableName);
		StringBuilder where = new StringBuilder();
		StringBuilder sum = new StringBuilder("SUM(1");
		if (clusterToSeps.get(cluster) != null) {
			for (IAttributeSet sep : clusterToSeps.get(cluster)) {
				if (visitedSep.contains(sep))
					continue;
				exploreSep(visitedSep, visitedCluster, sepsToClusters, clusterToSeps, sep, b);
				String subQueryName = nameTableOnAttSet(sep, "SEP_");
				from.append(", ").append(subQueryName);
				for (int i : ((AttributeSet) sep).setIdxList()) {
					if (where.length() != 0) {
						where.append(" AND ");
					}
					where.append(clusterTableName).append(".att").append(i).append(" = ").append(subQueryName)
							.append(".att").append(i);
				}
				sum.append('*').append(subQueryName).append(".cnt");
			}
		}
		sum.append(") AS cnt");
		StringBuilder group = new StringBuilder();
		for (int i : ((AttributeSet) groupby).setIdxList()) {
			if (group.length() != 0) {
				group.append(", ");
			}
			group.append(clusterTableName).append(".att").append(i);
		}

		if (b.length() == 0) {
			b.append("WITH ");
		} else {
			b.append(", ");
		}
		b.append(nameTableOnAttSet(cluster, "GR_CLUS_")).append(" AS (");

		b.append("SELECT ");
		if (group.length() != 0) {
			b.append(group).append(", ");
		}
		b.append(sum);
		b.append(from);
		if (where.length() != 0) {
			b.append(" WHERE ").append(where);
		}
		if (group.length() != 0) {
			b.append(" GROUP BY ").append(group);
		}
		b.append(')');
	}

	public static void exploreSep(Set<IAttributeSet> visitedSep, Set<IAttributeSet> visitedCluster,
			Map<IAttributeSet, Set<IAttributeSet>> sepsToClusters, Map<IAttributeSet, Set<IAttributeSet>> clusterToSeps,
			IAttributeSet sep, StringBuilder b) {
		visitedSep.add(sep);
		List<String> from = new ArrayList<>();
		StringBuilder where = new StringBuilder();
		StringBuilder cnt = new StringBuilder();
		if (sepsToClusters.get(sep) != null) {
			for (IAttributeSet cluster : sepsToClusters.get(sep)) {
				if (visitedCluster.contains(cluster))
					continue;
				exploreCluster(visitedSep, visitedCluster, sepsToClusters, clusterToSeps, cluster, sep, b);
				String subQueryName = nameTableOnAttSet(cluster, "GR_CLUS_");
				from.add(subQueryName);
				if (cnt.length() != 0) {
					cnt.append('*');
				}
				cnt.append(subQueryName).append(".cnt");
			}
		}
		cnt.append(" AS cnt");
		if (b.length() == 0) {
			throw new IllegalStateException("Critical error, detect seperator at the end.");
		}

		if (from.isEmpty()) {
			b.append(nameTableOnAttSet(sep, "SEP_")).append(" AS (SELECT 1 AS cnt)");
		}

		b.append(", ");
		b.append(nameTableOnAttSet(sep, "SEP_")).append(" AS (SELECT ").append(cnt);
		for (int i : ((AttributeSet) sep).setIdxList()) {
			b.append(", ").append(from.get(0)).append(".att").append(i);
			for (int j = 1; j < from.size(); j++) {
				if (where.length() != 0) {
					where.append(" AND ");
				}
				where.append(from.get(j)).append(".att").append(i).append(" = " ).append(from.get(j - 1)).append(".att").append(i);
			}
		}
		b.append(" FROM ").append(from.get(0));
		for (int j = 1; j < from.size(); j++) {
			b.append(", ").append(from.get(j));
		}
		if (where.length() != 0) {
			b.append(" WHERE ").append(where);
		}
		b.append(")");
	}
}
