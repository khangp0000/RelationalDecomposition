package uwdb.discovery.dependency.approximate.entropy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.management.RuntimeErrorException;
import com.opencsv.CSVParser;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.entropy.SmallDBInMemory.decompositionSize;

public class NewSmallDBInMemory {

    static final String SEP = ",";
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";

    // Database credentials
    static final String USER = "sa";
    static final String PASS = "";

    // Parallelism
    static final int TUPLES_ADD_AT_A_TIME = 1000;

    // Differentiate different databasee
    static final AtomicInteger DB_IDX = new AtomicInteger();

    // Universal main table name
    public static String TBL_NAME = "CSVTblEncoding";

    // Database information
    public final int numTuples;
    public final int numCells;

    public Connection conn = null;
    private String db_url;

    public NewSmallDBInMemory(String file, int numAtt, boolean hasHeader) throws Exception {
        db_url = "jdbc:h2:mem:db" + DB_IDX.getAndIncrement() + ";LOCK_TIMEOUT=10000";
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(db_url, USER, PASS);

        initDB(file, numAtt, hasHeader);
        numTuples = numTuples();
        numCells = numTuples * numAtt;
    }

    private void initDB(String file, int numAtt, boolean hasHeader) throws Exception {
        Statement st = conn.createStatement();

        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(TBL_NAME).append(" (");
        String sql = IntStream.range(0, numAtt).mapToObj(i -> "att" + i + " INT NOT NULL")
                .collect(Collectors.joining(",", sb.toString(), ");"));

        st.executeUpdate(sql);


        sb = new StringBuilder("INSERT INTO ").append(TBL_NAME).append(" VALUES ");
        AtomicIntegerArray domainSizes = new AtomicIntegerArray(numAtt);

        List<Map<String, Integer>> dictionaryEncoding = new ArrayList<>(numAtt);
        for (int i = 0; i < numAtt; ++i) {
            dictionaryEncoding.add(new ConcurrentHashMap<>());
        }

        CSVParser parser = new CSVParser();

        BufferedReader reader = new BufferedReader(new FileReader(file));
        if (hasHeader) {
            reader.readLine();
        }

        List<String> lines = new ArrayList<>(TUPLES_ADD_AT_A_TIME);
        while (true) {
            String line = reader.readLine();
            if (line != null && !line.isBlank())
                lines.add(line);
            if ((line == null && lines.size() > 0) || lines.size() == TUPLES_ADD_AT_A_TIME) {
                sql = lines.stream().parallel()
                        .map(s -> processLine(parser, s, numAtt, domainSizes, dictionaryEncoding))
                        .collect(Collectors.joining(",", sb.toString(), ";"));
                st.executeUpdate(sql);
                lines.clear();
            }
            if (line == null) {
                reader.close();
                st.close();
                return;
            }
        }
    }

    private static String processLine(CSVParser parser, String line, int numAtt,
            AtomicIntegerArray domainSizes, List<Map<String, Integer>> dictionaryEncoding) {
        String[] tokens;
        try {
            tokens = parser.parseLine(line);
            return IntStream.range(0, numAtt).parallel().mapToObj(i -> {
                String val = "";
                if (i < tokens.length) {
                    val = tokens[i].trim();
                }
                return dictionaryEncoding.get(i)
                        .computeIfAbsent(val, v -> domainSizes.getAndIncrement(i)).toString();
            }).collect(Collectors.joining(",", "(", ")"));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private int numTuples() throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs =
                st.executeQuery("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM " + TBL_NAME + ");");
        if (!rs.next()) {
            throw new IllegalStateException("COUNT always return 1 row");
        }
        int ret = rs.getInt(1);
        st.close();
        return ret;
    }

    public Connection getDBConnection() {
        return conn;
    }

    public void close() throws SQLException {
        conn.close();
        conn = null;
    }

    private static String nameTableOnAttSet(IAttributeSet attSet, String prefix) {
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(attSet.bitString());
        return sb.toString();
    }

    private static String clusterTableOnAttSet(IAttributeSet attSet) {
        return nameTableOnAttSet(attSet, "CLUSTER_");
    }

    private int generateProjectionTables(AttributeSet attSet) throws SQLException {
        Statement st = conn.createStatement();
        String clusterTableName = clusterTableOnAttSet(attSet);
        st.executeUpdate(new StringBuilder("CREATE TABLE ").append(clusterTableName)
                .append(" AS (SELECT DISTINCT ")
                .append(attSet.setIdxList().stream().map(i -> "att" + i)
                        .collect(Collectors.joining(",")))
                .append(",1 AS cnt FROM ").append(TBL_NAME).append(");").toString());

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + clusterTableName);
        if (!rs.next()) {
            throw new IllegalStateException("COUNT always return 1 row");
        }
        int ret = rs.getInt(1);
        st.close();
        return ret;
    }

    public int spuriousTuples(Set<IAttributeSet> clustersSet, decompositionSize ds)
            throws SQLException {
        List<IAttributeSet> clusters = new ArrayList<>(clustersSet);
        int[] clustersSize = clusters.stream().mapToInt(a -> {
            try {
                return generateProjectionTables((AttributeSet) a);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).toArray();

        ds.largestRelation = 0;
        ds.smallestRelation = Integer.MAX_VALUE;
        ds.totalTuplesInDecomposition = 0;
        ds.totalCellsInDecomposition = 0;
        for (int i = 0; i < clustersSize.length; ++i) {
            ds.largestRelation = Math.max(ds.largestRelation, clustersSize[i]);
            ds.smallestRelation = Math.min(ds.largestRelation, clustersSize[i]);
            ds.totalTuplesInDecomposition += clustersSize[i];
            ds.largestRelation = clustersSize[i] * clusters.get(i).cardinality();
        }


                return spuriousTuples(clusters);
    }

    private int spuriousTuples(List<IAttributeSet> clusters) throws SQLException {
        int size = 0;
        Statement st = conn.createStatement();
        while ((size = clusters.size()) > 1) {
            IAttributeSet c1 = clusters.remove(size - 1);
            String c1Name = clusterTableOnAttSet(c1);

            IAttributeSet c2 = clusters.remove(size - 2);
            String c2Name = clusterTableOnAttSet(c2);

            IAttributeSet newCluster = c1.union(c2);
            String newClusterName = clusterTableOnAttSet(newCluster);

            IAttributeSet group = new AttributeSet(c1.length());

            clusters.stream().forEach(a -> group.or(a));
            group.intersectNonConst(newCluster);

            String groupString = ((AttributeSet) group).setIdxList().stream().map(i -> "att" + i)
                    .collect(Collectors.joining(","));

            StringBuilder sb = new StringBuilder("CREATE TABLE TMP").append(newClusterName)
                    .append(" AS (SELECT SUM(").append(c1Name).append(".cnt1").append("*")
                    .append(c2Name).append(".cnt2").append(") AS cnt");

            if (!groupString.isEmpty()) {
                sb.append(",").append(groupString);
            }

            sb.append(" FROM ").append(c1Name).append(" NATURAL JOIN ").append(c2Name);

            if (!groupString.isEmpty()) {
                sb.append(" GROUP BY ").append(groupString);
            }

            sb.append(");");


            st.executeUpdate("ALTER TABLE " + c1Name + " ALTER COLUMN cnt RENAME TO cnt1;");
            st.executeUpdate("ALTER TABLE " + c2Name + " ALTER COLUMN cnt RENAME TO cnt2;");
            st.executeUpdate(sb.toString());
            st.executeUpdate("DROP TABLE " + c1Name + ";");
            st.executeUpdate("DROP TABLE " + c2Name + ";");
            st.executeUpdate(
                    "ALTER TABLE TMP" + newClusterName + " RENAME TO " + newClusterName + ";");
            clusters.add(newCluster);
        }

        ResultSet rs =
                st.executeQuery("SELECT SUM(cnt) FROM " + clusterTableOnAttSet(clusters.get(0)));
        if (!rs.next()) {
            throw new IllegalStateException("COUNT always return 1 row");
        }
        int ret = rs.getInt(1);
        st.close();
        return ret - numTuples;
    }

    public static void main(String[] args) throws Exception {
        NewSmallDBInMemory db = new NewSmallDBInMemory("adult.csv", 15, false);

        Set<IAttributeSet> clus = new HashSet<>();
        clus.add(new AttributeSet(new int[] {0, 1}, 15));
        clus.add(new AttributeSet(new int[] {1, 2}, 15));
        clus.add(new AttributeSet(new int[] {2, 3}, 15));
        clus.add(new AttributeSet(new int[] {3, 4}, 15));
        clus.add(new AttributeSet(new int[] {4, 5}, 15));
        clus.add(new AttributeSet(new int[] {5, 6}, 15));
        clus.add(new AttributeSet(new int[] {6, 7}, 15));
        clus.add(new AttributeSet(new int[] {7, 8}, 15));
        clus.add(new AttributeSet(new int[] {8, 9}, 15));
        clus.add(new AttributeSet(new int[] {9, 10}, 15));
        clus.add(new AttributeSet(new int[] {10, 11}, 15));
        clus.add(new AttributeSet(new int[] {11, 12}, 15));
        clus.add(new AttributeSet(new int[] {12, 13}, 15));
        clus.add(new AttributeSet(new int[] {13, 14}, 15));

        decompositionSize ds = new decompositionSize();
        long start = System.currentTimeMillis();
        System.out.println(db.spuriousTuples(clus, ds));
        System.out.println((System.currentTimeMillis() - start) + " ms");
        // System.out.println(db.conn.createStatement().executeUpdate(
        // "CREATE VIEW CL AS (SELECT DISTINCT ATT0, ATT1 FROM " + TBL_NAME + ")"));
        // ResultSet rs = db.conn.createStatement().executeQuery("SELECT * FROM CL");
        // ResultSetMetaData rsmd = rs.getMetaData();
        // System.out.println("querying SELECT * FROM XXX");
        // int columnsNumber = rsmd.getColumnCount();
        // while (rs.next()) {
        // for (int i = 1; i <= columnsNumber; i++) {
        // if (i > 1)
        // System.out.print(", ");
        // String columnValue = rs.getString(i);
        // System.out.print(columnValue + " " + rsmd.getColumnName(i));
        // }
        // System.out.println("");
        // }
    }
}
