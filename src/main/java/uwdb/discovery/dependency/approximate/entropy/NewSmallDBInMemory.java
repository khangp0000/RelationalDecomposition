package uwdb.discovery.dependency.approximate.entropy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
import com.opencsv.CSVParser;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

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
    public final long numTuples;
    public final long numCells;

    // Running or not
    private boolean running;

    public Connection conn = null;
    private String db_url;

    public NewSmallDBInMemory(String file, int numAtt, boolean hasHeader) throws Exception {
        db_url = "jdbc:h2:mem:db" + DB_IDX.getAndIncrement() + ";LOCK_TIMEOUT=10000";
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(db_url, USER, PASS);

        initDB(file, numAtt, hasHeader);
        numTuples = numTuples();
        numCells = numTuples * numAtt;
        running = false;
    }

    private void initDB(String file, int numAtt, boolean hasHeader) throws Exception {
        Statement st = createStatement();

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

    private long numTuples() throws SQLException {
        Statement st = createStatement();
        ResultSet rs =
                st.executeQuery("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM " + TBL_NAME + ");");
        if (!rs.next()) {
            throw new IllegalStateException("COUNT always return 1 row");
        }
        long ret = rs.getLong(1);
        st.close();
        return ret;
    }

    public synchronized Connection getDBConnection() {
        return conn;
    }

    public synchronized Statement createStatement() throws SQLException {
        if (conn == null) {
            throw new IllegalStateException("DB is closed");
        }
        return conn.createStatement();
    }

    public synchronized void close() throws SQLException {
        conn.close();
        conn = null;
    }

    public synchronized void stop() throws SQLException {
        if (conn == null) {
            throw new IllegalStateException("DB is closed");
        }
        Connection temp = DriverManager.getConnection(db_url, USER, PASS);
        conn.close();
        conn = temp;
        Statement st = createStatement();
        ResultSet rs = conn.getMetaData().getTables(null, null, "CLUSTER_%", null);
        List<String> cl_tables = new ArrayList<>();
        while (rs.next()) {
            cl_tables.add(rs.getString(3));
        }
        if (!cl_tables.isEmpty())
            st.executeUpdate(
                    cl_tables.stream().collect(Collectors.joining(",", "DROP TABLE ", ";")));
        st.close();
    }

    private static String nameTableOnAttSet(IAttributeSet attSet, String prefix) {
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(attSet.bitString());
        return sb.toString();
    }

    private static String clusterTableOnAttSet(IAttributeSet attSet) {
        return nameTableOnAttSet(attSet, "CLUSTER_");
    }

    private long generateProjectionTables(AttributeSet attSet) throws SQLException {
        Statement st = createStatement();
        String clusterTableName = clusterTableOnAttSet(attSet);
        st.executeUpdate(new StringBuilder("CREATE TABLE ").append(clusterTableName)
                .append(" AS (SELECT DISTINCT ")
                .append(attSet.setIdxList().stream().map(i -> "att" + i)
                        .collect(Collectors.joining(",")))
                .append(",CAST(1 AS BIGINT) AS cnt FROM ").append(TBL_NAME).append(");")
                .toString());

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + clusterTableName);
        if (!rs.next()) {
            throw new IllegalStateException("COUNT always return 1 row");
        }
        long ret = rs.getLong(1);
        st.close();
        return ret;
    }

    public DecompositionInfo proccessDecomposition(Set<IAttributeSet> clustersSet)
            throws SQLException {
        synchronized (this) {
            if (running) {
                return null;
            }
            running = true;
        }
        try {
            int size = 0;
            List<IAttributeSet> clusters = new ArrayList<>(clustersSet);

            DecompositionInfo dInfo = new DecompositionInfo();

            if ((size = clusters.size()) == 0) {
                return dInfo;
            }


            IAttributeSet c1 = clusters.remove(size - 1);
            String c1Name = clusterTableOnAttSet(c1);

            dInfo.add(c1, generateProjectionTables((AttributeSet) c1));
            Statement st = createStatement();
            while ((size = clusters.size()) > 0) {

                IAttributeSet c2 = clusters.remove(size - 1);
                String c2Name = clusterTableOnAttSet(c2);

                dInfo.add(c2, generateProjectionTables((AttributeSet) c2));

                IAttributeSet newCluster = c1.union(c2);
                String newClusterName = clusterTableOnAttSet(newCluster);

                IAttributeSet group = new AttributeSet(c1.length());

                clusters.stream().forEach(a -> group.or(a));
                group.intersectNonConst(newCluster);

                String groupString = ((AttributeSet) group).setIdxList().stream()
                        .map(i -> "att" + i).collect(Collectors.joining(","));

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


                st.addBatch("ALTER TABLE " + c1Name + " ALTER COLUMN cnt RENAME TO cnt1;");
                st.addBatch("ALTER TABLE " + c2Name + " ALTER COLUMN cnt RENAME TO cnt2;");
                st.addBatch(sb.toString());
                st.addBatch("DROP TABLE " + c1Name + ";");
                st.addBatch("DROP TABLE " + c2Name + ";");
                st.addBatch(
                        "ALTER TABLE TMP" + newClusterName + " RENAME TO " + newClusterName + ";");

                st.executeBatch();

                c1 = newCluster;
                c1Name = newClusterName;
            }


            ResultSet rs = st.executeQuery("SELECT SUM(cnt) FROM " + c1Name);
            if (!rs.next()) {
                throw new IllegalStateException("COUNT always return 1 row");
            }
            int ret = rs.getInt(1);
            st.executeUpdate("DROP TABLE " + c1Name + ";");
            st.close();

            dInfo.spuriousTuples = ret - numTuples;
            return dInfo;
        } finally {
            synchronized (this) {
                running = false;
            }
        }
    }

    public static class DecompositionInfo {
        public long smallestRelation;
        public long largestRelation;
        public long totalTuplesInDecomposition;
        public long totalCellsInDecomposition;
        public long spuriousTuples;

        public DecompositionInfo() {
            smallestRelation = Long.MAX_VALUE;
            largestRelation = 0;
            totalTuplesInDecomposition = 0;
            totalCellsInDecomposition = 0;
            spuriousTuples = -1;
        }

        public void add(IAttributeSet att, long tuples_cnt) {
            largestRelation = Math.max(smallestRelation, tuples_cnt);
            smallestRelation = Math.min(largestRelation, tuples_cnt);
            totalTuplesInDecomposition += tuples_cnt;
            totalCellsInDecomposition += tuples_cnt * att.cardinality();
        }
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

        // Thread thread = new Thread(() -> {
        long start = System.currentTimeMillis();
        try {
            System.out.println(db.proccessDecomposition(clus).spuriousTuples);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println((System.currentTimeMillis() - start) + " ms");
        // });
        // thread.start();
        // BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        // reader.readLine();
        // long start = System.currentTimeMillis();
        // try {
        // System.out.println(db.proccessDecomposition(clus).spuriousTuples);
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        // System.out.println((System.currentTimeMillis() - start) + " ms");
        // reader.readLine();
        // db.stop();
        // reader.readLine();
        // start = System.currentTimeMillis();
        // try {
        // System.out.println(db.proccessDecomposition(clus).spuriousTuples);
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        // System.out.println((System.currentTimeMillis() - start) + " ms");

    }
}

