package uwdb.discovery.dependency.approximate.entropy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
// import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.opencsv.CSVParser;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.entropy.NewSmallDBInMemory.DecompositionRunStatus.StatusCode;

public class NewSmallDBInMemory implements AutoCloseable {
    public static class CanceledJobException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    static final String SEP = ",";
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.sqlite.JDBC";

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
    public final List<String> header;
    public final String filename;
    public final int numAtt;
    public final long numTuples;
    public final long numCells;

    private ClustersConsumer[] threads;

    private final BlockingQueue<Set<IAttributeSet>> jobs;
    private final Map<Set<IAttributeSet>, DecompositionRunStatus> statusMap;
    private int cacheMax;
    private final String masterdb;

    public NewSmallDBInMemory(String file, int numAtt, boolean hasHeader) throws Exception {
        this(file, numAtt, hasHeader,
                Math.min(4, Math.max(Runtime.getRuntime().availableProcessors() - 1, 1)), 100);
    }

    public NewSmallDBInMemory(String file, int numAtt, boolean hasHeader, int numThread,
            int cacheNum) throws Exception {
        List<String> tempHeader = new ArrayList<>(numAtt);
        for (int i = 0; i < numAtt; ++i) {
            tempHeader.add(null);
        }
        masterdb = "db" + DB_IDX.getAndIncrement();
        String masterdb_url = "jdbc:sqlite:file:" + masterdb + "?mode=memory&cache=shared";

        if (numThread < 1 || cacheNum < 1) {
            throw new IllegalArgumentException();
        }
        Class.forName(JDBC_DRIVER);
        cacheMax = cacheNum;
        jobs = new LinkedBlockingQueue<>();
        statusMap = new LinkedHashMap<Set<IAttributeSet>, DecompositionRunStatus>(cacheMax, 0.75f,
                true) {
            static final long serialVersionUID = 0xab446bbL;

            public synchronized boolean removeEldestEntry(
                    Map.Entry<Set<IAttributeSet>, DecompositionRunStatus> eldest) {
                return size() > cacheMax;
            }
        };

        int refdbidx = 0;;
        threads = new ClustersConsumer[numThread];
        try (Connection masterConnection = DriverManager.getConnection(masterdb_url, USER, PASS)) {
            initDB(file, numAtt, hasHeader, masterConnection, tempHeader);
            header = Collections.unmodifiableList(tempHeader);
            for (int i = 0; i < numThread; ++i) {
                String db_url = "jdbc:sqlite:file:db" + DB_IDX.getAndIncrement()
                        + "?mode=memory&cache=shared";
                threads[i] = new ClustersConsumer(i, db_url);
                attachMasterDB(threads[i].conn);
                threads[i].start();
            }
        } catch (SQLException e) {
            throw e;
        }

        numTuples = numTuples(threads[0].conn);
        numCells = numTuples * numAtt;
        this.numAtt = numAtt;
        this.filename = new File(file).getName();
    }

    public DecompositionRunStatus submitJob(Set<IAttributeSet> s) throws InterruptedException {
        synchronized (statusMap) {
            DecompositionRunStatus dRunStatus = statusMap.get(s);
            if (dRunStatus == null) {
                dRunStatus = new DecompositionRunStatus();
                statusMap.put(s, dRunStatus);
                jobs.put(s);
            } else {
                synchronized (dRunStatus) {
                    if (dRunStatus.status == StatusCode.CANCELED) {
                        dRunStatus = new DecompositionRunStatus();
                        statusMap.put(s, dRunStatus);
                        jobs.put(s);
                    }
                }
            }

            return dRunStatus;
        }
    }

    public DecompositionInfo submitJobSynchronous(Set<IAttributeSet> s) throws Exception {
        DecompositionRunStatus dRunStatus = submitJob(s);
        synchronized (dRunStatus) {
            while (dRunStatus.status == StatusCode.PENDING
                    || dRunStatus.status == StatusCode.RUNNING) {
                dRunStatus.wait();
            }

            if (dRunStatus.status == StatusCode.CANCELED
                    || dRunStatus.status == StatusCode.FAILED) {
                throw dRunStatus.exception;
            }

            return dRunStatus.dInfo;
        }
    }

    public DecompositionRunStatus cancelJob(Set<IAttributeSet> s)
            throws InterruptedException, SQLException {
        synchronized (statusMap) {
            DecompositionRunStatus dRunStatus = statusMap.get(s);
            if (dRunStatus != null) {
                synchronized (dRunStatus) {
                    if (dRunStatus.status == StatusCode.RUNNING) {
                        dRunStatus.thread.stopRunning();
                    }

                    if (dRunStatus.status == StatusCode.RUNNING
                            || dRunStatus.status == StatusCode.PENDING) {
                        dRunStatus.status = StatusCode.CANCELED;
                        dRunStatus.exception = new CanceledJobException();
                    }
                }
            }

            return dRunStatus;
        }
    }

    private void attachMasterDB(Connection conn) throws Exception {
        Statement st = conn.createStatement();
        st.executeUpdate("attach 'file:" + masterdb + "?mode=memory&cache=shared' as " + masterdb);
        st.executeUpdate("CREATE TABLE " + TBL_NAME + " AS SELECT DISTINCT * FROM " + masterdb + "."
                + TBL_NAME);
        st.executeUpdate("DETACH DATABASE " + masterdb);
        st.close();
    }

    private void initDB(String file, int numAtt, boolean hasHeader, Connection conn,
            List<String> header) throws Exception {
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
            String[] parsedHeader = parser.parseLine(reader.readLine());
            int size = Math.min(parsedHeader.length, header.size());
            for (int i = 0; i < size; ++i) {
                header.set(i, parsedHeader[i]);
            }
        }

        header = Collections.unmodifiableList(header);

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

    private void initDBLinked(Connection conn, int refdbidx) throws Exception {
        Statement st = conn.createStatement();

        StringBuilder sb = new StringBuilder("CREATE LINKED TABLE ").append(TBL_NAME)
                .append(" ('org.h2.Driver', 'jdbc:h2:mem:db" + refdbidx + "', '" + USER + "', '"
                        + PASS + "', '" + TBL_NAME + "') READONLY;");

        st.executeUpdate(sb.toString());
        st.close();
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

    private long numTuples(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs =
                st.executeQuery("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM " + TBL_NAME + ");");
        if (!rs.next()) {
            throw new IllegalStateException("COUNT always return 1 row");
        }
        long ret = rs.getLong(1);
        st.close();
        return ret;
    }

    public synchronized void close() {
        for (ClustersConsumer cc : threads) {
            cc.close();
        }
    }

    private static String nameTableOnAttSet(IAttributeSet attSet, String prefix) {
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(attSet.bitString());
        return sb.toString();
    }

    private class ClustersConsumer extends Thread {
        private final int idx;
        private Connection conn;
        private Statement running_st;

        public ClustersConsumer(int idx, String db_url) throws SQLException {
            this.idx = idx;
            conn = DriverManager.getConnection(db_url, USER, PASS);
        }

        @Override
        public void run() {
            try {
                while (!Thread.interrupted() && conn != null) {
                    Set<IAttributeSet> clustersSet;
                    clustersSet = jobs.take();
                    boolean run = false;
                    DecompositionRunStatus dRunStatus;
                    synchronized (statusMap) {
                        dRunStatus = statusMap.get(clustersSet);
                        synchronized (dRunStatus) {
                            if (dRunStatus.status == DecompositionRunStatus.StatusCode.PENDING) {
                                dRunStatus.status = DecompositionRunStatus.StatusCode.RUNNING;
                                dRunStatus.thread = this;
                                run = true;
                            }
                            dRunStatus.notifyAll();
                        }
                    }

                    if (!run) {
                        continue;
                    }


                    try {
                        dRunStatus.dInfo = proccessDecomposition(clustersSet);
                        synchronized (dRunStatus) {
                            dRunStatus.status = StatusCode.FINISHED;
                            dRunStatus.thread = null;
                            dRunStatus.notifyAll();
                        }
                    } catch (Exception e) {
                        synchronized (dRunStatus) {
                            if (dRunStatus.status != StatusCode.CANCELED) {
                                dRunStatus.status = StatusCode.FAILED;
                                dRunStatus.exception = e;
                            } else {
                                dRunStatus.exception.addSuppressed(e);
                            }
                            dRunStatus.thread = null;
                            dRunStatus.notifyAll();
                        }
                    }
                }
            } catch (InterruptedException e1) {
            }
        }

        public DecompositionInfo proccessDecomposition(Set<IAttributeSet> clustersSet)
                throws SQLException {

            Connection conn = null;
            synchronized (this) {
                conn = this.conn;
            }

            int size = 0;
            List<IAttributeSet> clusters = new ArrayList<>(clustersSet);

            DecompositionInfo dInfo = new DecompositionInfo();

            if ((size = clusters.size()) == 0) {
                return dInfo;
            }

            IAttributeSet c1 = clusters.remove(size - 1);
            String c1Name = clusterTableOnAttSet(c1);

            long count = generateProjectionTables((AttributeSet) c1, conn);
            dInfo.add(c1, count);
            Statement st = conn.createStatement();
            while ((size = clusters.size()) > 0) {

                IAttributeSet c2 = clusters.remove(size - 1);
                String c2Name = clusterTableOnAttSet(c2);

                dInfo.add(c2, generateProjectionTables((AttributeSet) c2, conn));
                IAttributeSet newCluster = c1.union(c2);
                IAttributeSet join = c1.intersect(c2);
                String newClusterName = clusterTableOnAttSet(newCluster);

                IAttributeSet group = new AttributeSet(c1.length());

                clusters.stream().forEach(a -> group.or(a));
                group.intersectNonConst(newCluster);

                final String temp1 = c1Name;
                final String temp2 = c2Name;
                final IAttributeSet temp1c = c1;

                String joiString = ((AttributeSet) join).setIdxList().stream()
                        .map(i -> temp1 + ".att" + i + " = " + temp2 + ".att" + i)
                        .collect(Collectors.joining(" AND "));

                String groupString = ((AttributeSet) group).setIdxList().stream()
                        .map(i -> (temp1c.contains(i) ? temp1 : temp2) + ".att" + i)
                        .collect(Collectors.joining(","));

                StringBuilder sb = new StringBuilder("CREATE TABLE TMP").append(newClusterName)
                        .append(" AS SELECT SUM(").append(c1Name).append(".cnt").append("*")
                        .append(c2Name).append(".cnt").append(") AS cnt");

                if (!groupString.isEmpty()) {
                    sb.append(",").append(groupString);
                }

                sb.append(" FROM ").append(c1Name).append(", ").append(c2Name);

                if (!joiString.isEmpty()) {
                    sb.append(" WHERE ").append(joiString);
                }

                if (!groupString.isEmpty()) {
                    sb.append(" GROUP BY ").append(groupString);
                }

                sb.append(";");

                st.addBatch(sb.toString());
                st.addBatch("DROP TABLE " + c1Name + ";");
                st.addBatch("DROP TABLE " + c2Name + ";");
                st.addBatch(
                        "ALTER TABLE TMP" + newClusterName + " RENAME TO " + newClusterName + ";");
                running_st = st;
                st.executeBatch();
                running_st = null;

                c1 = newCluster;
                c1Name = newClusterName;
            }

            running_st = st;
            ResultSet rs = st.executeQuery("SELECT SUM(cnt) FROM " + c1Name);
            running_st = null;
            if (!rs.next()) {
                throw new IllegalStateException("COUNT always return 1 row");
            }
            long ret = rs.getLong(1);
            running_st = st;

            st.executeUpdate("DROP TABLE " + c1Name + ";");
            running_st = null;
            st.close();

            dInfo.spuriousTuples = ret - numTuples;
            return dInfo;
        }

        public synchronized void stopRunning() throws SQLException {
            if (conn == null) {
                throw new IllegalStateException("Consumming thread closed");
            }
            Statement st = running_st;

            if (st != null) {
                st.cancel();
            }

            st = conn.createStatement();
            ResultSet rs = conn.getMetaData().getTables(null, null, "CLUSTER_" + idx + "%", null);
            List<String> cl_tables = new ArrayList<>();
            while (rs.next()) {
                cl_tables.add(rs.getString(3));
            }
            for (String table : cl_tables) {
                st.addBatch("DROP TABLE IF EXISTS " + table);
            }
            st.executeBatch();
            st.close();
        }

        private String clusterTableOnAttSet(IAttributeSet attSet) {
            return nameTableOnAttSet(attSet, "CLUSTER_" + idx + "_");
        }

        private long generateProjectionTables(AttributeSet attSet, Connection conn)
                throws SQLException {
            Statement st = conn.createStatement();
            String clusterTableName = clusterTableOnAttSet(attSet);

            running_st = st;
            st.executeUpdate(new StringBuilder("CREATE TABLE ").append(clusterTableName)
                    .append(" AS SELECT DISTINCT ")
                    .append(attSet.setIdxList().stream().map(i -> "att" + i)
                            .collect(Collectors.joining(",")))
                    .append(",CAST(1 AS BIGINT) AS cnt FROM ").append(TBL_NAME).append(";")
                    .toString());

            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + clusterTableName);
            if (!rs.next()) {
                throw new IllegalStateException("COUNT always return 1 row");
            }
            long ret = rs.getLong(1);
            running_st = null;
            st.close();
            return ret;
        }

        public synchronized void close() {
            try {
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
            } finally {
                interrupt();
            }
        }
    }

    public static class DecompositionRunStatus {
        public enum StatusCode {
            PENDING, RUNNING, FINISHED, FAILED, CANCELED;
        }

        private StatusCode status;
        private ClustersConsumer thread;
        private DecompositionInfo dInfo;
        private Exception exception;

        public DecompositionRunStatus() {
            status = StatusCode.PENDING;
            thread = null;
            dInfo = null;
            exception = null;
        }

        public StatusCode status() {
            return status;
        }

        public DecompositionInfo dInfo() {
            return new DecompositionInfo(dInfo);
        }

        public Exception exception() {
            return exception;
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

        public DecompositionInfo(DecompositionInfo o) {
            smallestRelation = o.smallestRelation;
            largestRelation = o.largestRelation;
            totalTuplesInDecomposition = o.totalTuplesInDecomposition;
            totalCellsInDecomposition = o.totalCellsInDecomposition;
            spuriousTuples = o.spuriousTuples;
        }

        public void add(IAttributeSet att, long tuples_cnt) {
            largestRelation = Math.max(largestRelation, tuples_cnt);
            smallestRelation = Math.min(smallestRelation, tuples_cnt);
            totalTuplesInDecomposition += tuples_cnt;
            totalCellsInDecomposition += tuples_cnt * att.cardinality();
        }
    }

    public static void main(String[] args) throws Exception {
        try (NewSmallDBInMemory db =
                new NewSmallDBInMemory("FAVORITA_train.csv", 4, true, 4, 100)) {

            Set<IAttributeSet> clus = new HashSet<>();

            clus.add(new AttributeSet(new int[] {0, 2, 3}, 4));
            clus.add(new AttributeSet(new int[] {1, 2, 3}, 4));
            // clus.add(new AttributeSet(new int[] {0, 1, 3, 4, 5, 7, 8}, 9));

            // clus.add(new AttributeSet(new int[] {3, 4}, 15));
            // clus.add(new AttributeSet(new int[] {4, 5}, 15));
            // clus.add(new AttributeSet(new int[] {5, 6}, 15));
            // clus.add(new AttributeSet(new int[] {6, 7}, 15));
            // clus.add(new AttributeSet(new int[] {7, 8}, 15));
            // clus.add(new AttributeSet(new int[] {8, 9}, 15));
            // clus.add(new AttributeSet(new int[] {9, 10}, 15));
            // clus.add(new AttributeSet(new int[] {10, 11}, 15));
            // clus.add(new AttributeSet(new int[] {11, 12}, 15));
            // clus.add(new AttributeSet(new int[] {12, 13}, 15));
            // clus.add(new AttributeSet(new int[] {13, 14}, 15));

            // DecompositionRunStatus dRunStatus = db.submitJob(clus);
            System.out.println(db.submitJobSynchronous(clus).totalCellsInDecomposition);
            System.out.println(db.submitJobSynchronous(clus).totalTuplesInDecomposition);
            System.out.println(db.submitJobSynchronous(clus).spuriousTuples);
            // BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            // reader.readLine();

            // dRunStatus = db.cancelJob(clus);
            // reader.readLine();
            // dRunStatus = db.submitJob(clus);
            // reader.readLine();

            // // synchronized (dRunStatus) {
            // // System.out.println(dRunStatus.status);
            // // try {
            // // dRunStatus.exception.printStackTrace();
            // // } catch (Exception e) {
            // // e.printStackTrace();
            // // }
            // // }
            // // clus = new HashSet<>();
            // // clus.add(new AttributeSet(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 15));
            // // clus.add(new AttributeSet(new int[] {9, 10, 11, 12, 13, 14}, 15));
            // // System.out.println(db.submitJobSynchronous(clus).spuriousTuples);
            // System.out.println(dRunStatus.status);
            // reader.readLine();


            // db.close();
            // reader.close();
        }
    }
}

