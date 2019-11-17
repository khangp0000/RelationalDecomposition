package uwdb.discovery.dependency.approximate.entropy;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;

public class Database extends AbstractDataset {
    protected String host;
    protected String username;
    protected String password;
    protected String connectionString;
    protected Connection connection;

    public Database(String hostName, String username, String password, RelationSchema schema)
    {
        super(schema);
        this.host = hostName;
        this.username = username;
        this.password = password;

        try {
            this.connection = DriverManager.getConnection(this.host, this.username, this.password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Database(String connectionString, RelationSchema schema)
    {
        super(schema);
        this.connectionString = connectionString;
        try {
            this.connection = DriverManager.getConnection(this.connectionString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public RelationSchema getSchema() {
        return schema;
    }

    //TODO
    public double computeEntropy(IAttributeSet subset) {
        return 0;
    }


    public void computeEntropies(HashMap<IAttributeSet, Double> values) {

    }

	@Override
	public long getNumDBScans() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getTotalScanTime() {
		// TODO Auto-generated method stub
		return 0;
	}
}
