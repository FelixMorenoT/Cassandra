package ConexionCassandra.ConCass;

import java.awt.List;
import java.util.ArrayList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Conexion {
	
	private Session session;
	private KeyspaceMetadata ksmd;
	private Cluster cluster;
	private Metadata metadata;

	public void connect(String node) {
	    cluster = Cluster.builder().addContactPoint(node).build();
	    metadata = cluster.getMetadata();
	    
	    System.out.println("Cassandra connection established");
	    System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
	    
	    for (Host host : metadata.getAllHosts()) {
	        System.out.printf("Datatacenter: %s; Host: %s; Rack: %s; \n", host.getDatacenter(), host.getAddress(), host.getRack());
	        		session = cluster.connect();

	    }
	}
	
	public void createSchema(String nameKeySpace) {
			
		try {
			String keySpace = "CREATE KEYSPACE "+nameKeySpace+" WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};";
		    session.execute(keySpace);
		    System.out.println("KeySpace Creada.");
		} catch (Exception e) {
			System.out.println("KeySpace '"+nameKeySpace+"' , ya existe.");
		}

		    
		    /*ksmd= metadata.getKeyspace(nameKeySpace);

		    String colunmFamily ="CREATE COLUMNFAMILY simplex.songs (id uuid PRIMARY KEY,title text,album text,artist text, tags set<text>,data blob);";
		    session.execute(colunmFamily);*/
	}
	
	public void createColunmFamily() {
		
	}
	
	public void consultingAll(String nameKeySpace,String tableName) {
		String queryConsulting = "SELECT * FROM "+nameKeySpace+"."+tableName+";";
		ResultSet results= session.execute(queryConsulting);
		
		for(Row r:results.all()){
		    System.out.println(r.toString());
		}
	}
	
	public void consultingEspecific() {
		
	}
	
	public void dataLoad() {
		
	}
	
	public void deleteKeySpace(String nameKeySpace) {
		try {
			String queryDelete = "DROP KEYSPACE "+nameKeySpace +";";
			session.execute(queryDelete);
			System.out.println("KeySpace Eliminada.");
		} catch (Exception e) {
			//System.out.println(e);

		}


	}
}
