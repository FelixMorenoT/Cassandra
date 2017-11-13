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

	public void connect(String node) {
	    cluster = Cluster.builder().addContactPoint(node).build();
	    Metadata metadata = cluster.getMetadata();
	    
	    ksmd =  metadata.getKeyspace("basebosque");
	    
	    System.out.println("Cassandra connection established");
	    System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
	    
	    for (Host host : metadata.getAllHosts()) {
	        System.out.printf("Datatacenter: %s; Host: %s; Rack: %s; \n", host.getDatacenter(), host.getAddress(), host.getRack());
	        		session = cluster.connect();

	    }
	    System.out.println("KeySpace: " + ksmd +"\n");
	}
	
	public void consulting() {
		String queryConsulting = "SELECT * FROM basebosque.datosbosque";
		ResultSet results= session.execute(queryConsulting);
		
		for(Row r:results.all()){
		    System.out.println(r.toString());
		}
	}
	
	public void dataLoad() {
		
	}
}
