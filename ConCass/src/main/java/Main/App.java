package Main;

import ConexionCassandra.ConCass.Conexion;
import weka.*;
import weka.clusterers.SimpleKMeans;
public class App {

	public static void main(String[] args) {
		
	    Conexion client = new Conexion();
	    client.connect("127.0.0.1");
	    client.createSchema("dbf");
	    //client.consultingAll("dbf","dataforest");
	 
	    //client.deleteKeySpace("db1");

	}

}
