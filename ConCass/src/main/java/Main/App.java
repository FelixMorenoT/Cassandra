package Main;

import ConexionCassandra.ConCass.Conexion;

public class App {

	public static void main(String[] args) {
	    Conexion client = new Conexion();
	    client.connect("127.0.0.1");
	    client.consulting();
	}

}