

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Mstore {


	private Connection readConn = null;
	private List<Connection> writeConns = null;
	private List<Statement> writeSts = null;
	private Statement readSt;
	private static Coordinator coordinator = null;
	private static MConfig config = null;
	private MODE  mode;

	private Mstore(MODE mode,Integer server) throws SQLException{
		if(mode == MODE.WRITE){
			//Build connections to all servers
			this.writeConns = new ArrayList<>();
			for(String url : config.stores){
				this.writeConns.add( DriverManager.getConnection("jdbc:phoenix:"+url));
			}
		}else{ //READ
			// Get connection to random server
			String url = config.stores.get(server);
			this.readConn = DriverManager.getConnection("jdbc:phoenix:"+url);
		}

		this.mode = mode;
	}
	//initializes  both Coordinator and zookeeper 
	public static void init(MConfig conf){
		//init httpconn
		coordinator = new Coordinator(conf.coordinator);
		config = conf;

	}

	public static Mstore newInstance(MODE mode, int server) throws SQLException {
		return new Mstore(mode,server);
	}

	//Fetch instance
	public static Mstore newInstance(MODE mode) throws SQLException{
		int server =  (int) Math.random() * config.stores.size()-1;
		return new Mstore(mode,server);
	}

	//execute insert, udpate ,delete calls
	public boolean executeUpdate(String query) throws SQLException, IOException{
		return executeUpdate(0,query);
	}

	private boolean executeUpdate(int retryCount, String query) throws SQLException, IOException{
		Long pos =  coordinator.getNextLogPosition();
		String verifyQuery = coordinator.updateLog(pos,query);
		if(query.equals(verifyQuery)){ //success
			if(this.writeConns != null){
				this.writeSts = new ArrayList<>(this.writeConns.size());
				for(Connection c : this.writeConns){
					Statement statement  = c.createStatement();
					this.writeSts.add(statement); 
					statement.executeUpdate(query);
					//TODO add logic to check if majority where sucess
					return true;
				}
			}else{
				throw new SQLException("Primary connection  instance is null");
			}
		}else{
			if (retryCount < 3)
				return executeUpdate(retryCount+1,query);
			else
				throw new SQLException("Failed to write");
		}
		return false;
	}

	//excute select calls
	public ResultSet executeResult(String query) throws SQLException{
		if(this.readConn != null){
			Statement st  = readConn.createStatement();
			this.readSt = st;
			return st.executeQuery(query);
		}else{
			throw new SQLException("No read Conn");
		}

	}

	//close statement, connections if any
	//TODO better error handling and defensive coding
	public void close() throws SQLException{
		if(this.mode == MODE.READ){
			this.readSt.close();
			this.readConn.close();
		}else{
			for(int i=0; i<this.writeSts.size(); i++){
				this.writeSts.get(i).close();
				this.writeConns.get(i).close();
			}
		}


	}


}
