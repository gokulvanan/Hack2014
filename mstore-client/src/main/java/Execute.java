import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;



public class Execute {

	private static MConfig config;
	public static void main(String[] args){
		try{
			setup();
			writeToDBAndRead();
		}catch(IOException e){
			System.out.println("coordinator call failed");
		}catch (SQLException e) {
			System.out.println("phoneix execution failed");
		}

	}

	public  static void setup(){
		try {
//			config = new ObjectMapper().readValue(new File("src/main/resources/config.json"), MConfig.class);
			Mstore.init(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void writeToDBAndRead() throws SQLException, IOException{
		Mstore store  = Mstore.newInstance(MODE.WRITE);
		boolean writeSuccess = false;
		try{
			store.executeUpdate("upsert into test values (1,'new data here')");
			writeSuccess = true;
		}catch(SQLException e){
			System.out.println("write fail");
		}finally{
			store.close();
		}

		if(writeSuccess){

			int FIRST = 0;

			store  = Mstore.newInstance(MODE.READ,FIRST);
			ResultSet rset  = store.executeResult("select * from test");
			while (rset.next()) {
				System.out.println(rset.getString("mycolumn"));
			}

			int SECOND = 0;

			store  = Mstore.newInstance(MODE.READ,SECOND);
			rset  = store.executeResult("select * from test");
			while (rset.next()) {
				System.out.println(rset.getString("mycolumn"));
			}

			int THIRD = 0;

			store  = Mstore.newInstance(MODE.READ,THIRD);
			rset  = store.executeResult("select * from test");
			while (rset.next()) {
				System.out.println(rset.getString("mycolumn"));
			}
		}

	}
}
