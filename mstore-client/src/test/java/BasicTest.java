import java.io.IOException;
import java.util.ArrayList;

import org.junit.Before;
import org.testng.annotations.Test;



public class BasicTest {

	private MConfig config;

	@Before
	public void setup(){
		try {
			config = new MConfig();
			config.coordinator="http://localhost:9000";
			config.stores= new ArrayList<>();
			config.stores.add("localhost:2181:/hbase-unsecure");
			config.stores.add("localhost:2181:/hbase-unsecure");
			Mstore.init(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

//	@Test
//	public void writeToDBAndRead() {
//		try {
//			Mstore store  = Mstore.newInstance(MODE.WRITE);
//			boolean writeSuccess = false;
//			try{
//				store.executeUpdate("upsert into test values (1,'new data here')");
//				writeSuccess = true;
//			}catch(SQLException e){
//				System.out.println("write fail");
//			}finally{
//				store.close();
//			}
//
//			if(writeSuccess){
//
//				int FIRST = 0;
//
//				store  = Mstore.newInstance(MODE.READ,FIRST);
//				ResultSet rset  = store.executeResult("select * from test");
//				while (rset.next()) {
//					System.out.println(rset.getString("mycolumn"));
//				}
//
//				int SECOND = 0;
//
//				store  = Mstore.newInstance(MODE.READ,SECOND);
//				rset  = store.executeResult("select * from test");
//				while (rset.next()) {
//					System.out.println(rset.getString("mycolumn"));
//				}
//
//				int THIRD = 0;
//
//				store  = Mstore.newInstance(MODE.READ,THIRD);
//				rset  = store.executeResult("select * from test");
//				while (rset.next()) {
//					System.out.println(rset.getString("mycolumn"));
//				}
//			}
//		}catch(IOException i){
//			i.printStackTrace();
//		}catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
	
	@Test
	public void httpTest() throws IOException{
		try{
			Coordinator coordinator = new Coordinator("http://localhost:9001");
			Long pos = coordinator.getNextLogPosition();
			System.out.println(pos);
			System.out.println("here");
			String query = coordinator.updateLog(1L, "select * from test");
			System.out.println(query);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
