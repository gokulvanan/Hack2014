package phoenix;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Replicator {
	private String m_myIp;
	private String[] m_peers;
	private int m_myLogPos;
	private int m_currentLogPos;
	private Connection m_dbCon;
	private ExecutorService m_LogApplierService;
	//private Statement logStmt;
	private String m_getCurPos="SELECT max(LOGNUMBER) FROM LOG";

	public Replicator(String ip1,String ip2) throws ClassNotFoundException,SQLException
	{
		m_peers=new String[2];
		m_peers[0]=ip1;
		m_peers[1]=ip1;
      	try{
        	Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			m_dbCon =  DriverManager.getConnection("jdbc:phoenix:localhost:2181:/hbase-unsecure");
			//logStmt=m_dbCon.createStatement();
        	System.out.println("Got Connection!");
        	System.out.println(m_dbCon);
			m_LogApplierService= Executors.newSingleThreadExecutor();
			m_LogApplierService.execute(new Runnable() {
    			public void run() 
				{
					//Find out last resolved log number and query the next one after that
					//The lifecycle of stats string is 'invalid', Not applicable for mutation
					//'nodata':Waiting for sqlstring,
					//'pendingvalidation': Pendingfor validation
					//'validated', validated
					//'applied', mutations applied
					
					while(true)
					{
					try {
						String lastApplied="select max(lognumber) from log where sqlstring!='null' and status='applied'";
						Statement logStmt=m_dbCon.createStatement();
						ResultSet rs=logStmt.executeQuery(lastApplied);
						int nextLogPos=-1;
						System.out.println("Finding max() applied position");
						while(rs.next())
						{
							nextLogPos=rs.getInt(1);
						}
						System.out.println("max applied position is "+nextLogPos);
					
						if(nextLogPos>0)
						{
							String pending="select lognumber,sqlstring,status from log where lognumber>"+nextLogPos+" order by lognumber";
							Statement pendingStmt=m_dbCon.createStatement();
							System.out.println("Querying log for unapplied logs");
							ResultSet rs1=pendingStmt.executeQuery(pending);
							while(rs1.next())
							{
								int logNum=rs1.getInt("lognumber");
								//Apply mutations only if the sequence of logs have been validated
								if(rs1.getString("status").compareToIgnoreCase("validated")==0)
								{
									Statement mut=m_dbCon.createStatement();
									String logSqlStr=rs1.getString("sqlstring");	
									System.out.println("Applying mutation "+logNum+":"+logSqlStr);
									int n=mut.executeUpdate(logSqlStr);
									if(n>0)
									{
										System.out.println("Sucessfully applied mutation "+logNum+":"+logSqlStr);
										Statement pst=m_dbCon.createStatement();
										System.out.println("updating log status");
										int up=pst.executeUpdate("upsert log (status) values('validated') where lognumber="+logNum);
										m_dbCon.commit();
									}
								}else
								{
									break;
								}
							}
						}
					}catch (Exception ex)
					{
						System.out.println("Exception"+ex.getMessage());
					}
					}
    			}
			});
		}catch (Exception ex)
		{
			System.out.println("Exception"+ex.getMessage());
			throw new RuntimeException(ex);
		}
	}

	public static void main(String[] args)
    {
	}
}

