package phoenix;

import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.net.URI;

public class Coordinator {
	private String m_myIp;
	private String[] m_peers;
	private int m_myLogPos;
	private int m_currentLogPos;
	private Connection m_dbCon;
	//private Statement logStmt;
	private String m_getCurPos="SELECT max(LOGNUMBER) FROM LOG";

	public Coordinator(String ip1,String ip2) throws ClassNotFoundException,SQLException
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

		}catch (Exception ex)
		{
			System.out.println("Exception"+ex.getMessage());
			throw ex;
		}
	}

	/**
	 * Return my current log position. Called by other coordinators
	 */
	public int GetMyCurrentLogPos()
	{
		int logNum=-1;
		try
		{
			Statement logStmt=m_dbCon.createStatement();
			ResultSet rs=logStmt.executeQuery(m_getCurPos);
			while(rs.next())
			{
				logNum=rs.getInt(1);
			}
		}catch(Exception ex)
		{
			System.out.println("Exception"+ex.getMessage());
		}
		return logNum;
	}

	/**
 	 *Return the sqlstring given the log position.  This should be exposed as a webservice and
	 *called by other coordinator
 	 */
	public String GetMyLogString(int pos)
	{
		String retVal="null";
		String query="select sqlstr from log where lognumber="+pos;
		try
		{
			Statement logStmt=m_dbCon.createStatement();
			ResultSet rs=logStmt.executeQuery(query);
			while(rs.next())
			{
				retVal=rs.getString(1);
			}
		}catch(Exception ex)
		{
			System.out.println("Exception"+ex.getMessage());
		}
		return retVal;
	}



	/**
	 * Return the next valid log position. This may involve communication with other
	 * peers. This should be exposed as a webservice and called by other coordinator
	 */
	public int GetMyNextLogPos()
	{
		int retVal=-1;
		int nextLogNum=GetMyCurrentLogPos()+1;
		String ins="upsert into log(lognumber,sqlstr,status) values("+nextLogNum+",'null','waiting')";
		System.out.println(ins);
		try
		{
			Statement logStmt=m_dbCon.createStatement();
			int n=logStmt.executeUpdate(ins);
			m_dbCon.commit();
			retVal=GetMyCurrentLogPos();
		}catch(Exception ex)
		{
			System.out.println("Exception"+ex.getMessage());
		}
		return retVal;
	}

	/**
     *Invoked by client to validate the log position.
     */
	public boolean validateMyLogPos(int logPos)
	{
		int curLogPos=GetMyCurrentLogPos();
		int n=-1;
		if(curLogPos<logPos)
		{
			return false;
		}
		String ins="upsert into log(lognumber,status) values("+logPos+",'validated')";
		try
		{
			Statement logStmt=m_dbCon.createStatement();
			n=logStmt.executeUpdate(ins);
			m_dbCon.commit();
		}catch(Exception ex)
		{
			System.out.println("Exception"+ex.getMessage());
		}
		if(n>0)
		{
			return true;
		}
		return false;
	}

	/**
     * Called by client. This will invoke GetMyNextLogPos() on other coordinators.
     */
	public  int GetGlobalNextLogPos()
    {
		return -1;
    }

	/**
     * Called by client. This will invoke GetMyCurrentLogPos() on other coordinators.
     */
	public  int GetGlobalHighestLogPos()
    {
		return -1;
    }



	public static void main(String[] args)
    {
		try
		{
			Coordinator coord=new Coordinator("ip1","ip2");
			int curLogPos=coord.GetMyCurrentLogPos();
			System.out.println("Current Log Pos="+curLogPos);
			int nextLogPos=coord.GetMyNextLogPos();
			System.out.println("Next Log Pos="+nextLogPos);
			System.out.println("Current Log Pos after next="+coord.GetMyCurrentLogPos());
			System.out.println("Str at pos 1="+coord.GetMyLogString(1));
		}catch(Exception ex)
		{
			System.out.println("Exception"+ex.getMessage());

		}
	}
}