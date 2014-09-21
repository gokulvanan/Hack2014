package controllers;

import phoenix.Coordinator;
import play.*;
import play.libs.WS;
import play.libs.WS.HttpResponse;
import play.libs.WS.WSRequest;
import play.mvc.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sun.org.apache.bcel.internal.generic.NEW;

import models.*;

public class Application extends Controller {

	
	public static void getGlobalNextLog() throws ClassNotFoundException, SQLException {
		Long maxCout = (long) new Coordinator().GetMyNextLogPos();
		Long maxNew = maxCout;
		for(String peers : getHosts()){
			System.out.println(peers);
			if(peers == null) continue;
			Long count = 0L;
			try{
				HttpResponse httRes = WS.url(peers+"/getNextLog").get();
				Response resp =  new Gson().fromJson(httRes.getJson(),Response.class);
				count =  resp.data.getAsLong();
			}catch(Exception e){
				continue; //ingore
			}
			if(count > maxNew){
				maxNew = count;
			}

		}
		Response response = new Response();
		response.status="success";
		response.data=new Gson().toJsonTree(maxNew);
		renderJSON(response);
	}

	public static void getGlobalHighestLog() throws ClassNotFoundException, SQLException {
		Long maxCout = (long) new Coordinator().GetMyCurrentLogPos();
		Long maxNew = maxCout;
		for(String peers : getHosts()){
			System.out.println(peers);
			if(peers == null) continue;
			Long count = 0L;
			try{
				HttpResponse httRes = WS.url(peers+"/getHighestLog").get();
				Response resp =  new Gson().fromJson(httRes.getJson(),Response.class);
				count =  resp.data.getAsLong();
			}catch(Exception e){
				continue; //ingore
			}
			if(count > maxNew){
				maxNew = count;
			}

		}
		Response response = new Response();
		response.status="success";
		response.data=new Gson().toJsonTree(maxNew);
		renderJSON(response);
	}

	public static void getNextLog() throws ClassNotFoundException, SQLException{
		Response response = new Response();
		response.status="success";
		response.data=new Gson().toJsonTree(new Coordinator().GetMyNextLogPos());
		renderJSON(response);
	}

	public static void getHighestLog() throws ClassNotFoundException, SQLException{
		Response response = new Response();
		response.status="success";
		response.data=new Gson().toJsonTree(new Coordinator().GetMyCurrentLogPos());
		renderJSON(response);
	}

	public static void putAllLog() throws IOException, ClassNotFoundException, SQLException{
		List<String> lines =  IOUtils.readLines(request.body);
		LogMessage logMsg  = new Gson().fromJson(lines.get(0),LogMessage.class);
		String query = logMsg.query;
		Long count = 0L;
		boolean status = new Coordinator().putInMyLogPos(logMsg.id.intValue(), logMsg.query);
		if(status){

			for(String peers : getHosts()){
				System.out.println(peers);
				if(peers == null) continue;
				try{
					WSRequest req = WS.url(peers+"/putLog").body(new Gson().toJson(logMsg));
					req.setHeader("Content-Type", "application/json");
					HttpResponse httRes = req.post();
					Response resp =  new Gson().fromJson(httRes.getJson(),Response.class);
					boolean buffQuery =  resp.data.getAsJsonObject().get("value").getAsBoolean();
					if(buffQuery) count++;
				}catch(Exception e){
					continue; //ignore
				}
			}
		}
		Response response = new Response();
		if(count > getHosts().size()/2){
			response.status="success";
			response.data= new Gson().toJsonTree(query);
		}else{
			response.status="failure";
			response.data= new Gson().toJsonTree("");
		}
		renderJSON(response);
	}

	public static void putLog() throws IOException, ClassNotFoundException, SQLException{
		List<String> lines =  IOUtils.readLines(request.body);
		LogMessage logMsg  = new Gson().fromJson(lines.get(0),LogMessage.class);
		boolean status = new Coordinator().putInMyLogPos(logMsg.id.intValue(), logMsg.query);
		Response response = new Response();
		response.status="success";
		response.data= new Gson().toJsonTree(status);
		renderJSON(response);
	}
	
	
	public static void validateAllLog() throws IOException, ClassNotFoundException, SQLException{
		List<String> lines =  IOUtils.readLines(request.body);
		LogMessage logMsg  = new Gson().fromJson(lines.get(0),LogMessage.class);
		boolean status = new Coordinator().validateMyLogPos(logMsg.id.intValue());
		int count = 0;
		if(status){
			for(String peers : getHosts()){
				if(peers == null) continue;
				try{
					WSRequest req = WS.url(peers+"/validateLog").body(new Gson().toJson(logMsg));
					req.setHeader("Content-Type", "application/json");
					HttpResponse httRes = req.post();
					Response resp =  new Gson().fromJson(httRes.getJson(),Response.class);
					boolean buffQuery =  resp.data.getAsJsonObject().get("value").getAsBoolean();
					if(buffQuery) count++;
				}catch(Exception e){
					continue; //ignore
				}
			}
		}
		Response response = new Response();
		if(count == getHosts().size()){
			response.status="success";
			response.data= new Gson().toJsonTree(true);
		}else{
			response.status="failure";
			response.data= new Gson().toJsonTree(false);
		}
		renderJSON(response);
	}

	public static void validateLog() throws IOException, ClassNotFoundException, SQLException{
		List<String> lines =  IOUtils.readLines(request.body);
		LogMessage logMsg  = new Gson().fromJson(lines.get(0),LogMessage.class);
		boolean status = new Coordinator().validateMyLogPos(logMsg.id.intValue());
		Response response = new Response();
		response.status="success";
		response.data= new Gson().toJsonTree(status);
		renderJSON(response);
	}
	
	
	
	private static List<String> getHosts(){
		try{
			File hosts = new File("conf/peerhost");
			List<String> lines = IOUtils.readLines(new FileReader(hosts));
			return lines;
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}

}