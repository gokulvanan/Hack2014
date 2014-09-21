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
import java.util.*;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sun.media.jai.iterator.WrapperWRI;
import com.sun.xml.internal.ws.server.ServerSchemaValidationTube;

import models.*;

public class Application extends Controller {

	public static void getGlobalNextLog() {
		Long maxCout = 1L;//Coordinator.getNextLogCount();
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

	public static void getGlobalHighestLog() {
		Long maxCout = 1L;//Coordinator.getNextLogCount();
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

	public static void getNextLog(){
		Response response = new Response();
		response.status="success";
		response.data=new Gson().toJsonTree(1L);
		renderJSON(response);
	}
	
	public static void getHighestLog(){
		Response response = new Response();
		response.status="success";
		response.data=new Gson().toJsonTree(1L);
		renderJSON(response);
	}
	
	public static void putLog(){
		Long id  = Long.parseLong(params.get("id"));
		String query = params.get("query");
		LogMessage logMsg =  new LogMessage();
		logMsg.id = id;
		logMsg.query = query;
		// Coordinator calls put log in all if majroity retrun success 
		//else changes
		Response response = new Response();
		response.status="success";
		response.data=(JsonObject) new Gson().toJsonTree(query);
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