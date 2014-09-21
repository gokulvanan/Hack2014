package controllers;

import play.*;
import play.libs.WS;
import play.libs.WS.HttpResponse;
import play.mvc.*;

import java.util.*;

import com.sun.xml.internal.ws.server.ServerSchemaValidationTube;

import models.*;

public class Application extends Controller {

    public static void getNextLog() {
    	String[] urls = Play.configuration.getProperty("ws.hosts").split(",");
    	
    	
    	for(String peers : urls){
    		 HttpResponse res = WS.url(peers+"/getNexLog").get();
    	}
    	
        render();
    }
    
    
    public static void updateLog(){
    	render();
    }

}