

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Coordinator {


	private String baseurl;
	public Coordinator(String url) {
		this.baseurl = url;
	}

	public Long getNextLogPosition() throws IOException {
		HttpClient client = HttpClientBuilder.create().build();
		HttpGet request = new HttpGet(baseurl+"/getGlobalNextLog");

		// add request header
		HttpResponse response = client.execute(request);

		BufferedReader rd = new BufferedReader(
				new InputStreamReader(response.getEntity().getContent()));

		StringBuilder result = new StringBuilder();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		System.out.println(result);
		Response resp = new Gson().fromJson(result.toString(), Response.class);
		if(resp.status.equals("success")){
			return resp.data.getAsJsonObject().get("value").getAsLong();
		}else{
			throw new RuntimeException("Couldnt fech log position");
		}

	}

	public  String updateLog(Long count, String query) throws IOException {
		HttpClient client = HttpClientBuilder.create().build();
		HttpPost request = new HttpPost(baseurl+"/putLog");
		request.setHeader("Content-Type","application/json");
		LogMessage msg  = new LogMessage();
		msg.id= count;
		msg.query = query;
		request.setEntity(new ByteArrayEntity(new Gson().toJson(msg).getBytes()));
		HttpResponse response = client.execute(request);

		BufferedReader rd = new BufferedReader(
				new InputStreamReader(response.getEntity().getContent()));

		StringBuilder result = new StringBuilder();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		System.out.println(result);
		Response resp = new Gson().fromJson(result.toString(), Response.class);
		if(resp.status.equals("success")){
			return resp.data.getAsJsonObject().get("value").getAsString();
		}else{
			throw new RuntimeException("Failed to udpate log position");
		}
	}

}
