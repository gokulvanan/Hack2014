

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class Coordinator {

	private static final ObjectMapper parser = new ObjectMapper();

	private String baseurl;
	public Coordinator(String url) {
		this.baseurl = url;
	}

	public Long getNextLogPosition() throws IOException {
		HttpClient client = HttpClientBuilder.create().build();
		HttpGet request = new HttpGet(baseurl+"/getNextLogPosition");

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
		Response resp = parser.readValue(result.toString(), Response.class);
		if(resp.status == "success"){
			return resp.data.asLong();
		}else{
			throw new RuntimeException("Couldnt fech log position");
		}

	}

	public  String updateLog(Long count, String query) throws IOException {
		HttpClient client = HttpClientBuilder.create().build();
		HttpPost request = new HttpPost(baseurl+"/updateLogPosition");
		request.setHeader("Content-Type","application/json");
		ObjectNode node = new ObjectNode(parser.getNodeFactory());
		node.put("id", count);
		node.put("query", query);
		request.setEntity(new ByteArrayEntity(node.toString().getBytes()));
		HttpResponse response = client.execute(request);

		BufferedReader rd = new BufferedReader(
				new InputStreamReader(response.getEntity().getContent()));

		StringBuilder result = new StringBuilder();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		System.out.println(result);
		Response resp = parser.readValue(result.toString(), Response.class);
		if(resp.status == "success"){
			return resp.data.asText();
		}else{
			throw new RuntimeException("Couldnt fech log position");
		}
	}

}
