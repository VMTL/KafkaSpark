package app.helpers;

import java.util.HashMap;
import java.util.Map;

public class NewsClientParams {

	public final String baseUri = "https://newsapi.org/v2/";
	public final String pathEverything = "everything";
	public final String topHeadlines = "top-headlines";
	public Map<String, String> newsParams;
	
	public NewsClientParams() {
		this.newsParams = new HashMap<String,String>();
		//newsParams.put("sortBy", "popularity");
		newsParams.put("from", "2019-12-04");
		newsParams.put("country", "us");
		newsParams.put("apiKey", "32506696a2f54bf1b26c6e124e311a05");
	}
}