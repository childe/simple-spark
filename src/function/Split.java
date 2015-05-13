package function;

import org.apache.spark.api.java.function.Function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Split implements Function {
	private String src;
	private String delimiter;
	HashMap<String, Integer> fields;

	static public final String defaultTransformation = "map";

	public Split() {
	}

	@SuppressWarnings("unchecked")
	public Split(HashMap<String, Object> conf) {
		System.out.println(conf);

		if (!conf.containsKey("src")) {
			this.src = "message";
		} else {
			this.src = (String) conf.get("src");
		}

		if (!conf.containsKey("delimiter")) {
			this.delimiter = "\\s";
		} else {
			this.delimiter = (String) conf.get("delimiter");
		}

		this.fields = (HashMap<String, Integer>) conf.get("fields");

	}

	@Override
	public Object call(Object arg0) {
		// TODO Auto-generated method stub
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		if (event == null) {
			return event;
		}

		if (!event.containsKey(this.src)) {
			return event;
		}

		String[] splited = ((String) event.get(src)).split(delimiter);

		Iterator<Entry<String, Integer>> entries = this.fields.entrySet()
				.iterator();
		try {
			while (entries.hasNext()) {

				Map.Entry<String, Integer> entry = entries.next();

				String key = (String) entry.getKey();

				Integer value = (Integer) entry.getValue();

				event.put(key, splited[value]);

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return event;
		}

		return event;

	}

	public static void main(String[] args) {
		String[] a = "True|1".split("\\|");
		System.out.println(a[0]);

		String message = "2015-05-07T11:15:58.326	|604.001|3202";
		a = message.split("[\t|]");
		System.out.println(a.length);

		message = " 1427965391.659     29 10.8.74.48[-] TCP_MISS/500 150 513 GET http://www.weather.com.cn/weather/%E5%8B%83%E5%88%A9.shtml - HIER_DIRE    CT/180.97.161.108 text/html 500 -";
		String delimiter = "\\s+|/";
		System.out.println(delimiter.length());
		a = message.split(delimiter);
		for (int i = 0; i < a.length; i++) {
			System.out.println(i + ": " + a[i]);
		}
	}
}
