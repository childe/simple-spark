package transformation;

import org.apache.spark.api.java.function.Function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Split implements Function {
	private HashMap<String, Object> conf;

	public Split() {
		this.conf = null;
	}

	public Split(HashMap<String, Object> conf) {
		this.conf = conf;
	}

	@Override
	public Object call(Object arg0) {
		// TODO Auto-generated method stub
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		System.out.println(this.conf);
		if (event == null) {
			return event;
		}

		String src = (String) this.conf.get("src");

		if (!event.containsKey(src)) {
			return event;
		}

		String delimiter = (String) this.conf.get("delimiter");

		String[] splited = ((String) event.get(src)).split(delimiter);
		System.out.println(splited);

		@SuppressWarnings("unchecked")
		HashMap<String, Integer> fields = (HashMap<String, Integer>) this.conf
				.get("fields");
		Iterator<Entry<String, Integer>> entries = fields.entrySet().iterator();
		try {
			while (entries.hasNext()) {

				Map.Entry<String, Integer> entry = entries.next();

				String key = (String) entry.getKey();

				Integer value = (Integer) entry.getValue();

				event.put(key, splited[value]);

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~");
			e.printStackTrace();
			return event;
		}

		event.remove("message");
		return event;

	}

	public static void main(String[] args) {
		String[] a = "True|1".split("\\|");
		System.out.println(a[0]);

	}
}
