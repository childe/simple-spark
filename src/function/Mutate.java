package function;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import utils.postProcess.PostProcess;

import com.google.protobuf.WireFormat.FieldType;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.tree.Node;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jinmanager.JinManager;

public class Mutate implements Function {

	static public final String defaultTransformation = "mapToPair";

	private Map conf;

	public Mutate(Map conf) {
		this.conf = conf;
	}

	private void rename(Map<String, Object> event, Map<String, String> fields) {
		Iterator<Entry<String, String>> it = fields.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();

			String oldname = entry.getKey();
			String newname = entry.getValue();

			if (event.containsKey(oldname)) {
				event.put(newname, event.remove(oldname));
			}
		}
	};

	private void update(Map<String, Object> event) {
	};

	private void replace(Map<String, Object> event) {
	};

	private void convert(Map<String, Object> event, Map<String, String> fields) {
		Iterator<Entry<String, String>> it = fields.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();

			String field = entry.getKey();
			String filedtype = entry.getValue();

			Object newvalue = null;
			if (!event.containsKey(field)) {
				return;
			}
			if (filedtype.equalsIgnoreCase("integer")) {
				newvalue = Integer.parseInt((String) event.get(field));
			} else if (filedtype.equalsIgnoreCase("float")) {
				newvalue = Float.parseFloat((String) event.get(field));
			} else if (filedtype.equalsIgnoreCase("string")) {
				newvalue = event.get(field).toString();
			}else{
				newvalue = event.get(field);
			}
			event.put(field, newvalue);
		}
	};

	private void gsub(Map<String, Object> event,
			Map<String, List<String>> fields) {
		Iterator<Entry<String, List<String>>> it = fields.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, List<String>> entry = it.next();

			String field = entry.getKey();
			String regex = entry.getValue().get(0);
			String replacement = entry.getValue().get(1);

			if (event.containsKey(field)) {
				event.put(field, ((String) event.remove(field)).replaceAll(
						regex, replacement));
			}
		}
	};

	private void uppercase(Map<String, Object> event, List<String> fields) {
		for (String field : fields) {
			if (event.containsKey(field)) {
				event.put(field, ((String) event.remove(field)).toUpperCase());
			}
		}
	};

	private void lowercase(Map<String, Object> event, List<String> fields) {
		for (String field : fields) {
			if (event.containsKey(field)) {
				event.put(field, ((String) event.remove(field)).toLowerCase());
			}
		}
	};

	private void strip(Map<String, Object> event, List<String> fields) {

		for (String field : fields) {
			if (event.containsKey(field)) {
				event.put(field, ((String) event.remove(field)).trim());
			}
		}
	};

	private void remove(Map<String, Object> event) {
	};

	private void split(Map<String, Object> event) {
	};

	private void join(Map<String, Object> event) {
	};

	private void merge(Map<String, Object> event) {
	};

	@Override
	public Object call(Object arg0) throws Exception {
		Tuple2 t = (Tuple2) arg0;
		Object originKey = t._1;
		HashMap<String, Object> event = (HashMap<String, Object>) t._2;

		// update(event);
		// replace(event);

		// split(event);
		// join(event);
		// merge(event);

		if (this.conf.containsKey("gsub")) {
			gsub(event, (Map<String, List<String>>) this.conf.get("gsub"));
		}
		if (this.conf.containsKey("rename")) {
			rename(event, (Map<String, String>) this.conf.get("rename"));
		}
		if (this.conf.containsKey("convert")) {
			convert(event, (Map<String, String>) this.conf.get("convert"));
		}
		if (this.conf.containsKey("uppercase")) {
			uppercase(event, (List<String>) this.conf.get("uppercase"));
		}
		if (this.conf.containsKey("lowercase")) {
			lowercase(event, (List<String>) this.conf.get("lowercase"));
		}
		if (this.conf.containsKey("strip")) {
			strip(event, (List<String>) this.conf.get("strip"));
		}

		PostProcess.process(event, conf);

		return new Tuple2(originKey, event);
	}

	public static void main(String[] args) throws Exception {

		Map conf = new HashMap();

		conf.put("lowercase", new ArrayList(Arrays.asList("domain")));
		conf.put("uppercase", new ArrayList(Arrays.asList("url")));
		conf.put("rename", new HashMap() {
			{
				put("r", "refer");
			}
		});
		conf.put("strip", new ArrayList(Arrays.asList("username")));
		conf.put("gsub", new HashMap() {
			{
				put("cookie", new ArrayList(Arrays.asList("password", "X")));
			}
		});
		conf.put("convert", new HashMap() {
			{
				put("timetaken", "xx");
			}
		});

		Mutate mutate = new Mutate(conf);

		Object originKey = null;
		Tuple2 event = new Tuple2(originKey, new HashMap() {
			{
				put("domain", "www.Google.com");
				put("url", "/Service/info.html");
				put("r", "http://www.baidu.com");
				put("timetaken", "10");
				put("username", "  abc ");
				put("cookie", "password=123");
			}
		});

		Tuple2 rst = (Tuple2) mutate.call(event);

		System.out.println(rst);

	}
}
