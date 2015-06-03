package function;

import org.apache.spark.api.java.function.Function;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.tree.Node;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jinmanager.JinManager;

public class Mutate implements Function {

	static public final String defaultTransformation = "map";

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

	private void number(Map<String, Object> event, Map<String, String> fields) {
		Iterator<Entry<String, String>> it = fields.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();

			String field = entry.getKey();
			String valuetype = entry.getValue();

			if (event.containsKey(field)) {
				event.put(field,
						Double.parseDouble((String) event.remove(field)));
			}
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

	private void strip(Map<String, Object> event, Map<String, String> fields) {
		Iterator<Entry<String, String>> it = fields.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();

			String field = entry.getKey();
			String regex = entry.getValue();

			if (event.containsKey(field)) {
				event.put(field,
						((String) event.remove(field)).replaceAll(regex, ""));
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
		// TODO Auto-generated method stub
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		// update(event);
		// replace(event);

		// split(event);
		// join(event);
		// merge(event);
		if (this.conf.containsKey("rename")) {
			gsub(event, (Map<String, List<String>>) this.conf.get("rename"));
		}
		if (this.conf.containsKey("rename")) {
			rename(event, (Map<String, String>) this.conf.get("rename"));
		}
		if (this.conf.containsKey("number")) {
			number(event, (Map<String, String>) this.conf.get("number"));
		}
		if (this.conf.containsKey("uppercase")) {
			uppercase(event, (List<String>) this.conf.get("uppercase"));
		}
		if (this.conf.containsKey("lowercase")) {
			lowercase(event, (List<String>) this.conf.get("lowercase"));
		}
		if (this.conf.containsKey("strip")) {
			strip(event, (Map<String, String>) this.conf.get("number"));
		}

		return event;
	}

	public static void main(String[] args) {

		// test getField

		String a = null;
		try {
			a = (String) Class.forName("function.Mutate")
					.getField("defaultTransformation").get(null);
		} catch (IllegalArgumentException | IllegalAccessException
				| NoSuchFieldException | SecurityException
				| ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println(a);

		// test getMethod with uncertain parameters

		ArrayList<Class> parameters = new ArrayList<Class>();
		Class[] p = {};
		parameters.add(String.class);
		parameters.add(int.class);
		p = parameters.toArray(p);
		System.out.println(p);

		Class[] p2 = new Class[] { String.class, int.class };
		System.out.println(p2);

		try {
			Method m = Class.forName("function.Mutate").getMethod("testFunc1",
					p);

			m.invoke(Class.forName("function.Mutate").newInstance(), "ab", 10);

		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | InstantiationException
				| NoSuchMethodException | SecurityException
				| ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// test getMethod with void parameter

		try {
			p = new Class[] {};
			Method m = Class.forName("function.Mutate").getMethod("testFunc2",
					p);

			m.invoke(Class.forName("function.Mutate").newInstance());

		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | InstantiationException
				| NoSuchMethodException | SecurityException
				| ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		long s = System.currentTimeMillis();

		Jinjava jinjava = new Jinjava();

		Map<String, Object> context = new HashMap<>();
		context.put("name", "Jared");
		String template = "Hello, {% if name is defined %} {{name}} {% else %} world {% endif %}";

		for (int i = 0; i < 1; i++) {

			jinjava.renderForResult(template, context);

		}
		System.out.println(System.currentTimeMillis() - s);

		template = "{{event[\"@timestamp\"]}}";
		context = new HashMap<>();
		HashMap event = new HashMap<>();
		event.put("@timestamp", 100000000);
		context.put("event", event);
		String rst = jinjava.render(template, context);
		System.out.println(rst);

	}
}
