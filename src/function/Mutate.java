package function;

import org.apache.spark.api.java.function.Function;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.tree.Node;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Mutate implements Function {

	static public final String defaultTransformation = "map";

	private void rename(HashMap<String, Object> event) {
	};

	private void update(HashMap<String, Object> event) {
	};

	private void replace(HashMap<String, Object> event) {
	};

	private void convert(HashMap<String, Object> event) {
	};

	private void gsub(HashMap<String, Object> event) {
	};

	private void uppercase(HashMap<String, Object> event) {
	};

	private void lowercase(HashMap<String, Object> event) {
	};

	private void strip(HashMap<String, Object> event) {
	};

	private void remove(HashMap<String, Object> event) {
	};

	private void split(HashMap<String, Object> event) {
	};

	private void join(HashMap<String, Object> event) {
	};

	private void merge(HashMap<String, Object> event) {
	};

	@Override
	public Object call(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		rename(event);
		update(event);
		replace(event);
		convert(event);
		gsub(event);
		uppercase(event);
		lowercase(event);
		strip(event);
		remove(event);
		split(event);
		join(event);
		merge(event);

		return event;
	}

	public void testFunc1(String a, int b) {
		for (int i = 0; i < b; i++) {
			System.out.println(a);
		}
	}

	public void testFunc2() {
		System.out.println("testFunc2");
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
