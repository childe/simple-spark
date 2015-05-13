package function;

import org.apache.spark.api.java.function.Function;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.PUT;

import jinmanager.JinManager;

public class Filter implements Function {
	ArrayList<HashMap<String, Object>> convert;

	static public final String defaultTransformation = "filter";

	private ArrayList<String> IF;

	public Filter() {
	}

	@SuppressWarnings("unchecked")
	public Filter(HashMap<String, Object> conf) {
		System.out.println(conf);

		this.IF = (ArrayList<String>) conf.get("if");
	}

	@Override
	public Object call(Object arg0) {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		final HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		if (event == null) {
			return false;
		}

		HashMap binding = new HashMap() {
			{
				put("event", event);
			}
		};

		Context cc = new Context(JinManager.c, binding);

		for (String object : this.IF) {
			if (!JinManager.jinjava.render(object, cc).equals("true")) {
				return false;
			}
		}

		return true;
	}

	public static void main(String[] args) {
		Jinjava jinjava = new Jinjava();

		Map<String, Object> context = new HashMap<String, Object>() {
			{

				put("name", "jia.liu");
				put("age", 29);
			}
		};
		String template = "{{age>20}}";

		String s = jinjava.render(template, context);
		System.out.println(s);

	}
}
