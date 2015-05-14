package function;

import org.apache.spark.api.java.function.Function;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;
import jinmanager.JinManager;

public class Filter2 implements Function {
	ArrayList<HashMap<String, Object>> convert;

	static public final String defaultTransformation = "filter";

	private final ArrayList<String> IF;

	@SuppressWarnings("unchecked")
	public Filter2(HashMap<String, Object> conf) {
		System.out.println(conf);

		this.IF = (ArrayList<String>) conf.get("if");
	}

	@Override
	public Object call(Object arg0) {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		final Tuple2 event = (Tuple2) arg0;

		HashMap binding = new HashMap() {
			{
				put("event", new Object[] { event._1, event._2 });
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

		final Map<String, Object> value = new HashMap<String, Object>() {
			{

				put("name", "jia.liu");
				put("age", 29);
			}
		};
		
		final ArrayList<String> key = new ArrayList<String>() {
			{

				add("name");
				add("3");
			}
		};
		HashMap binding = new HashMap() {
			{
				put("event", new ArrayList(Arrays.asList(key,value)));
			}
		};

		
		String template = "{{event[0][1]|int*1000000000000> ''|nowtime/1000}}";
		
		Context cc = new Context(JinManager.c, binding);

		String s = jinjava.render(template, cc);
		System.out.println(s);

	}
}
