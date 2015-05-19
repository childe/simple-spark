package jinfilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import jinmanager.JinManager;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;
import com.hubspot.jinjava.parse.TokenParser;
import com.hubspot.jinjava.tree.Node;
import com.hubspot.jinjava.tree.TreeParser;

public class IntegerFilter implements Filter {

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "integer";
	}

	@Override
	public Object filter(Object arg0, JinjavaInterpreter arg1, String... arg2) {
		// TODO Auto-generated method stub
		try {
			return Integer.parseInt((String) arg0);
		} catch (Exception e) {
			try {
				return (int)Double.parseDouble((String) arg0);
			} catch (Exception e2) {
				return 0;
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		final Map<String, Object> value = new HashMap<String, Object>() {
			{

				put("name", "jia.liu");
				put("age", 29);
				put("logtime", "1432020638.958");

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
				put("event", new ArrayList(Arrays.asList(key, value)));
			}
		};

		Context cc = new Context(JinManager.c, binding);
		String template, s;


		long st = System.currentTimeMillis();

		for (int i = 0; i < 10000; i++) {

			template = "{{event[1].logtime|double|int}}";
			s = JinManager.jinjava.render(template, cc);
			// System.out.println(s);
		}
		System.out.println(System.currentTimeMillis() - st);
		
		st = System.currentTimeMillis();

		for (int i = 0; i < 10000; i++) {

			template = "{{event[1].logtime|integer}}";
			s = JinManager.jinjava.render(template, cc);
			// System.out.println(s);
		}
		System.out.println(System.currentTimeMillis() - st);
	}
}
