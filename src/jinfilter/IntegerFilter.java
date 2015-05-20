package jinfilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import jinmanager.JinManager;

import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;

public class IntegerFilter implements Filter {

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "integer";
	}

	@Override
	public Object filter(Object arg0, JinjavaInterpreter arg1, String... arg2) {
		// TODO Auto-generated method stub
		if (arg0 == null) {
			return 0;
		}
		Class c = arg0.getClass();
		if (c == String.class) {
			try {
				return Long.parseLong((String) arg0);
			} catch (Exception e) {
				try {
					return (long) Double.parseDouble((String) arg0);
				} catch (Exception e2) {
					return 0;
				}
			}
		} else if (c == Double.class) {
			return ((Double) arg0).longValue();
		} else if (c == Float.class) {
			return ((Float) arg0).longValue();

		} else {
			try {
				return (long) arg0;
			} catch (Exception e) {
				return 0;
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Float d = 1.1f;
		long a = d.longValue();
		final Map<String, Object> value = new HashMap<String, Object>() {
			{

				put("name", "jia.liu");
				put("age", 29);
				put("logtime", "1432020638.958");
				put("longtime", 1432144740100.0d);

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

		template = "{{event[1].miss|integer>1000}}";
		s = JinManager.jinjava.render(template, cc);
		System.out.println(s);
		//
		// template = "{{(event[1].longtime|integer/1000)|integer}}";
		// s = JinManager.jinjava.render(template, cc);
		// System.out.println(s);
		//
		// long st = System.currentTimeMillis();
		//
		// for (int i = 0; i < 10000; i++) {
		//
		// template = "{{event[1].logtime|double|int}}";
		// s = JinManager.jinjava.render(template, cc);
		// // System.out.println(s);
		// }
		// System.out.println(System.currentTimeMillis() - st);
		//
		// st = System.currentTimeMillis();
		//
		// for (int i = 0; i < 10000; i++) {
		//
		// template = "{{event[1].logtime|integer}}";
		// s = JinManager.jinjava.render(template, cc);
		// // System.out.println(s);
		// }
		// System.out.println(System.currentTimeMillis() - st);
	}
}
