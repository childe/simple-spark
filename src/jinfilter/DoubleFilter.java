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

public class DoubleFilter implements Filter {

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "double";
	}

	@Override
	public Object filter(Object arg0, JinjavaInterpreter arg1, String... arg2) {
		// TODO Auto-generated method stub
		try {
			return Double.parseDouble((String) arg0);
		} catch (Exception e) {
			return 0.0;
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
				put("logtime", "1432020599.157");

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
		
		template = "{{event[1].logtime|integer/10}}";
		s = JinManager.jinjava.render(template, cc);
		System.out.println(s);
		
		template = "{{event[1].logtime|integer/10  >= ''|nowtime/1000000}}";
		s = JinManager.jinjava.render(template, cc);
		System.out.println(s);

		template = "{{event[1].logtime}}";
		s = JinManager.jinjava.render(template, cc);
		System.out.println(s);

		template = "{{event[1].logtime|float}}";
		s = JinManager.jinjava.render(template, cc);
		System.out.println(s);
		
		template = "{{event[1].logtime|float|int}}";
		s = JinManager.jinjava.render(template, cc);
		System.out.println(s);

	}
}
