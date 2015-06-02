package jinfilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import jinmanager.JinManager;

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;

public class DateFormat implements Filter {

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "dateformat";
	}

	@Override
	public Object filter(Object arg0, JinjavaInterpreter arg1, String... arg2) {

		DateTimeFormatter f = DateTimeFormat.forPattern(arg2[0]).withZone(
				org.joda.time.DateTimeZone.UTC);
		Instant instant = new Instant(Long.parseLong((String) arg0));
		return instant.toDateTime().toString(f);

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		final Map<String, Object> value = new HashMap<String, Object>() {
			{

				put("name", "jia.liu");
				put("age", 29);
				put("logtime", "1433238542142");

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

		template = "{{event[1][\"logtime\"]|dateformat(\"YYYY.MM.dd\")}}";
		s = JinManager.jinjava.render(template, cc);
		System.out.println(s);
	}
}
