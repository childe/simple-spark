package jinfilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import jinmanager.JinManager;

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;

/*
 * input could be Unix time stamp in MS unit or ISO8601 string
 */
public class DateFormat implements Filter {

	@Override
	public String getName() {
		return "dateformat";
	}

	@Override
	public Object filter(Object arg0, JinjavaInterpreter arg1, String... arg2) {
		long time = System.currentTimeMillis();
		try {
			time = Long.parseLong((String) arg0);
		} catch (Exception e) {
			// adjust to logstash which has @timestamp filed in ISO8601 format
			time = ISODateTimeFormat.dateTimeParser()
					.parseMillis((String) arg0);
		}

		DateTimeFormatter f = DateTimeFormat.forPattern(arg2[0]).withZone(
				org.joda.time.DateTimeZone.UTC);
		Instant instant = new Instant(time);
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
