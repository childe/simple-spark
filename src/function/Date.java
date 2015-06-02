package function;

import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import utils.date.*;

public class Date implements PairFunction {
	private final static Logger LOGGER = Logger.getLogger(Date.class.getName());

	ArrayList<HashMap<String, Object>> convert;

	static public final String defaultTransformation = "mapToPair";

	@SuppressWarnings("unchecked")
	public Date(HashMap<String, Object> conf) {
		LOGGER.log(Level.FINER, conf.toString());

		this.convert = new ArrayList<HashMap<String, Object>>();
		for (HashMap<String, Object> object : (ArrayList<HashMap<String, Object>>) conf
				.get("convert")) {
			if (!object.containsKey("target")) {
				object.put("target", "@timestamp");
			}

			// prepare SimpleDateFormat arrays

			ArrayList<Parser> parsers = new ArrayList<Parser>();
			
			ArrayList<String> formats = (ArrayList<String>) object
					.get("format");
			for (String format : formats) {
				// if (format.equalsIgnoreCase("UNIX")) {
				// parsers.add(new UnixParser());
				// } else if (format.equalsIgnoreCase("UNIXMS")) {
				// parsers.add(new UnixMSParser());
				// } else {
				// parsers.add(new FormatParser(format,(String)object.get("timezone")));
				// }
			}
			object.put("parsers", parsers);

			this.convert.add(object);
		}

		LOGGER.log(Level.FINER, this.convert.toString());
	}

	@Override
	public Tuple2 call(Object arg0) {
		// TODO Auto-generated method stub
		Tuple2 t = (Tuple2) arg0;
		Object originKey = t._1;
		HashMap<String, Object> event = (HashMap<String, Object>) t._2;

		for (HashMap<String, Object> object : this.convert) {
			String src = (String) object.get("src");
			if (!event.containsKey(src)) {
				continue;
			}

			String stringDate = (String) event.get(src);

			String target = (String) object.get("target");

			boolean success = false;
			@SuppressWarnings("unchecked")
			ArrayList<Parser> parsers = (ArrayList<Parser>) object
					.get("parsers");
			for (Parser parser : parsers) {
				try {
					event.put(target, parser.parse(stringDate));
					success = true;
					break;
				} catch (ParseException e) {
				}
			}

			if (success == false) {
				LOGGER.log(Level.WARNING, "date failed." + event.toString());

				if (!event.containsKey("tags")) {
					event.put("tags",
							new ArrayList<String>(Arrays.asList("datefail")));
				} else {
					Object tags = event.get("tags");
					if (tags.getClass() == ArrayList.class
							&& ((ArrayList) tags).indexOf("datefail") == -1) {
						((ArrayList) tags).add("datefail");
					}
				}
			}
		}

		return new Tuple2(originKey, event);
	}

	public static void main(String[] args) {
		DateTimeFormatter formatter = DateTimeFormat
				.forPattern("YYYY/MM/dd HH:mm:ss.SSS");
		long time = formatter.parseMillis("2015/05/06 10:31:20.527");
		System.out.println(time);
	}
}
