package function;

import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import utils.date.*;
import utils.postProcess.PostProcess;

public class Date implements PairFunction {
	private final static Logger LOGGER = Logger.getLogger(Date.class.getName());

	ArrayList<HashMap<String, Object>> convert;

	static public final String defaultTransformation = "mapToPair";

	private final String tagOnFailure;

	private Map conf;

	@SuppressWarnings("unchecked")
	public Date(HashMap<String, Object> conf) {
		LOGGER.log(Level.FINER, conf.toString());

		this.conf = conf;

		if (conf.containsKey("tag_on_failure")) {
			this.tagOnFailure = (String) conf.get("tag_on_failure");
		} else {
			this.tagOnFailure = "datefail";
		}

		this.convert = new ArrayList<HashMap<String, Object>>();
		for (HashMap<String, Object> object : (ArrayList<HashMap<String, Object>>) conf
				.get("convert")) {

			if (!object.containsKey("target")) {
				object.put("target", "@timestamp");
			}
			if (!object.containsKey("locale")) {
				object.put("locale", "en");
			}

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

			if (object.containsKey("default_year")
					&& (Boolean) object.get("default_year") == true) {
				stringDate = Calendar.getInstance().get(Calendar.YEAR) + stringDate;
			}

			String target = (String) object.get("target");

			boolean success = false;
			@SuppressWarnings("unchecked")
			// ArrayList<Parser> parsers = (ArrayList<Parser>) object
			// .get("parsers");
			List<Parser> parsers = ParserManager.getParsers(
					String.valueOf(object.hashCode()), object);
			for (Parser parser : parsers) {
				try {
					event.put(target, parser.parse(stringDate));
					success = true;
					break;
				} catch (Exception e) {
				}
			}

			if (success == false) {
				LOGGER.log(Level.FINE, "date failed." + event.toString());

				if (!event.containsKey("tags")) {
					event.put(
							"tags",
							new ArrayList<String>(Arrays
									.asList(this.tagOnFailure)));
				} else {
					Object tags = event.get("tags");
					if (tags.getClass() == ArrayList.class
							&& ((ArrayList) tags).indexOf(this.tagOnFailure) == -1) {
						((ArrayList) tags).add(this.tagOnFailure);
					}
				}
			} else {
				PostProcess.process(event, object);
			}

		}

		return new Tuple2(originKey, event);
	}

	public static void main(String[] args) {

		
		System.out.println(2015+"1212");
	}
}
