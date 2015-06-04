package utils.date;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ParserManager {
	private static Map<String, List<Parser>> prepo = null;

	private final static Logger LOGGER = Logger.getLogger(ParserManager.class
			.getName());

	public static List<Parser> getParsers(String instanceID, Map conf) {
		if (prepo == null) {
			prepo = new HashMap<String, List<Parser>>();
		}

		if (prepo.containsKey(instanceID)) {
			return prepo.get(instanceID);
		}

		synchronized (ParserManager.class) {
			ArrayList<Parser> parsers = new ArrayList<Parser>();

			ArrayList<String> formats = (ArrayList<String>) conf.get("format");
			for (String format : formats) {
				if (format.equalsIgnoreCase("UNIX")) {
					parsers.add(new UnixParser());
				} else if (format.equalsIgnoreCase("UNIXMS")) {
					parsers.add(new UnixMSParser());
				} else {
					parsers.add(new FormatParser(format, (String) conf
							.get("timezone"), (String) conf.get("locale")));
				}
			}
			prepo.put(instanceID, parsers);
		}

		return prepo.get(instanceID);
	}
}
