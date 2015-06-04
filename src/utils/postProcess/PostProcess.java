package utils.postProcess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;

import scala.Tuple2;

import com.hubspot.jinjava.interpret.Context;

import jinmanager.JinManager;

public class PostProcess {

	private final static Logger LOGGER = Logger.getLogger(PostProcess.class
			.getName());

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void addField(Map event, Map fields) {

		Iterator<Entry<String, String>> it = fields.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();

			String newfields = entry.getKey();
			String newvalue = entry.getValue();

			HashMap binding = new HashMap();
			binding.put("event", event);
			Context cc = new Context(JinManager.c, binding);

			event.put(newfields, JinManager.jinjava.render(newvalue, cc));
		}
	}

	private static void removeField(Map event, List<String> fields) {
		for (String field : fields) {
			if (event.containsKey(field)) {
				event.remove(field);
			}
		}
	}

	public static void process(Map event, Map conf) {
		if (conf.containsKey("add_fields")) {
			addField(event, (Map) conf.get("add_fields"));
		}
		if (conf.containsKey("remove_fields")) {
			removeField(event, (List<String>) conf.get("remove_fields"));
		}
	}
}
