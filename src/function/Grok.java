package function;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import utils.postProcess.PostProcess;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * we could config more than one match in this function;
 * same tagOnFailure applies to the event if one match fails.
 */
public class Grok implements PairFunction {

	private final static Logger LOGGER = Logger.getLogger(Grok.class.getName());

	static public final String defaultTransformation = "mapToPair";

	final private List<Tuple3> matches;

	private final String tagOnFailure;

	private Map conf;

	@SuppressWarnings("unchecked")
	public Grok(HashMap<String, Object> conf) {
		System.out.println(conf);

		this.conf = conf;

		ArrayList<HashMap> originalMatches = (ArrayList<HashMap>) conf
				.get("match");
		this.matches = this.prepareMatchConf(originalMatches);

		if (conf.containsKey("tag_on_failure")) {
			this.tagOnFailure = (String) conf.get("tag_on_failure");
		} else {
			this.tagOnFailure = "grokfail";
		}
	}

	private Set<String> getNamedGroupCandidates(String regex) {
		Set<String> namedGroups = new TreeSet<String>();

		Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(
				regex);

		while (m.find()) {
			namedGroups.add(m.group(1));
		}

		return namedGroups;
	}

	private List prepareMatchConf(ArrayList<HashMap> originalMatches) {
		ArrayList<Tuple3> matches = new ArrayList<Tuple3>();
		for (HashMap matchconf : originalMatches) {
			String src = (String) matchconf.keySet().iterator().next();

			final ArrayList<Tuple2> patternAndGroupnames = new ArrayList<Tuple2>();

			for (String m : (ArrayList<String>) matchconf.get(src)) {
				Pattern p = Pattern.compile(m);
				Set<String> groupnames = this.getNamedGroupCandidates(m);
				patternAndGroupnames.add(new Tuple2(p, groupnames));
			}

			matches.add(new Tuple3(src, patternAndGroupnames, matchconf));
		}
		return matches;
	}

	private void match(HashMap event) {

		for (Tuple3 match : this.matches) {

			String src = (String) match._1();
			if (!event.containsKey(src)) {
				continue;
			}

			boolean success = false;
			try {
				String input = (String) event.get(src);

				ArrayList<Tuple2> pAndGns = (ArrayList<Tuple2>) match._2();
				for (Tuple2 pAndgn : pAndGns) {
					Pattern p = (Pattern) pAndgn._1;

					Matcher m = p.matcher(input);

					if (!m.find()) {
						continue;
					}

					success = true;

					Set<String> groupnames = (Set) pAndgn._2;
					for (String groupname : groupnames) {
						event.put(groupname, (String) m.group(groupname));
					}
					break;
				}

			} catch (Exception e) {
				LOGGER.log(Level.WARNING, e.getLocalizedMessage());
				success = false;
			}

			if (success == false) {

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
				PostProcess.process(event, (Map) match._3());
			}
		}

	}

	public Tuple2 call(Object arg0) {
		// TODO Auto-generated method stub
		Tuple2 t = (Tuple2) arg0;
		Object originKey = t._1;
		HashMap<String, Object> event = (HashMap<String, Object>) t._2;

		this.match(event);

		return new Tuple2(originKey, event);

	}

	public static void main(String[] args) {
		String input = "   1427965391.659     29 10.8.74.48[-] TCP_MISS/500 150 513 GET http://www.weather.com.cn/weather/%E5%8B%83%E5%88%A9.shtml - HIER_DIRECT/180.9    7.161.108 text/html 500 -";
		String pattern = "\\s*(?<logtime>\\S+)\\s+(?<responseTime>\\d+)\\s+\\S+[^/]+/(?<requeststatusCode>\\d+)(\\s+\\S+){3}\\s+(.*://)?(?<domain>[^/:]+)";
		Matcher m = Pattern.compile(pattern).matcher(input);
		m.find();
		System.out.println(m.group("logtime"));
		System.out.println(m.group("domain"));

		input = "     1431920251.572    303 10.8.88.136[-] TCP_MISS/200 166 4386 CONNECT api.weixin.qq.com:443 - HIER_DIRECT/101.226.90.58 -   - -";
		m = Pattern.compile(pattern).matcher(input);
		m.find();
		System.out.println(m.group("logtime"));
		System.out.println(m.group("domain"));
	}
}
