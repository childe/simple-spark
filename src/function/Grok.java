package function;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Grok implements PairFunction {

	static public final String defaultTransformation = "mapToPair";

	final private ArrayList<Tuple2> matches;

	@SuppressWarnings("unchecked")
	public Grok(HashMap<String, Object> conf) {
		System.out.println(conf);

		ArrayList<HashMap> originalMatches = (ArrayList<HashMap>) conf
				.get("match");
		this.matches = this.prepareMatchConf(originalMatches);
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

	private ArrayList prepareMatchConf(ArrayList<HashMap> originalMatches) {
		ArrayList<Tuple2> matches = new ArrayList<Tuple2>();
		for (HashMap match : originalMatches) {
			String src = (String) match.keySet().iterator().next();

			final ArrayList<Tuple2> patternAndGroupnames = new ArrayList<Tuple2>();

			for (String m : (ArrayList<String>) match.get(src)) {
				Pattern p = Pattern.compile(m);
				Set<String> groupnames = this.getNamedGroupCandidates(m);
				patternAndGroupnames.add(new Tuple2(p, groupnames));
			}

			matches.add(new Tuple2(src, patternAndGroupnames));
		}
		return matches;
	}

	public Tuple2 call(Object arg0) {
		// TODO Auto-generated method stub
		Tuple2 t = (Tuple2) arg0;
		Object originKey = t._1;
		HashMap<String, Object> event = (HashMap<String, Object>) t._2;

		try {
			for (Tuple2 match : this.matches) {
				String src = (String) match._1;
				if (!event.containsKey(src)) {
					continue;
				}
				String input = (String) event.get(src);

				ArrayList<Tuple2> pAndGns = (ArrayList<Tuple2>) match._2;
				for (Tuple2 pAndgn : pAndGns) {
					Pattern p = (Pattern) pAndgn._1;
					
					Matcher m = p.matcher(input);

					if (!m.find()) {
						continue;
					}

					Set<String> groupnames = (Set) pAndgn._2;
					for (String groupname : groupnames) {
						event.put(groupname, (String) m.group(groupname));
					}
					
					break;
				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new Tuple2(originKey, event);
		}

		return new Tuple2(originKey, event);

	}

	public static void main(String[] args) {
		String input = "   1427965391.659     29 10.8.74.48[-] TCP_MISS/500 150 513 GET http://www.weather.com.cn/weather/%E5%8B%83%E5%88%A9.shtml - HIER_DIRECT/180.9    7.161.108 text/html 500 -";
		String pattern = "\\s*(?<logtime>\\S+)\\s+(?<responseTime>\\d+)\\s+\\S+[^/]+/(?<requeststatusCode>\\d+)(\\s+\\S+){3}\\s+(.*://)?(?<domain>[^/]+)/";
		Matcher m = Pattern.compile(pattern).matcher(input);
		m.find();
		System.out.println(m.group("logtime"));
	}
}
