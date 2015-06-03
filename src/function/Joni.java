package function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

public class Joni implements PairFunction {

	private final static Logger LOGGER = Logger.getLogger(Joni.class.getName());

	static public final String defaultTransformation = "mapToPair";

	private ArrayList<Tuple2> matches = null;
	private final String tagOnFailure;
	private Map conf;

	@SuppressWarnings("unchecked")
	public Joni(HashMap<String, Object> conf) {
		System.out.println(conf);

		this.conf = conf;

		if (conf.containsKey("tag_on_failure")) {
			this.tagOnFailure = (String) conf.get("tag_on_failure");
		} else {
			this.tagOnFailure = "grokfail";
		}

		this.matches = this.prepareMatchConf((ArrayList<HashMap>) conf
				.get("match"));

	}

	private ArrayList<String> getNamedGroupCandidates(String regex) {
		ArrayList<String> namedGroups = new ArrayList<String>();

		java.util.regex.Matcher m = Pattern.compile(
				"\\(\\?<([a-zA-Z][_-a-zA-Z0-9]*)>").matcher(regex);

		while (m.find()) {
			namedGroups.add(m.group(1));
		}

		return namedGroups;
	}

	private ArrayList prepareMatchConf(ArrayList<HashMap> originalMatches) {
		ArrayList<Tuple2> matches = new ArrayList<Tuple2>();
		for (HashMap match : originalMatches) {
			String src = (String) match.keySet().iterator().next();

			final ArrayList<Tuple2> regexAndGroupnames = new ArrayList<Tuple2>();

			for (String m : (ArrayList<String>) match.get(src)) {
				Regex regex = new Regex(m.getBytes(), 0, m.getBytes().length,
						Option.NONE, UTF8Encoding.INSTANCE);
				regexAndGroupnames.add(new Tuple2(regex, this
						.getNamedGroupCandidates(m)));
			}

			matches.add(new Tuple2(src, regexAndGroupnames));
		}
		return matches;
	}

	@SuppressWarnings("unchecked")
	private boolean match(Map event) {
		boolean rst = true;
		try {
			for (Tuple2 match : this.matches) {
				boolean thisMatch = false;

				String src = (String) match._1;
				if (!event.containsKey(src)) {
					continue;
				}

				String input = ((String) event.get(src));

				ArrayList<Tuple2> regexAndGroupnames = (ArrayList<Tuple2>) match._2;
				for (Tuple2 rAndgn : regexAndGroupnames) {
					Regex regex = (Regex) rAndgn._1;

					Matcher matcher = regex.matcher(input.getBytes());
					int result = matcher.search(0, input.getBytes().length,
							Option.DEFAULT);

					if (result != -1) {
						thisMatch = true;
						Region region = matcher.getEagerRegion();
						ArrayList<String> groupnames = (ArrayList<String>) rAndgn._2;
						for (int i = 0; i < region.numRegs; i++) {
							event.put(groupnames.get(i), input.substring(
									region.beg[i], region.end[i]));
						}

						break;
					}
				}

				if (thisMatch == false) {
					rst = false;
				}
			}

		} catch (Exception e) {
			LOGGER.log(Level.WARNING, e.getLocalizedMessage());
			rst = false;
		}

		return rst;
	}

	public Tuple2 call(Object arg0) {
		Tuple2 t = (Tuple2) arg0;
		Object originKey = t._1;
		HashMap<String, Object> event = (HashMap<String, Object>) t._2;

		boolean success = this.match(event);

		if (success == false) {
			LOGGER.log(Level.WARNING, "grok failed." + event.toString());

			if (!event.containsKey("tags")) {
				event.put("tags",
						new ArrayList<String>(Arrays.asList(this.tagOnFailure)));
			} else {
				Object tags = event.get("tags");
				if (tags.getClass() == ArrayList.class
						&& ((ArrayList) tags).indexOf(this.tagOnFailure) == -1) {
					((ArrayList) tags).add(this.tagOnFailure);
				}
			}
		}

		return new Tuple2(originKey, event);

	}

	public static void main(String[] args) {
		String input = "[01/Jun/2015:16:48:28 +0800] 10.8.88.110 GET /index.php action=liancheng&from=%BB%A2%C1%D6&to=%C4%CF%B5%A4&format=json&user=tieyou&reqtime=1433148508&sign=9fff44495fbdf0c5239dcc9fdd9bc21b  80 - 10.8.109.237 10.8.56.48 HTTP/1.1 \"PHP/5.3.17\" \"-\" \"-\" ws.shopping.train.ctripcorp.com 200 94 0.026 0.026 127.0.0.1:9000 200";
		//
		String pattern = "\\[(?<logtime>\\S+\\s+\\S+)\\]\\s+(?<serverAddr>\\S+)\\s+(?<requestMethod>\\S+)\\s+(?<uri>\\S+)\\s+(?<queryString>\\S+)\\s+(?<serverPort>\\S+)\\s+(?<remoteUser>\\S+)\\s+(?<remoteAddr>\\S+)\\s+(?<forwarded>\\S+)\\s+(?<serverProtocol>\\S+)\\s+(?<UA>\\S+)\\s+(?<cookie>.+?)\\s+(?<referer>.+?)\\s+(?<csHost>\\S+)\\s+(?<statusCode>\\S+)\\s+(?<bodyBytesSent>\\S+)\\s+(?<requestTime>\\S+)\\s+(?<upstreamResponseTime>\\S+)\\s+(?<upstreamAddr>\\S+)\\s+(?<upstreamStatus>\\S+)";

		TreeSet<String> namedGroups = new TreeSet<String>();

		java.util.regex.Matcher m = Pattern.compile(
				"\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(pattern);

		while (m.find()) {
			namedGroups.add(m.group(1));
		}

		Pattern p = Pattern.compile(pattern);

		long s = System.currentTimeMillis();

		for (int i = 0; i < 1000; i++) {
			m = p.matcher(input);
			m.find();
			HashMap event = new HashMap();
			for (String groupname : namedGroups) {
				event.put(groupname, (String) m.group(groupname));
			}
		}

		System.out.println(System.currentTimeMillis() - s);

		pattern = "\\[(?<logtime>\\S+\\s+\\S+)\\]\\s+(?<server_addr>\\S+)\\s+(?<request_method>\\S+)\\s+(?<uri>\\S+)\\s+(?<queryString>\\S+)\\s+(?<serverPort>\\S+)\\s+(?<remoteUser>\\S+)\\s+(?<remoteAddr>\\S+)\\s+(?<forwarded>\\S+)\\s+(?<serverProtocol>\\S+)\\s+(?<UA>\\S+)\\s+(?<cookie>.+?)\\s+(?<referer>.+?)\\s+(?<csHost>\\S+)\\s+(?<statusCode>\\S+)\\s+(?<bodyBytesSent>\\S+)\\s+(?<requestTime>\\S+)\\s+(?<upstreamResponseTime>\\S+)\\s+(?<upstreamAddr>\\S+)\\s+(?<upstreamStatus>\\S+)";

		Regex regex = new Regex(pattern.getBytes(), 0,
				pattern.getBytes().length, Option.NONE, UTF8Encoding.INSTANCE);

		s = System.currentTimeMillis();

		for (int i = 0; i < 1000; i++) {
			Matcher matcher = regex.matcher(input.getBytes());
			int result = matcher.search(0, input.getBytes().length,
					Option.DEFAULT);

			if (result != -1) {
				Region region = matcher.getEagerRegion();

				HashMap event = new HashMap();

				for (Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry
						.hasNext();) {
					NameEntry e = entry.next();

					int number = e.getBackRefs()[0];

					int begin = region.beg[number];
					int end = region.end[number];
					event.put(pattern.substring(e.nameP, e.nameEnd),
							input.substring(begin, end));

				}
				// System.out.println(event);
			}
		}

		System.out.println(System.currentTimeMillis() - s);
	}
}
