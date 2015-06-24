package utils.joni;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

import scala.Tuple2;
import scala.Tuple3;
import utils.postProcess.PostProcess;

public class JoniRegex {

	private final static Logger LOGGER = Logger.getLogger(JoniRegex.class
			.getName());

	private List<Tuple2> matches = null;
	private Map conf;
	private final String tagOnFailure;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public JoniRegex(Map conf) {

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

	private List<String> getNamedGroupCandidates(String regex) {
		ArrayList<String> namedGroups = new ArrayList<String>();

		java.util.regex.Matcher m = Pattern.compile(
				"\\(\\?<([a-zA-Z][-_a-zA-Z0-9]*)>").matcher(regex);

		while (m.find()) {
			namedGroups.add(m.group(1));
		}

		return namedGroups;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List prepareMatchConf(ArrayList<HashMap> originalMatches) {
		ArrayList<Tuple2> matches = new ArrayList<Tuple2>();
		for (HashMap matchconf : originalMatches) {
			String src = (String) matchconf.keySet().iterator().next();

			final ArrayList<Tuple2> regexAndGroupnames = new ArrayList<Tuple2>();

			for (String m : (ArrayList<String>) matchconf.get(src)) {

				LOGGER.log(Level.FINE, this.getNamedGroupCandidates(m)
						.toString());

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
	public void process(Map event) {
		for (Tuple2 match : this.matches) {

			String src = (String) match._1();
			if (!event.containsKey(src)) {
				continue;
			}

			boolean success = false;
			String input = ((String) event.get(src));

			try {
				ArrayList<Tuple2> regexAndGroupnames = (ArrayList<Tuple2>) match
						._2();
				for (Tuple2 rAndgn : regexAndGroupnames) {
					Regex regex = (Regex) rAndgn._1;

					Matcher matcher = regex.matcher(input.getBytes());
					int result = matcher.search(0, input.getBytes().length,
							Option.DEFAULT);

					if (result != -1) {
						success = true;
						Region region = matcher.getEagerRegion();
						ArrayList<String> groupnames = (ArrayList<String>) rAndgn._2;
						for (int i = 1; i < region.numRegs; i++) {
							if (region.beg[i] != -1) {
								event.put(groupnames.get(i - 1),
										input.substring(region.beg[i],
												region.end[i]));
							}
						}

						break;
					}
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
				PostProcess.process(event, this.conf);
			}
		}
	}

	public static void main(String[] args) {

		String pattern, input;
		pattern = "(?<logtime>\\S+\\s+\\S+)\\s+\"(?<domain>\\S+)\"\\s+(?<local_host>\\S+)\\s+(?<ip>\\S+)\\s+(?<method>\\S+)\\s+(?<uri>\\S+)\\s+(?<query_string>\\S+)\\s+(?<server_port>\\S+)\\s+(?<username>\\S+)\\s+(?<c_ip>\\S+)\\s+(?<real_ip>\\S+)\\s+"
				+ "(?<cs_version>\\S+)\\s+\"(?<UA>.*)\"\\s+\"(-|(?<cs_Cookie_>.*?))\"\\s+\"(-|(?<cs_Referer_>.*?))\";?\\s+(?<sc_status>\\d+)\\s+(?<send_bytes>\\d+)\\s+(?<receive_bytes>\\d+)\\s+(?<time_taken>\\d+)";
		input = "2015-06-24 14:40:59 \"ws.connect.qiche.ctripcorp.com\" ws.connect.qiche.ctripcorp.com 10.8.91.167 GET /index.php \"?param=/ticket/pullPay&website=&userAccount=zhifu5\" 80 - 10.8.91.169 - HTTP/1.1  \"PHP/5.3.17\" \"-\" \"-\" 200 316 152 5919";
		Regex regex = new Regex(pattern.getBytes(), 0,
				pattern.getBytes().length, Option.NONE, UTF8Encoding.INSTANCE);

		java.util.regex.Matcher m = Pattern.compile(
				"\\(\\?<([a-zA-Z][-_a-zA-Z0-9]*)>").matcher(pattern);

		ArrayList groupnames = new ArrayList();
		while (m.find()) {
			groupnames.add(m.group(1));
		}

		Matcher matcher = regex.matcher(input.getBytes());

		int result = matcher.search(0, input.getBytes().length, Option.DEFAULT);

		if (result != -1) {
			Region region = matcher.getEagerRegion();
			System.out.println(groupnames.size());
			System.out.println(region.beg.length);

			for (int i = 1; i < region.numRegs; i++) {
				System.out.println(groupnames.get(i - 1) + ": " + region.beg[i]
						+ "," + region.end[i]);
				if (region.beg[i] != -1) {

					System.out.println(input.substring(region.beg[i],
							region.end[i]));
				}
			}
		}

	}
}
