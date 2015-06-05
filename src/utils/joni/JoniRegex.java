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
				"\\(\\?<([a-zA-Z][_-a-zA-Z0-9]*)>").matcher(regex);

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
						for (int i = 0; i < region.numRegs; i++) {
							event.put(groupnames.get(i), input.substring(
									region.beg[i], region.end[i]));
						}

						break;
					}
				}

			} catch (Exception e) {
				LOGGER.log(Level.WARNING, e.getLocalizedMessage());
				success = false;
			}

			if (success == false) {
				LOGGER.log(Level.WARNING, "grok failed." + event.toString());

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
}
