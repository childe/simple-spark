package utils.joni;

import java.util.ArrayList;
import java.util.HashMap;
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

public class JoniRegex {

	private final static Logger LOGGER = Logger.getLogger(JoniRegex.class.getName());
	
	private ArrayList<Tuple2> matches = null;

	public JoniRegex(Map conf) {
		ArrayList<HashMap> originalMatches = (ArrayList<HashMap>) conf
				.get("match");
		this.matches = this.prepareMatchConf(originalMatches);
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

	public boolean match(Map event) {
		try {
			for (Tuple2 match : this.matches) {
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
						Region region = matcher.getEagerRegion();
						ArrayList<String> groupnames = (ArrayList<String>) rAndgn._2;
						for (int i = 0; i < region.numRegs; i++) {
							event.put(groupnames.get(i), input.substring(
									region.beg[i], region.end[i]));
						}
						return true;
					}
				}

			}

		} catch (Exception e) {
			LOGGER.log(Level.WARNING, e.getLocalizedMessage());
			return false;
		}
		
		return false;
	}
}
