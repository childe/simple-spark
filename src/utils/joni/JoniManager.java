package utils.joni;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class JoniManager {
	private static Map<String, JoniRegex> regexrepo = null;

	private final static Logger LOGGER = Logger.getLogger(JoniManager.class.getName());

	public static JoniRegex getInstance(String instanceID, Map conf) {
		synchronized (JoniManager.class) {
			if (regexrepo == null) {
				regexrepo = new HashMap<String, JoniRegex>();
				regexrepo.put(instanceID, new JoniRegex(conf));
			}
			
			else if (regexrepo.get(instanceID)==null){
				regexrepo.put(instanceID, new JoniRegex(conf));
			}
		}

		return regexrepo.get(instanceID);
	}
}
