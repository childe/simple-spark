package utils.es;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class ESManager {
	private static Map<String, ESClient> esrepo = null;

	private final static Logger LOGGER = Logger.getLogger(ESManager.class.getName());

	public static ESClient getInstance(String instanceID, Map conf) {
		synchronized (ESManager.class) {
			if (esrepo == null) {
				esrepo = new HashMap<String, ESClient>();
				esrepo.put(instanceID, new ESClient(conf));
			}
			
			else if (esrepo.get(instanceID)==null){
				esrepo.put(instanceID, new ESClient(conf));
			}
		}

		return esrepo.get(instanceID);
	}
}
