package jinmanager;

import jinfilter.FloorFilter;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;

public class JinManager {
	public static final Jinjava jinjava = new Jinjava();
	public static final Context c = jinjava.getGlobalContext();
	
	static {
		c.registerFilter(new FloorFilter());
    }
}
