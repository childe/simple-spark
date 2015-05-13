package jinmanager;

import jinfilter.Floor;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;

public class JinManager {
	public static final Jinjava jinjava = new Jinjava();
	public static final Context c = jinjava.getGlobalContext();
	
	public static void prepare(){
		c.registerFilter(new Floor());
	}
}
