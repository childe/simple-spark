package jinmanager;

import jinfilter.DateFormat;
import jinfilter.DefaultYearFilter;
import jinfilter.DoubleFilter;
import jinfilter.FloorFilter;
import jinfilter.IntegerFilter;
import jinfilter.NowtimeFilter;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;

public class JinManager {
	public static final Jinjava jinjava = new Jinjava();
	public static final Context c = jinjava.getGlobalContext();
	
	static {
		c.registerFilter(new FloorFilter());
		c.registerFilter(new NowtimeFilter());
		c.registerFilter(new DoubleFilter());
		c.registerFilter(new IntegerFilter());
		c.registerFilter(new DateFormat());
		c.registerFilter(new DefaultYearFilter());
    }
}
