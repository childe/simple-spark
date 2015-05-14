package jinfilter;

import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;

public class NowtimeFilter implements Filter {

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "nowtime";
	}

	@Override
	public Object filter(Object arg0, JinjavaInterpreter arg1, String... arg2) {
		// TODO Auto-generated method stub
		return System.currentTimeMillis();
	}
}
