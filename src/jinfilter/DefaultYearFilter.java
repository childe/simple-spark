package jinfilter;

import java.util.Calendar;

import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;

public class DefaultYearFilter implements Filter {

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "defautyear";
	}

	@Override
	public Object filter(Object arg0, JinjavaInterpreter arg1, String... arg2) {
		// TODO Auto-generated method stub
		return Calendar.getInstance().get(Calendar.YEAR);
	}
	
	public static void main(String[] args){
		System.out.println(Calendar.getInstance().get(Calendar.YEAR));
	}
}
