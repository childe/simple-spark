package transformation;

import org.apache.spark.api.java.function.Function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

public class Date implements Function {
	private HashMap<String, Object> conf;

	public Date() {
		this.conf = null;
	}

	public Date(HashMap<String, Object> conf) {
		this.conf = conf;
	}

	@Override
	public Object call(Object arg0) {
		// TODO Auto-generated method stub
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		if (event == null) {
			return event;
		}

		String src = (String) this.conf.get("src");
		if (!event.containsKey(src)) {
			return event;
		}

		String stringDate = (String) event.get(src);
		String target = (String) this.conf.get("target");
		String stringFormat = (String) (this.conf.get("format"));
		SimpleDateFormat sdf = new SimpleDateFormat(stringFormat);
		java.util.Date date;
		try {
			date = sdf.parse(stringDate);
			event.put(target, date.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}

		return event;
	}

	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		java.util.Date date;
		try {
			date = sdf.parse("2015/05/06 10:31:20.527");
			System.out.println(date.toLocaleString());
			System.out.println(date.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		try {
			date = sdf.parse("2015-05-07 09:47:23.495");
			System.out.println(date.toLocaleString());
			System.out.println(date.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
