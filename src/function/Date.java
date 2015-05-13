package function;

import org.apache.spark.api.java.function.Function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class Date implements Function {
	ArrayList<HashMap<String, Object>> convert;

	public Date() {
	}

	@SuppressWarnings("unchecked")
	public Date(HashMap<String, Object> conf) {
		System.out.println(conf);

		this.convert = new ArrayList<HashMap<String, Object>>();
		for (HashMap<String, Object> object : (ArrayList<HashMap<String, Object>>) conf
				.get("convert")) {
			if (!object.containsKey("target")) {
				object.put("target", "@timestamp");
			}

			// prepare SimpleDateFormat arrays

			ArrayList<SimpleDateFormat> realformats = new ArrayList<SimpleDateFormat>();
			@SuppressWarnings("unchecked")
			ArrayList<String> formats = (ArrayList<String>) object
					.get("format");
			for (String format : formats) {
				realformats.add(new SimpleDateFormat(format));
			}
			object.put("formats", realformats);

			this.convert.add(object);
		}

		System.out.println(this.convert);
	}

	@Override
	public Object call(Object arg0) {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		HashMap<String, Object> event = (HashMap<String, Object>) arg0;

		if (event == null) {
			return event;
		}

		for (HashMap<String, Object> object : this.convert) {
			String src = (String) object.get("src");
			if (!event.containsKey(src)) {
				continue;
			}

			String stringDate = (String) event.get(src);

			String target = (String) object.get("target");

			boolean success = false;
			@SuppressWarnings("unchecked")
			ArrayList<SimpleDateFormat> formats = (ArrayList<SimpleDateFormat>) object
					.get("formats");
			for (SimpleDateFormat format : formats) {
				java.util.Date date;
				try {
					date = format.parse(stringDate);
					event.put(target, date.getTime());
					success = true;
					break;
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
				}
			}

			if (success == false) {
				// TODO: log
			}
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
