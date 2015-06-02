package utils.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class FormatParser implements Parser {

	private DateTimeFormatter formatter;

	public FormatParser(String format, String timezone, String locale) {
		this.formatter = DateTimeFormat.forPattern("YYYY/MM/dd HH:mm:ss.SSS");

		if (timezone != null) {
			this.formatter.withZone(DateTimeZone.forID(timezone));
		}

		//TODO
		// if (locale != null) {
		// this.formatter.withLocale(locale);
		// }

	}

	@Override
	public long parse(String input) throws ParseException {
		// TODO Auto-generated method stub
		return this.formatter.parseMillis(input);

	}

	public static void main(String[] args) throws ParseException {
		String input = "2015/05/06 10:31:20.427";
		FormatParser p = new FormatParser("yyyy/MM/dd HH:mm:ss.SSS",null,null);
		System.out.println(p.parse(input));
	}
}
