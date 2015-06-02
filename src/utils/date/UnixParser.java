package utils.date;

public class UnixParser implements Parser {

	@Override
	public long parse(String input) {
		// TODO Auto-generated method stub
		return (long) (Double.parseDouble(input) * 1000);
	}

	public static void main(String[] args) {
		String input = "1433238542.48729";
		UnixParser p = new UnixParser();
		System.out.println(p.parse(input));
	}
}
