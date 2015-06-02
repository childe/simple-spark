package utils.date;

public class UnixMSParser implements Parser {

	@Override
	public long parse(String input) {
		// TODO Auto-generated method stub
		return (long) (Double.parseDouble(input));
	}

	public static void main(String[] args) {
		String input = "1433238542488.29";
		UnixMSParser p = new UnixMSParser();
		System.out.println(p.parse(input));
	}
}
