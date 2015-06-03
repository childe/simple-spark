package utils.date;

import java.io.Serializable;


public interface Parser extends Serializable {
	public long parse(String input);
}
