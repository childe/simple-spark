package serializer;

import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.serializer.SerializerInstance;

import com.esotericsoftware.kryo.Kryo;

import org.apache.spark.serializer.Serializer;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

public class CKryoRegistrator implements KryoRegistrator{

	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(UTF8Encoding.class);
		kryo.register(Matcher.class);
		kryo.register(NameEntry.class);
		kryo.register(Option.class);
		kryo.register(Regex.class);
		kryo.register(Region.class);
	}
}
