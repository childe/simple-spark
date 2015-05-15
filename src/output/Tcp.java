package output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import jinmanager.JinManager;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;

import com.hubspot.jinjava.interpret.Context;

import scala.Tuple2;

public class Tcp implements Function2 {
	static public final String defaultTransformation = "foreachRDD";
	private final String host;
	private final int port;
	private final int tryTime;
	private final String format;

	public Tcp(HashMap conf) {
		System.out.println(conf);

		this.host = (String) conf.get("host");
		this.port = (int) conf.get("port");
		if (conf.containsKey("try_time")) {
			this.tryTime = (int) conf.get("try_time");
		} else {
			this.tryTime = 5;
		}

		if (conf.containsKey("format")) {
			String _format = "";
			for (String string : (ArrayList<String>) conf.get("format")) {
				_format += "\n" + string;
			}
			this.format = _format;
		} else {
			this.format = "{{event}}";
		}
	}

	@Override
	public Object call(Object arg0, Object arg1) throws Exception {

		JavaPairRDD rdd = (JavaPairRDD) arg0;

		rdd.foreachPartition(new VoidFunction<Iterator>() {

			@Override
			public void call(Iterator iter) throws Exception {

				String msg = "";

				while (iter.hasNext()) {
					final Tuple2 e = (Tuple2) iter.next();

					final ArrayList event = new ArrayList() {
						{
							add(e._1);
							add(e._2);
						}
					};

					HashMap binding = new HashMap() {
						{
							put("event", event);
						}
					};

					Context cc = new Context(JinManager.c, binding);

					msg += "\n" + JinManager.jinjava.render(format, cc);

				}

				System.out.println(msg);

				int try_count = 0;
				while (try_count < tryTime) {
					try {
						Socket socket = new Socket(host, port);
						OutputStream s = socket.getOutputStream();
						PrintWriter out = new PrintWriter(s, true);
						out.print(msg);
						out.close();
						socket.close();
						return;
					} catch (Exception e) {
						e.printStackTrace();
						try_count++;
					}
				}

				System.out.println("could not send");
				return;
			}

		});
		return null;
	}
}
