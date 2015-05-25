package output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import jinmanager.JinManager;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.simple.JSONValue;

import akka.event.Logging;

import com.hubspot.jinjava.interpret.Context;

import scala.Tuple2;

public class ES implements Function2 {
	static public final String defaultTransformation = "foreachRDD";
	private final String index;
	private final String indexType;

	private final int tryTime;
	private final int interval;
	private TransportClient client;

	/**
	 * @param conf
	 * @throws Exception
	 */
	public ES(HashMap conf) throws Exception {
		System.out.println(conf);

		String clusterName = (String) conf.get("cluster");
		String[] hosts = (String[]) conf.get("host");

		this.index = (String) conf.get("index");

		if (conf.containsKey("index_type")) {
			this.indexType = (String) conf.get("index_type");
		} else {
			this.indexType = "logs";
		}

		if (conf.containsKey("try_time")) {
			this.tryTime = (int) conf.get("try_time");
		} else {
			this.tryTime = 3;
		}

		if (conf.containsKey("interval")) {
			this.interval = (int) conf.get("interval");
		} else {
			this.interval = 1;
		}

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.sniff", true)
				.put("cluster.name", clusterName).build();
		this.client = new TransportClient(settings);
		for (String host : hosts) {
			String[] hp = host.split(":");
			String h, p;
			if (hp.length == 2) {
				h = hp[0];
				p = hp[1];
			} else if (hp.length == 1) {
				h = hp[0];
				p = "9300";
			} else {
				throw new Exception(host + " format wrong");
			}
			client.addTransportAddress(new InetSocketTransportAddress(hp[0],
					Integer.parseInt(hp[1])));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Object call(Object arg0, Object arg1) throws Exception {

		JavaPairRDD rdd = (JavaPairRDD) arg0;

		rdd.foreachPartition(new VoidFunction<Iterator>() {

			@Override
			public void call(Iterator iter) throws Exception {
				BulkRequestBuilder bulkRequest = client.prepareBulk();
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

					String _index = JinManager.jinjava.render(index, cc);
					String _type = JinManager.jinjava.render(indexType, cc);

					bulkRequest.add(client.prepareIndex(_index, _type)
							.setSource(JSONValue.toJSONString(e._2)));
				}

				int try_count = 0;
				while (try_count < tryTime) {
					try {
						BulkResponse bulkResponse = bulkRequest.execute()
								.actionGet();
						if (bulkResponse.hasFailures()) {
							throw new Exception(bulkResponse
									.buildFailureMessage());
							// process failures by iterating through each bulk
							// response item
						}
						return;
					} catch (Exception e) {
						e.printStackTrace();
						try_count++;
						Thread.sleep(interval * 1000);
					}
				}

				System.out.println("could not send");
				return;
			}

		});
		return null;
	}
}
