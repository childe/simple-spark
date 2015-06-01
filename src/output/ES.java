package output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jinmanager.JinManager;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.json.simple.JSONValue;

import com.hubspot.jinjava.interpret.Context;

import scala.Tuple2;
import utils.ESClient;

public class ES implements Function2 {
	static public final String defaultTransformation = "foreachRDD";

	private final Map conf;

	private final String index;
	private final String indexType;

	private Object bulkProcessor = null, esclient = null;

	private final static Logger LOGGER = Logger.getLogger(ES.class.getName());

	/**
	 * @param conf
	 * @throws Exception
	 */
	public ES(HashMap conf) throws Exception {
		LOGGER.log(Level.INFO, conf.toString());

		this.conf = conf;

		this.index = (String) conf.get("index");

		if (conf.containsKey("index_type")) {
			this.indexType = (String) conf.get("index_type");
		} else {
			this.indexType = "logs";
		}
	}

	private void initESClient() {

		if (this.esclient != null) {
			return;
		}

		String clusterName = (String) this.conf.get("cluster");

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.sniff", true)
				.put("cluster.name", clusterName).build();

		this.esclient = new TransportClient(settings);

		ArrayList<String> hosts = (ArrayList<String>) conf.get("hosts");
		for (String host : hosts) {
			String[] hp = host.split(":");
			String h = null, p = null;
			if (hp.length == 2) {
				h = hp[0];
				p = hp[1];
			} else if (hp.length == 1) {
				h = hp[0];
				p = "9300";
			}
			((TransportClient) this.esclient)
					.addTransportAddress(new InetSocketTransportAddress(h,
							Integer.parseInt(p)));
		}

		int bulkActions = 5000, bulkSize = 200, flushInterval = 5, concurrentRequests = 1;
		if (conf.containsKey("bulk_actions")) {
			bulkActions = (int) this.conf.get("bulk_actions");
		}
		if (conf.containsKey("bulk_size")) {

			bulkSize = (int) this.conf.get("bulk_size");
		}
		if (conf.containsKey("flush_interval")) {

			flushInterval = (int) this.conf.get("flush_interval");
		}
		if (conf.containsKey("concurrent_requests")) {

			concurrentRequests = (int) this.conf.get("concurrent_requests");
		}

		bulkProcessor = BulkProcessor
				.builder((TransportClient) this.esclient,
						new BulkProcessor.Listener() {
							@Override
							public void afterBulk(long arg0, BulkRequest arg1,
									BulkResponse arg2) {
								// TODO Auto-generated method stub
								if (arg2.hasFailures()) {
									LOGGER.log(Level.SEVERE,
											arg2.buildFailureMessage());
									LOGGER.log(Level.SEVERE, arg1.toString());
								}
							}

							@Override
							public void afterBulk(long arg0, BulkRequest arg1,
									Throwable arg2) {
								// TODO Auto-generated method stub
								LOGGER.log(Level.SEVERE, arg2.getMessage());
								LOGGER.log(Level.SEVERE, arg1.toString());
							}

							@Override
							public void beforeBulk(long arg0, BulkRequest arg1) {
								// TODO Auto-generated method stub

							}
						}).setBulkActions(bulkActions)
				.setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
				.setConcurrentRequests(concurrentRequests).build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Object call(Object arg0, Object arg1) throws Exception {
		JavaPairRDD rdd = (JavaPairRDD) arg0;

		rdd.foreachPartition(new VoidFunction<Iterator>() {
			@Override
			public void call(Iterator iter) throws Exception {

				// initESClient();

				while (iter.hasNext()) {
					final Tuple2 e = (Tuple2) iter.next();

					if (e._2 == null) {
						continue;
					}

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

					ESClient.getInstance(conf).add(_index, _type,
							JSONValue.toJSONString(e._2));

					// either use client#prepare, or use Requests# to directly
					// build index/delete requests
					// ((BulkProcessor) bulkProcessor).add(new IndexRequest(
					// _index, _type).source(JSONValue.toJSONString(e._2)));
				}

				return;
			}

		});
		return null;
	}
}
