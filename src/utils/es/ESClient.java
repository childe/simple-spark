package utils.es;

import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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

public class ESClient {

	private final static Logger LOGGER = Logger.getLogger(ESClient.class
			.getName());

	private Map conf;

	private static ESClient esClient = null;
	private Object bulkProcessor = null, esclient = null;

	public ESClient(Map conf) {
		this.conf = conf;

		initESClient();
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
								if (arg2.hasFailures()) {
									LOGGER.log(Level.SEVERE,
											arg2.buildFailureMessage());
									LOGGER.log(Level.SEVERE, arg1.toString());
								}
							}

							@Override
							public void afterBulk(long arg0, BulkRequest arg1,
									Throwable arg2) {
								LOGGER.log(Level.FINE, arg2.getMessage());
								LOGGER.log(Level.FINE, arg1.toString());
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

	public void add(String index, String type, String source) {
		((BulkProcessor) bulkProcessor).add(new IndexRequest(index, type)
				.source(source));
	}
}