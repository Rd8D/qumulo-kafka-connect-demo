// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.tools.irc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.demo.kafka.commons.qf2.ExceptionUtils;
import org.demo.kafka.connect.qf2.ConfigurationUtil;
import org.demo.kafka.connect.qf2.RestApiClient;
import org.demo.kafka.utils.qf2.crypto.RSAUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikipediaConsumer implements Runnable {

	private static final String SEP = "/";

	private static final String CONSUMER = "WikipediaConsumer";

	private static final Logger log = LoggerFactory.getLogger(WikipediaConsumer.class);

	public static final String SERVERS_CONFIG = "bootstrap.servers";

	public static final String TOPICS_CONFIG = "topics";

	public static final String DIR_CONFIG = "dir";

	private static final String SPOOL = "spool";

	private static final String VAR = SEP + "var";
	
	private static final String INBOX = ".inbox";
	
	public static final String RINBOX = VAR + SEP + SPOOL + SEP + INBOX + SEP;
	
	public static final String RCONSUMER = VAR + SEP + CONSUMER + SEP;

	private final KafkaConsumer<String, String> consumer;

	private final List<String> topics;

	private final int id;

	private RestApiClient restApiClient = null;

	private String dir;

	private String hostname;

	private String username;

	private String pagesize;

	private String password;

	private String key;

	private RSAUtils rsaUtil;

	public WikipediaConsumer(int id, String group, Map<String, String> props) throws Exception {

		String servers = props.get(SERVERS_CONFIG);
		List<String> topics = Arrays.asList(props.get(TOPICS_CONFIG).split(","));
		this.id = id;
		this.topics = topics;
		props.put(SERVERS_CONFIG, servers);
		props.put("group.id", group);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.dir = props.get(DIR_CONFIG);
		this.hostname = props.get(RestApiClient.HOSTNAME_CONFIG);
		this.username = props.get(RestApiClient.USERNAME_CONFIG);
		this.password = props.get(RestApiClient.PASSWORD_CONFIG);
		this.pagesize = props.get(RestApiClient.PAGESIZE_CONFIG);
		this.key = props.get(RSAUtils.KEY_CONFIG);
		if (this.key == null || this.key.isEmpty())
			throw new ConnectException(
					"WikipediaConsumer configuration must include '" + RSAUtils.KEY_CONFIG + "' setting");
		if (this.hostname == null || this.hostname.isEmpty())
			throw new ConnectException(
					"WikipediaConsumer configuration must include '" + RestApiClient.HOSTNAME_CONFIG + "' setting");
		if (this.username == null || this.username.isEmpty())
			throw new ConnectException(
					"WikipediaConsumer configuration must include '" + RestApiClient.USERNAME_CONFIG + "' setting");
		if (this.password == null || this.password.isEmpty())
			throw new ConnectException(
					"WikipediaConsumer configuration must include '" + RestApiClient.PASSWORD_CONFIG + "' setting");
		if (this.pagesize == null || this.pagesize.isEmpty())
			throw new ConnectException(
					"WikipediaConsumer configuration must include '" + RestApiClient.PAGESIZE_CONFIG + "' setting");
		if (this.dir == null || this.dir.isEmpty())
			throw new ConnectException("WikipediaConsumer config missing directory setting");
		log.info(props.toString());
		this.consumer = new KafkaConsumer<>(ConfigurationUtil.mapToprops(props));
		this.restApiClient = RestApiClient.getInstance(props);
		this.restApiClient.createDirectory(this.dir);
		this.restApiClient.createDirectory(this.dir + RCONSUMER);
		this.rsaUtil = RSAUtils.getInstance(props);
	}

	@Override
	public void run() {
		try {
			this.consumer.subscribe(this.topics);

			while (true) {
				ConsumerRecords<String, String> records = this.consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					log.trace("Consumer #{}: partition: {}, offset: {}, value:{}", this.id, record.partition(),
							record.offset(), record.value());
					try {
						String event = UUID.randomUUID().toString();
						Long id = this.restApiClient.writeFile(this.dir + RCONSUMER, event,
								this.rsaUtil.encrypt(record.value().getBytes()).getBytes());
						this.restApiClient.rename(this.dir + RINBOX, this.dir + RCONSUMER + event, event);
						log.trace("Write event {} in spool", id);
						Thread.sleep(1L);
					} catch (Throwable t) {
						ExceptionUtils.log(log, t);
					}

				}
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			this.consumer.close();
		}
	}

	public void shutdown() {
		this.consumer.wakeup();
	}

	public static void main(String[] args) throws Exception {

		int numConsumers = 1;
		String group = "consumer-IRC-group";
		Map<String, String> props = Utils.propsToStringMap(Utils.loadProps(args[0]));
		ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

		final List<WikipediaConsumer> consumers = new ArrayList<>();
		for (int i = 0; i < numConsumers; i++) {
			WikipediaConsumer consumer = new WikipediaConsumer(i, group, props);
			consumers.add(consumer);
			executor.submit(consumer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (WikipediaConsumer consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

}
