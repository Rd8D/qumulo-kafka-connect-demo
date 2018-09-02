// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.connect.qf2;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.demo.kafka.commons.qf2.ExceptionUtils;
import org.demo.kafka.connect.qf2.RestApiSourceTransporter.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestApiSourceTask extends SourceTask {

	private static final Logger log = LoggerFactory.getLogger(RestApiSourceTask.class);

	private static final String SEP = "/";

	private static final String TMP = ".tmp";

	private static final String TRASH = ".wash";

	private static final String TRASHD = ".washd";

	private static final String INBOX = ".inbox";

	private static final String OUTBOX = ".outbox";

	private static final String SPOOL = "spool";

	private static final String VAR = SEP + "var";

	public static final String RTMP = VAR + SEP + SPOOL + SEP + TMP + SEP;

	public static final String RWASH = VAR + SEP + SPOOL + SEP + TRASH + SEP;

	public static final String RWASHD = VAR + SEP + SPOOL + SEP + TRASHD + SEP;

	public static final String RINBOX = VAR + SEP + SPOOL + SEP + INBOX + SEP;

	public static final String ROUTBOX = VAR + SEP + SPOOL + SEP + OUTBOX + SEP;

	private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;

	private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

	public static final String SPOOL_FIELD = SPOOL;

	public static final String OFFSET_FIELD = "id";

	private String dir;

	private RestApiClient restApiClient;

	private RestApiSourceTransporter inbox;

	private RestApiSourceTransporter outbox;

	private String topic = null;

	private BlockingQueue<Map.Entry<Long, RestApiClient.Entry>> queue;

	private class RestApiTransportListener extends TransportListener {

		@Override
		public void onMove(Long id, RestApiClient.Entry payload) {
			if (id != null) {
				queue.offer(new AbstractMap.SimpleEntry<Long, RestApiClient.Entry>(id, payload));
			} else {
				log.error("onMove(): received empty payload");
			}
		}
	}

	@Override
	public String version() {
		return new RestApiSourceConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {

		log.trace("Starting RestApiSourceConnector, ts:{}", System.currentTimeMillis());
		this.dir = props.get(RestApiSourceConnector.DIR_CONFIG);
		if (this.dir == null || this.dir.isEmpty())
			throw new ConnectException("RestApiSourceTask config missing directory setting");
		StringUtils.stripEnd(this.dir, SEP);
		if (!this.dir.startsWith(SEP))
			this.dir = SEP + this.dir;
		this.topic = props.get(RestApiSourceConnector.TOPIC_CONFIG);
		if (this.topic == null)
			throw new ConnectException("RestApiSourceTask config missing topic setting");
		try {
			log.info(props.toString());
			int count = 0;
			int attempt = 0;
			while (count < 4) {
				Map<Long, RestApiClient.Entry> dirs = null;
				try {
					this.restApiClient = RestApiClient.getInstance(props);
					synchronized (this.restApiClient) {
						wash();
						this.restApiClient.createDirectory(this.dir + RTMP);
						this.restApiClient.createDirectory(this.dir + RWASH);
						this.restApiClient.createDirectory(this.dir + RINBOX);
						this.restApiClient.createDirectory(this.dir + ROUTBOX);
						recycle();
					}
					dirs = this.restApiClient.listDirectories(this.dir + VAR + SEP + SPOOL, false);
				} catch (Exception e) {
					ExceptionUtils.log(log, e);
					dirs = null;
				}
				if (dirs != null)
					log.trace("dirs cnt: {}", dirs.size());
				for (Entry<Long, RestApiClient.Entry> dir : dirs.entrySet()) {
					log.trace("Spool tree: {}", dir.getValue().toString());
					if (dir.getValue().getPath().endsWith(RTMP) || dir.getValue().getPath().endsWith(RWASH)
							|| dir.getValue().getPath().endsWith(RWASHD) || dir.getValue().getPath().endsWith(RINBOX)
							|| dir.getValue().getPath().endsWith(ROUTBOX))
						count += 1;
				}
				if (count < 5) {
					if (attempt > 5)
						throw new Exception("RestApiSourceTask could not initialize spool tree when starting");
					log.error("RestApiSourceTask could not initialize spool tree when starting, retrying...");
					Thread.sleep(5000L);
					count = 0;
					attempt += 1;
				}
			}

			log.trace("Starting transporters, ts:{}", System.currentTimeMillis());
			this.queue = new LinkedBlockingQueue<>();
			this.inbox = new RestApiSourceTransporter(this.dir + RINBOX, this.dir + RTMP);
			this.inbox.start();
			this.outbox = new RestApiSourceTransporter(this.dir + RTMP, this.dir + ROUTBOX);
			this.outbox.setEventListener(new RestApiTransportListener());
			this.outbox.start();

		} catch (Throwable t) {
			throw new ConnectException(t);
		}
	}

	private void wash() {
		try {
			this.restApiClient.createDirectory(this.dir + RWASHD);
			String newname = UUID.randomUUID().toString();
			this.restApiClient.rename(this.dir + RWASHD, this.dir + RWASH, newname);
			this.restApiClient.deleteTree(this.dir + RWASHD + SEP + newname);
		} catch (Exception e) {
			ExceptionUtils.log(log, e);
		}
	}

	private void recycle() throws Exception {
		int count = 0;
		Map<Long, RestApiClient.Entry> payloads = this.restApiClient.listFiles(this.dir + ROUTBOX, true);
		for (Entry<Long, RestApiClient.Entry> payload : payloads.entrySet()) {
			log.trace("Recycling {} back to {}", payload.getValue().getPath(), this.dir + RINBOX);
			rename(this.dir + RINBOX, payload.getValue().getPath(), payload.getValue().getName());
			count += 1;
		}
		payloads = this.restApiClient.listFiles(this.dir + RTMP, true);
		for (Entry<Long, RestApiClient.Entry> payload : payloads.entrySet()) {
			log.trace("Recycling {} back to {}", payload.getValue().getPath(), this.dir + RINBOX);
			rename(this.dir + RINBOX, payload.getValue().getPath(), payload.getValue().getName());
			count += 1;
		}
		log.trace("Recycled payloads cnt: {}", count);
	}

	private void check() throws Exception {
		int count = 0;
		Map<Long, RestApiClient.Entry> dirs = this.restApiClient.listDirectories(this.dir + VAR + SEP + SPOOL, false);
		if (dirs != null)
			log.trace("Pool() dirs cnt: {}", dirs.size());
		for (Entry<Long, RestApiClient.Entry> dir : dirs.entrySet()) {
			log.trace("Spool tree: {}", dir.getValue().toString());
			if (dir.getValue().getPath().endsWith(RTMP) || dir.getValue().getPath().endsWith(RWASH)
					|| dir.getValue().getPath().endsWith(RWASHD) || dir.getValue().getPath().endsWith(RINBOX)
					|| dir.getValue().getPath().endsWith(ROUTBOX))
				count += 1;
		}
		if (count < 4)
			throw new Exception("Spool tree was not properly initialized");
	}

	private Long rename(String path, String oldpath, String newname) throws Exception {

		Long id = null;
		boolean done = false;
		if (!path.endsWith(SEP))
			path += SEP;
		while (!done) {
			try {
				id = this.restApiClient.rename(path, oldpath, newname);
				log.trace("{} is now {}", oldpath, path + newname);
				done = true;
			} catch (Exception e) {
				log.error("Cannot rename {} to {}, trying to investigate", oldpath, path + newname);
				ExceptionUtils.log(log, e);
				Thread.sleep(1000L);
			}
			// Inspect new path
			if (!done) {
				try {
					log.trace("Looking after indolent move");
					Map<String, String> attrs = this.restApiClient.attributes(path + newname);
					if (attrs.get("name").equals(newname)) {
						log.trace("Indolent move occured for entry: {}", attrs.toString());
						id = Long.valueOf(attrs.get("id"));
						done = true;
					}
				} catch (Exception e) {
					log.error("{} existence not verifiable in {} as {}", oldpath, path, newname);
					ExceptionUtils.log(log, e);
				}
			}
			// Inspect old path
			if (!done) {
				try {
					log.trace("Try to inspect old path for {}", oldpath);
					Map<String, String> attrs = this.restApiClient.attributes(oldpath);
					if (attrs.get("path").startsWith(oldpath))
						log.trace("Found entry: {}", attrs.toString());
				} catch (Exception e) {
					log.error("{} existence is not verifiable", oldpath);
					ExceptionUtils.log(log, e);
					done = true;
				}
			}
		}

		return id;
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {

		ArrayList<SourceRecord> records = null;
		List<Map.Entry<Long, RestApiClient.Entry>> payloads = null;

		try {
			check();
		} catch (Exception e) {
			throw new InterruptedException(e.getMessage());
		}

		try {
			records = new ArrayList<SourceRecord>();
			payloads = new LinkedList<>();
			if (this.queue.isEmpty())
				payloads.add(this.queue.take());
			this.queue.drainTo(payloads);
			log.trace("Payloads cnt in blocking queue: {}", payloads.size());
			for (Map.Entry<Long, RestApiClient.Entry> payload : payloads) {
				byte[] data = null;
				try {
					try {
						data = this.restApiClient.readFile(payload.getKey());
					} catch (Exception e) {
						log.error("Cannot read content for payload {}", payload.getValue().getPath());
						ExceptionUtils.log(log, e);
						data = null;
					}
					try {
						String newname = UUID.randomUUID().toString();
						rename(this.dir + RWASH, payload.getValue().getPath(), newname);
					} catch (Exception e) {
						log.error("Cannot move {} to recycle bin", payload.getValue().getPath());
						ExceptionUtils.log(log, e);
						data = null;
					}
					Long id = payload.getKey();
					if (data != null) {
						SourceRecord record = new SourceRecord(Collections.singletonMap(SPOOL_FIELD, payload.getValue().getName()),
								Collections.singletonMap(OFFSET_FIELD, Long.toString(id)), this.topic, null, KEY_SCHEMA, Long.toString(id),
								VALUE_SCHEMA, new String(Base64.getEncoder().encode(data)), System.currentTimeMillis());
						records.add(record);
						Thread.sleep(1L);
					} else {
						log.error("Payload {} won't be convoyed", payload.getValue().getPath());
					}
				} catch (Exception e) {
					ExceptionUtils.log(log, e);
					throw e;
				}
			}
			log.trace("Fetched payloads cnt: {}", records.size());
		} catch (Throwable t) {
			ExceptionUtils.log(log, t);
			throw new InterruptedException("RestApiClient failed");
		}

		return records;

	}

	@Override
	public void stop() {
		this.inbox.stop();
		try {
			log.info("Waiting for inbox worker thread landing");
			this.inbox.getWorker().join();
		} catch (InterruptedException e) {
			ExceptionUtils.log(log, e);
		}
		this.outbox.stop();
		try {
			log.info("Waiting for outbox worker thread landing");
			this.outbox.getWorker().join();
		} catch (InterruptedException e) {
			ExceptionUtils.log(log, e);
		}
		this.queue.clear();
	}

}
