// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.connect.qf2;

import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.demo.kafka.commons.qf2.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestApiSourceTransporter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(RestApiSourceTransporter.class);

	private Thread worker;

	private String from;

	private String to;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private TransportListener listener;

	private static final String SEP = "/";

	public RestApiSourceTransporter(String from, String to) {
		this.from = from;
		this.to = to;
		this.listener = null;
	}

	public void interrupt() {
		running.set(false);
		worker.interrupt();
	}

	public void start() {
		worker = new Thread(this);
		worker.start();
	}

	public void stop() {
		running.set(false);
	}

	public boolean isRunning() {
		return running.get();
	}

	public Thread getWorker() {
		return this.worker;
	}

	public void setEventListener(TransportListener listener) {
		this.listener = listener;
	}

	public void run() {
		running.set(true);
		int count = 0;
		log.trace("Transporter worker thread {} is in the air", Thread.currentThread().getId());
		log.trace("Transporter worker thread context:\tid:{}\tfrom: {}\tto: {}\tlistener: {}",
				Thread.currentThread().getId(), this.from, this.to, this.listener);
		while (running.get()) {
			try {
				try {
					log.trace("Transporter worker thread {} still up and running", Thread.currentThread().getId());
					RestApiClient restApiClient = RestApiClient.getInstance();

					Map<Long, RestApiClient.Entry> payloads = restApiClient.listFiles(from, false);
					if (payloads.size() == 0) {
						Thread.sleep(1000L);
						continue;
					}
					log.trace("Transporter worker thread {}: payloads cnt: {}", Thread.currentThread().getId(),
							payloads.size());
					for (Entry<Long, RestApiClient.Entry> payload : payloads.entrySet()) {
						String newname = UUID.randomUUID().toString();
						Long id = restApiClient.rename(to, payload.getValue().getPath(), newname);
						if (id != null && this.listener != null) {
							String newpath = this.to;
							if (!newpath.endsWith(SEP))
								newpath = newpath + SEP;
							newpath = newpath + newname;
							Map<String, String> attrs = restApiClient.attributes(newpath);
							RestApiClient.Entry newpayload = new RestApiClient.Entry(attrs);
							this.listener.onMove(id, newpayload);
						}
						Thread.sleep(1L);
						count += 1;
					}
					log.trace("Transporter worker thread {}: total payloads cnt: {}", Thread.currentThread().getId(),
							count);
				} catch (Exception e) {
					ExceptionUtils.log(log, e);
				}
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				log.trace("Transporter worker thread {} was interrupted", Thread.currentThread().getId());
			}
		}
		log.trace("Transporter worker thread {} has landed", Thread.currentThread().getId());
	}

	public abstract static class TransportListener implements TransportAdapter {

		@Override
		public abstract void onMove(Long id, RestApiClient.Entry payload);

	}
}
