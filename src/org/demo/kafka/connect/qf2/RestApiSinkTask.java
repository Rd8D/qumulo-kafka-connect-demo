// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.connect.qf2;

import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.demo.kafka.commons.qf2.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestApiSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(RestApiSinkTask.class);

	private static final String SEP = "/";

	private static final String LF = "\n";

	private String dir;

	private String batch;

	private RestApiClient restApiClient;

	public RestApiSinkTask() {
	}

	@Override
	public String version() {
		return new RestApiSinkConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		this.dir = props.get(RestApiSinkConnector.DIR_CONFIG);
		if (this.dir == null || this.dir.isEmpty())
			throw new ConnectException("RestApiSinkTask config missing directory setting");
		StringUtils.stripEnd(this.dir, SEP);
		if (!this.dir.startsWith(SEP))
			this.dir = SEP + this.dir;
		try {
			log.info(props.toString());
			this.restApiClient = RestApiClient.getInstance(props);
			this.restApiClient.createDirectory(this.dir);
			this.batch = UUID.randomUUID().toString();
			this.restApiClient.writeFile(this.dir, this.batch, new String("# " + new Date() + LF + "#" + LF).getBytes(),
					false);
		} catch (Throwable t) {
			throw new ConnectException(t);
		}
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		for (SinkRecord record : sinkRecords) {
			log.info("New payload: key:{} offset:{} size:{}", record.key().toString(),
					Long.toString(record.kafkaOffset()), record.value().toString().length());
			try {
				this.restApiClient.patchFile(this.dir + SEP + this.batch,
						(new String(Base64.getDecoder().decode(record.value().toString())) + LF).getBytes());
				Thread.sleep(0L);
			} catch (Throwable t) {
				ExceptionUtils.log(log, t);
			}
		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		// Nothing much to get done here
	}

	@Override
	public void stop() {
		// Nothing much to get done here
	}

}
