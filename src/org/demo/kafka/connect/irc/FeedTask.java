/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.demo.kafka.connect.irc;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventAdapter;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeedTask extends SourceTask {

	private static final Logger log = LoggerFactory.getLogger(FeedTask.class);

	private static final Schema KEY_SCHEMA = Schema.INT64_SCHEMA;
	
	private static final String TIMESTAMP_FIELD = "timestamp";
	
	private static final String CHANNEL_FIELD = "channel";

	private BlockingQueue<SourceRecord> queue = null;
	
	private String[] channels = null;
	
	private String topic = null;
	
	private String host;
	
	private int port;
	
	private IRCConnection conn;

	@Override
	public String version() {
		return new FeedSourceConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		this.queue = new LinkedBlockingQueue<>();
		this.host = props.get(FeedSourceConnector.IRC_HOST_CONFIG);
		this.port = Integer.parseInt(props.get(FeedSourceConnector.IRC_PORT_CONFIG));
		this.channels = props.get(FeedSourceConnector.IRC_CHANNELS_CONFIG).split(",");
		this.topic = props.get(FeedSourceConnector.TOPIC_CONFIG);

		String nick = "kafka-connect-irc-" + Math.abs(new Random().nextInt());
		log.info("Starting irc feed task " + nick + ", channels " + props.get(FeedSourceConnector.IRC_CHANNELS_CONFIG));
		this.conn = new IRCConnection(host, new int[] { port }, "", nick, nick, nick);
		this.conn.addIRCEventListener(new IRCMessageListener());
		this.conn.setEncoding("UTF-8");
		this.conn.setPong(true);
		this.conn.setColors(false);
		try {
			this.conn.connect();
		} catch (IOException e) {
			throw new RuntimeException("Unable to connect to " + host + ":" + port + ".", e);
		}
		for (String channel : channels) {
			this.conn.send("JOIN " + channel);
		}

	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		List<SourceRecord> result = new LinkedList<>();
		if (this.queue.isEmpty())
			result.add(this.queue.take());
		this.queue.drainTo(result);
		return result;
	}

	@Override
	public void stop() {
		for (String channel : channels) {
			try {
				this.conn.send("PART " + channel);
			} catch (Throwable e) {
				log.warn("Problem leaving channel ", e);
			}
		}
		this.conn.interrupt();
		try {
			this.conn.join();
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted while trying to shutdown IRC connection for " + host + ":" + port,
					e);
		}

		if (this.conn.isAlive()) {
			throw new RuntimeException("Unable to shutdown IRC connection for " + host + ":" + port);
		}
		this.queue.clear();
	}

	class IRCMessageListener extends IRCEventAdapter {
		@Override
		public void onPrivmsg(String channel, IRCUser user, String raw) {
			WikipediaMessage event = null;
			try {
				event = WikipediaMessage.parseMessage(channel, raw);
				log.trace(event.toString());
				Long ts = System.currentTimeMillis();
				Map<String, ?> srcOffset = Collections.singletonMap(TIMESTAMP_FIELD, ts);
				Map<String, ?> srcPartition = Collections.singletonMap(CHANNEL_FIELD, channel);
				SourceRecord record = new SourceRecord(srcPartition, srcOffset, topic, KEY_SCHEMA, ts,
						WikipediaMessage.SCHEMA, event);
				queue.offer(record);
			} catch (Exception e) {
				log.error("Cannot parse: {}", raw);
			}
		}

		@Override
		public void onError(String msg) {
			log.warn("IRC Error: " + msg);
		}
	}

}
