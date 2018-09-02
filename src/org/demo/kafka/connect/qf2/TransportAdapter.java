// Kafka is licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
// Please note that any Qumulo Core REST API used in this demo is copyrighted to Qumulo, Inc.
// You can read more about Qumulo's API here: https://qumulo.github.io/
// CAUTION: THIS DEMO CODE IS NOT PRODUCTION READY

package org.demo.kafka.connect.qf2;

public interface TransportAdapter {

	void onMove(Long id, RestApiClient.Entry payload);

}
