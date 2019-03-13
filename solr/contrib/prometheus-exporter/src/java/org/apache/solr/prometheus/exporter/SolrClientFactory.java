/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.prometheus.exporter;

import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.zookeeper.client.ConnectStringParser;

public class SolrClientFactory {

  private PrometheusExporterSettings settings;

  public SolrClientFactory(PrometheusExporterSettings settings) {
    this.settings = settings;
  }

  public HttpSolrClient createStandaloneSolrClient(String solrHost) {
    NoOpResponseParser responseParser = new NoOpResponseParser();
    responseParser.setWriterType("json");

    HttpSolrClient.Builder standaloneBuilder = new HttpSolrClient.Builder();

    standaloneBuilder.withBaseSolrUrl(solrHost);

    standaloneBuilder.withConnectionTimeout(settings.getHttpConnectionTimeout())
        .withSocketTimeout(settings.getHttpReadTimeout());

    HttpSolrClient httpSolrClient = standaloneBuilder.build();
    httpSolrClient.setParser(responseParser);

    return httpSolrClient;
  }

  public CloudSolrClient createCloudSolrClient(String zookeeperConnectionString) {
    NoOpResponseParser responseParser = new NoOpResponseParser();
    responseParser.setWriterType("json");

    ConnectStringParser parser = new ConnectStringParser(zookeeperConnectionString);

    CloudSolrClient.Builder cloudBuilder = new CloudSolrClient.Builder(
        parser.getServerAddresses().stream()
            .map(address -> String.format(Locale.ROOT, "%s:%s", address.getHostString(), address.getPort()))
            .collect(Collectors.toList()),
        Optional.ofNullable(parser.getChrootPath()));

    cloudBuilder.withConnectionTimeout(settings.getHttpConnectionTimeout())
        .withSocketTimeout(settings.getHttpReadTimeout());

    CloudSolrClient client = cloudBuilder.build();
    client.setParser(responseParser);

    client.connect();

    return client;
  }


}
