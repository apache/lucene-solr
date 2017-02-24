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
package org.apache.solr.security;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.solr.client.solrj.impl.HttpClientBuilderFactory;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.core.CoreContainer;

/**
 * This class extends {@linkplain HadoopAuthPlugin} by enabling configuration of
 * authentication mechanism for Solr internal communication.
 **/
public class ConfigurableInternodeAuthHadoopPlugin extends HadoopAuthPlugin implements HttpClientBuilderPlugin {

  /**
   * A property specifying the {@linkplain HttpClientBuilderFactory} used for the Solr internal
   * communication.
   */
  private static final String HTTPCLIENT_BUILDER_FACTORY = "clientBuilderFactory";

  private HttpClientBuilderFactory factory = null;

  public ConfigurableInternodeAuthHadoopPlugin(CoreContainer coreContainer) {
    super(coreContainer);
  }

  @Override
  public void init(Map<String,Object> pluginConfig) {
    super.init(pluginConfig);

    String httpClientBuilderFactory = (String)Objects.requireNonNull(pluginConfig.get(HTTPCLIENT_BUILDER_FACTORY),
        "Please specify clientBuilderFactory to be used for Solr internal communication.");
    factory = this.coreContainer.getResourceLoader().newInstance(httpClientBuilderFactory, HttpClientBuilderFactory.class);
  }

  @Override
  public SolrHttpClientBuilder getHttpClientBuilder(SolrHttpClientBuilder builder) {
    return factory.getHttpClientBuilder(Optional.ofNullable(builder));
  }

  @Override
  public void close() throws IOException {
    super.close();

    if (factory != null) {
      factory.close();
    }
  }
}
