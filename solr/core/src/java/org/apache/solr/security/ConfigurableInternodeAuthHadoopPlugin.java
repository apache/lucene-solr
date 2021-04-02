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
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientBuilderFactory;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.eclipse.jetty.client.api.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends {@linkplain HadoopAuthPlugin} by enabling configuration of
 * authentication mechanism for Solr internal communication.
 **/
public class ConfigurableInternodeAuthHadoopPlugin extends HadoopAuthPlugin implements HttpClientBuilderPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * A property specifying the {@linkplain HttpClientBuilderFactory} used for the Solr internal
   * communication.
   */
  private static final String HTTPCLIENT_BUILDER_FACTORY = "clientBuilderFactory";

  private static final String DO_AS = "doAs";

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
  public void setup(Http2SolrClient client) {
    factory.setup(client);
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

  @Override
  public boolean interceptInternodeRequest(HttpRequest httpRequest, HttpContext httpContext) {
    if (! (httpRequest instanceof HttpRequestWrapper)) {
      log.warn("Unable to add doAs to forwarded/distributed request - unknown request type");
      return false;
    }
    AtomicBoolean success = new AtomicBoolean(false);
    return intercept((key, value) -> {
      HttpRequestWrapper request = (HttpRequestWrapper) httpRequest;
      URIBuilder uriBuilder = new URIBuilder(request.getURI());
      uriBuilder.setParameter(key, value);
      try {
        request.setURI(uriBuilder.build());
        success.set(true);
      } catch (URISyntaxException e) {
        log.warn("Unable to add doAs to forwarded/distributed request - bad URI");
      }
    }) && success.get();
  }

  @Override
  protected boolean interceptInternodeRequest(Request request) {
    return intercept(request::param);
  }

  private boolean intercept(BiConsumer<String, String> setParam) {
    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    if (info != null && (info.getAction() == SolrDispatchFilter.Action.FORWARD ||
        info.getAction() == SolrDispatchFilter.Action.REMOTEQUERY)) {
      if (info.getUserPrincipal() != null) {
        String name = info.getUserPrincipal().getName();
        log.debug("Setting doAs={} to forwarded/remote request", name);
        setParam.accept(DO_AS, name);
        return true;
      }
    }
    return false;
  }
}
