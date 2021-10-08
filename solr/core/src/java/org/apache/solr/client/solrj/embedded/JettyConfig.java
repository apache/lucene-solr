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
package org.apache.solr.client.solrj.embedded;

import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.Filter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class JettyConfig {

  // by default jetty will start with http2 + http1 support
  public final boolean onlyHttp1;

  public final int port;

  public final String host;

  public final String context;

  public final boolean enableV2;

  public final boolean stopAtShutdown;
  
  public final Long waitForLoadingCoresToFinishMs;

  public final Map<ServletHolder, String> extraServlets;

  public final Map<Class<? extends Filter>, String> extraFilters;

  public final SSLConfig sslConfig;
  
  public final int portRetryTime;

  private JettyConfig(boolean onlyHttp1, int port, String host, int portRetryTime , String context, boolean stopAtShutdown,
                      Long waitForLoadingCoresToFinishMs, Map<ServletHolder, String> extraServlets,
                      Map<Class<? extends Filter>, String> extraFilters, SSLConfig sslConfig, boolean enableV2) {
    this.onlyHttp1 = onlyHttp1;
    this.port = port;
    this.host = host;
    this.context = context;
    this.stopAtShutdown = stopAtShutdown;
    this.waitForLoadingCoresToFinishMs = waitForLoadingCoresToFinishMs;
    this.extraServlets = extraServlets;
    this.extraFilters = extraFilters;
    this.sslConfig = sslConfig;
    this.portRetryTime = portRetryTime;
    this.enableV2 = enableV2;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(JettyConfig other) {
    Builder builder = new Builder();
    builder.port = other.port;
    builder.host = other.host;
    builder.context = other.context;
    builder.stopAtShutdown = other.stopAtShutdown;
    builder.extraServlets = other.extraServlets;
    builder.extraFilters = other.extraFilters;
    builder.sslConfig = other.sslConfig;
    return builder;
  }

  public static class Builder {

    boolean onlyHttp1 = false;
    int port = 0;
    String host = "127.0.0.1";
    String context = "/solr";
    boolean enableV2 = true;
    boolean stopAtShutdown = true;
    Long waitForLoadingCoresToFinishMs = 300000L;
    Map<ServletHolder, String> extraServlets = new TreeMap<>();
    Map<Class<? extends Filter>, String> extraFilters = new LinkedHashMap<>();
    SSLConfig sslConfig = null;
    int portRetryTime = 60;

    public Builder useOnlyHttp1(boolean useOnlyHttp1) {
      this.onlyHttp1 = useOnlyHttp1;
      return this;
    }
    public Builder enableV2(boolean flag){
      this.enableV2 = flag;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setContext(String context) {
      this.context = context;
      return this;
    }

    public Builder stopAtShutdown(boolean stopAtShutdown) {
      this.stopAtShutdown = stopAtShutdown;
      return this;
    }
    
    public Builder waitForLoadingCoresToFinish(Long waitForLoadingCoresToFinishMs) {
      this.waitForLoadingCoresToFinishMs = waitForLoadingCoresToFinishMs;
      return this;
    }

    public Builder withServlet(ServletHolder servlet, String pathSpec) {
      extraServlets.put(servlet, pathSpec);
      return this;
    }

    public Builder withServlets(Map<ServletHolder, String> servlets) {
      if (servlets != null)
        extraServlets.putAll(servlets);
      return this;
    }

    public Builder withFilter(Class<? extends Filter> filterClass, String pathSpec) {
      extraFilters.put(filterClass, pathSpec);
      return this;
    }

    public Builder withFilters(Map<Class<? extends Filter>, String> filters) {
      if (filters != null)
        extraFilters.putAll(filters);
      return this;
    }

    public Builder withSSLConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }
    
    public Builder withPortRetryTime(int portRetryTime) {
      this.portRetryTime = portRetryTime;
      return this;
    }


    public JettyConfig build() {
      return new JettyConfig(onlyHttp1, port, host, portRetryTime, context, stopAtShutdown,
          waitForLoadingCoresToFinishMs, extraServlets, extraFilters, sslConfig, enableV2);
    }

  }

}
