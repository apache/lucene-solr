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

import java.util.List;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.DOMUtil;
import org.w3c.dom.Node;

public class PrometheusExporterSettings {

  private final int httpConnectionTimeout;
  private final int httpReadTimeout;

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int httpConnectionTimeout = 10000;
    private int httpReadTimeout = 60000;

    private Builder() {

    }

    public Builder withConnectionHttpTimeout(int httpConnectionTimeout) {
      this.httpConnectionTimeout = httpConnectionTimeout;
      return this;
    }

    public Builder witReadHttpTimeout(int httpReadTimeout) {
      this.httpReadTimeout = httpReadTimeout;
      return this;
    }

    public PrometheusExporterSettings build() {
      return new PrometheusExporterSettings(httpConnectionTimeout, httpReadTimeout);
    }

  }

  public static PrometheusExporterSettings from(Node settings) {
    @SuppressWarnings({"rawtypes"})
    NamedList config = DOMUtil.childNodesToNamedList(settings);

    Builder builder = builder();

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<NamedList> httpClientSettings = config.getAll("httpClients");

    for (@SuppressWarnings({"rawtypes"})NamedList entry : httpClientSettings) {
      Integer connectionTimeout = (Integer) entry.get("connectionTimeout");
      if (connectionTimeout != null) {
        builder.withConnectionHttpTimeout(connectionTimeout);
      }

      Integer readTimeout = (Integer) entry.get("readTimeout");
      if (readTimeout != null) {
        builder.witReadHttpTimeout(readTimeout);
      }
    }

    return builder.build();
  }

  private PrometheusExporterSettings(
      int httpConnectionTimeout,
      int httpReadTimeout) {
    this.httpConnectionTimeout = httpConnectionTimeout;
    this.httpReadTimeout = httpReadTimeout;
  }

  public int getHttpConnectionTimeout() {
    return httpConnectionTimeout;
  }

  public int getHttpReadTimeout() {
    return httpReadTimeout;
  }

}
