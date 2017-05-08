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
package org.apache.solr.response;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.request.SolrQueryRequest;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

public class PrometheusMetricsWriter extends BinaryResponseWriter {

  @Override
  public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response) throws IOException {
    Writer writer = new OutputStreamWriter(out);
    Object collectorRegistry = response.getValues().get(MetricsHandler.PROMETHEUS_METRICS_WT);
    
    if(null == collectorRegistry || !(collectorRegistry instanceof CollectorRegistry)) {
      throw new SolrException(ErrorCode.INVALID_STATE, "I was expecting a CollectorRegistry but got null or something else");
    }
    
    TextFormat.write004(writer, ((CollectorRegistry)collectorRegistry).metricFamilySamples());
    
    writer.flush();
    writer.close();
  }
  
  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return TextFormat.CONTENT_TYPE_004;  
  }
  
}
