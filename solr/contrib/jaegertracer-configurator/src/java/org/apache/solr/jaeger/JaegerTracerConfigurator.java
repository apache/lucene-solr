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

package org.apache.solr.jaeger;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Tracer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.TracerConfigurator;

public class JaegerTracerConfigurator extends TracerConfigurator {
  public static final String AGENT_HOST = "agentHost";
  public static final String AGENT_PORT = "agentPort";
  public static final String LOG_SPANS = "logSpans";
  public static final String FLUSH_INTERVAL = "flushInterval";
  public static final String MAX_QUEUE_SIZE = "maxQueueSize";

  private volatile Tracer tracer;

  @Override
  public Tracer getTracer() {
    return tracer;
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    Object host = args.get(AGENT_HOST);
    if (!(host instanceof String)) {
      throw new IllegalArgumentException("Expected a required string for param '" + AGENT_HOST + "'");
    }

    Object portArg = args.get(AGENT_PORT);
    if (!(portArg instanceof Integer)) {
      throw new IllegalArgumentException("Expected a required int for param '" + AGENT_PORT + "'");
    }
    int port = (Integer) portArg;

    Boolean logSpans = args.getBooleanArg(LOG_SPANS);
    if (logSpans == null)
      logSpans = true;

    Object flushIntervalArg = args.get(FLUSH_INTERVAL);
    if (flushIntervalArg != null && !(flushIntervalArg instanceof Integer)) {
      throw new IllegalArgumentException("Expected a required int for param '" + FLUSH_INTERVAL +"'");
    }
    int flushInterval = flushIntervalArg == null ? 1000 : (Integer) flushIntervalArg;

    Object maxQueueArgs = args.get(MAX_QUEUE_SIZE);
    if (maxQueueArgs != null && !(maxQueueArgs instanceof Integer)) {
      throw new IllegalArgumentException("Expected a required int for param '" + MAX_QUEUE_SIZE +"'");
    }
    int maxQueue = maxQueueArgs == null ? 10000 : (Integer) maxQueueArgs;

    Configuration.SamplerConfiguration samplerConfig = new Configuration.SamplerConfiguration()
        .withType(ConstSampler.TYPE)
        .withParam(1);

    Configuration.ReporterConfiguration reporterConfig = Configuration.ReporterConfiguration.fromEnv();
    Configuration.SenderConfiguration senderConfig = reporterConfig.getSenderConfiguration()
        .withAgentHost(host.toString())
        .withAgentPort(port);

    reporterConfig.withLogSpans(logSpans)
        .withFlushInterval(flushInterval)
        .withMaxQueueSize(maxQueue)
        .withSender(senderConfig);
    tracer = new Configuration("solr")
        .withSampler(samplerConfig)
        .withReporter(reporterConfig)
        .getTracer();
  }


}
