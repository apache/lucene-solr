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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.tracing.GlobalTracer;
import org.junit.Test;

import static org.apache.solr.jaeger.JaegerTracerConfigurator.AGENT_HOST;
import static org.apache.solr.jaeger.JaegerTracerConfigurator.AGENT_PORT;
import static org.apache.solr.jaeger.JaegerTracerConfigurator.FLUSH_INTERVAL;
import static org.apache.solr.jaeger.JaegerTracerConfigurator.LOG_SPANS;
import static org.apache.solr.jaeger.JaegerTracerConfigurator.MAX_QUEUE_SIZE;

public class TestJaegerConfigurator extends SolrTestCaseJ4 {

  @Test
  public void testInjected() throws Exception{
    MiniSolrCloudCluster cluster = new SolrCloudTestCase.Builder(2, createTempDir())
        .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
        .withSolrXml(getFile("solr/solr.xml").toPath())
        .build();
    CollectionAdminRequest.setClusterProperty(ZkStateReader.SAMPLE_PERCENTAGE, "100.0")
        .process(cluster.getSolrClient());
    try {
      TimeOut timeOut = new TimeOut(2, TimeUnit.MINUTES, TimeSource.NANO_TIME);
      timeOut.waitFor("Waiting for GlobalTracer is registered", () -> GlobalTracer.getTracer() instanceof io.jaegertracing.internal.JaegerTracer);

      //TODO add run Jaeger through Docker and verify spans available after run these commands
      CollectionAdminRequest.createCollection("test", 2, 1).process(cluster.getSolrClient());
      new UpdateRequest()
          .add("id", "1")
          .add("id", "2")
          .process(cluster.getSolrClient(), "test");
      cluster.getSolrClient().query("test", new SolrQuery("*:*"));
    } finally {
      cluster.shutdown();
    }

  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testRequiredParameters() throws IOException {
    JaegerTracerConfigurator configurator = new JaegerTracerConfigurator();
    @SuppressWarnings({"rawtypes"})
    NamedList initArgs = new NamedList();
    IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> configurator.init(initArgs));
    assertTrue(exc.getMessage().contains(AGENT_HOST) || exc.getMessage().contains(AGENT_PORT));
    initArgs.add(AGENT_HOST, "localhost");

    exc = expectThrows(IllegalArgumentException.class, () -> configurator.init(initArgs));
    assertTrue(exc.getMessage().contains(AGENT_PORT));
    initArgs.add(AGENT_PORT, 5775);

    // no exception should be thrown
    configurator.init(initArgs);
    ((Closeable)configurator.getTracer()).close();

    initArgs.add(LOG_SPANS, true);
    initArgs.add(FLUSH_INTERVAL, 1000);
    initArgs.add(MAX_QUEUE_SIZE, 10000);
    configurator.init(initArgs);
    ((Closeable)configurator.getTracer()).close();
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testIncorrectFormat() {
    JaegerTracerConfigurator configurator = new JaegerTracerConfigurator();
    @SuppressWarnings({"rawtypes"})
    NamedList initArgs = new NamedList();
    initArgs.add(AGENT_HOST, 100);
    initArgs.add(AGENT_PORT, 5775);

    IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> configurator.init(initArgs));
    assertTrue(exc.getMessage().contains(AGENT_HOST));

    initArgs.clear();
    initArgs.add(AGENT_HOST, "localhost");
    initArgs.add(AGENT_PORT, "5775");
    exc = expectThrows(IllegalArgumentException.class, () -> configurator.init(initArgs));
    assertTrue(exc.getMessage().contains(AGENT_PORT));

    initArgs.clear();
    initArgs.add(AGENT_HOST, "localhost");
    initArgs.add(AGENT_PORT, 5775);
    initArgs.add(LOG_SPANS, 10);
    SolrException solrExc = expectThrows(SolrException.class, () -> configurator.init(initArgs));
    assertTrue(solrExc.getMessage().contains(LOG_SPANS));

    initArgs.clear();
    initArgs.add(AGENT_HOST, "localhost");
    initArgs.add(AGENT_PORT, 5775);
    initArgs.add(FLUSH_INTERVAL, "10");
    exc = expectThrows(IllegalArgumentException.class, () -> configurator.init(initArgs));
    assertTrue(exc.getMessage().contains(FLUSH_INTERVAL));

    initArgs.clear();
    initArgs.add(AGENT_HOST, "localhost");
    initArgs.add(AGENT_PORT, 5775);
    initArgs.add(MAX_QUEUE_SIZE, "10");
    exc = expectThrows(IllegalArgumentException.class, () -> configurator.init(initArgs));
    assertTrue(exc.getMessage().contains(MAX_QUEUE_SIZE));

  }
}
