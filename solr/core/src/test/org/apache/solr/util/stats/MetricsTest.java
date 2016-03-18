package org.apache.solr.util.stats;

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

import java.io.IOException;

import com.codahale.metrics.SharedMetricRegistries;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;
import com.codahale.metrics.Timer;

public class MetricsTest   extends LuceneTestCase {

  @Test
  public void testSharedRegistryUse() throws IOException {

    Timer t1 = Metrics.namedTimer("timer1");
    assertTrue(SharedMetricRegistries.names().contains(Metrics.DEFAULT_REGISTRY));
    assertTrue(SharedMetricRegistries.getOrCreate(Metrics.DEFAULT_REGISTRY).getNames().contains("timer1"));
    SharedMetricRegistries.getOrCreate(Metrics.DEFAULT_REGISTRY).remove("timer1");

    Timer t2 = Metrics.namedTimer("timer2", "test");
    assertTrue(
        SharedMetricRegistries.getOrCreate(Metrics.mkName(Metrics.REGISTRY_NAME_PREFIX, "test"))
            .getNames().contains("timer2")
    );
    SharedMetricRegistries.getOrCreate(Metrics.mkName(Metrics.REGISTRY_NAME_PREFIX, "test")).remove("timer2");
  }

  @Test
  public void testOverridableSharedRegistryNaming() throws IOException {
     try {
       System.setProperty("solr.registry.default", "override1");
       System.setProperty("solr.registry.test", "solr.registry.override1");

       Timer t1 = Metrics.namedTimer("timer10");
       Timer t2 = Metrics.namedTimer("timer11", "test");

       assertTrue(
           SharedMetricRegistries.getOrCreate(Metrics.mkName(Metrics.REGISTRY_NAME_PREFIX, "override1"))
               .getNames().contains("timer10")
       );
       assertTrue(
           SharedMetricRegistries.getOrCreate(Metrics.mkName(Metrics.REGISTRY_NAME_PREFIX, "override1"))
               .getNames().contains("timer11")
       );
       SharedMetricRegistries.getOrCreate(Metrics.mkName(Metrics.REGISTRY_NAME_PREFIX, "override1")).remove("timer10");
       SharedMetricRegistries.getOrCreate(Metrics.mkName(Metrics.REGISTRY_NAME_PREFIX, "override1")).remove("timer11");

     }
     finally {
       System.clearProperty("solr.registry.default");
       System.clearProperty("solr.registry.test");
     }
  }

}
