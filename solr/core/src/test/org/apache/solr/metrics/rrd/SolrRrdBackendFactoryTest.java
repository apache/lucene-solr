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

package org.apache.solr.metrics.rrd;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.MockSearchableSolrClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.FetchData;
import org.rrd4j.core.FetchRequest;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.Sample;

/**
 *
 */
public class SolrRrdBackendFactoryTest extends SolrTestCaseJ4 {

  private SolrRrdBackendFactory factory;
  private MockSearchableSolrClient solrClient;
  private TimeSource timeSource;

  @Before
  public void setup() {
    solrClient = new MockSearchableSolrClient();
    if (random().nextBoolean()) {
      timeSource = TimeSource.NANO_TIME;
    } else {
      timeSource = TimeSource.get("simTime:50");
    }
    factory = new SolrRrdBackendFactory(solrClient, CollectionAdminParams.SYSTEM_COLL, 1, timeSource);
  }

  @After
  public void teardown() throws Exception {
    if (factory != null) {
      factory.close();
    }
  }

  private RrdDef createDef() {
    RrdDef def = new RrdDef("solr:foo", 60);
    def.addDatasource("one", DsType.COUNTER, 120, Double.NaN, Double.NaN);
    def.addDatasource("two", DsType.GAUGE, 120, Double.NaN, Double.NaN);
    def.addArchive(ConsolFun.AVERAGE, 0.5, 1, 120); // 2 hours
    def.addArchive(ConsolFun.AVERAGE, 0.5, 10, 288); // 48 hours
    def.addArchive(ConsolFun.AVERAGE, 0.5, 60, 336); // 2 weeks
    def.addArchive(ConsolFun.AVERAGE, 0.5, 240, 180); // 2 months
    return def;
  }

  @Test
  public void testBasic() throws Exception {
    RrdDb db = new RrdDb(createDef(), factory);
    List<String> list = factory.list(100);
    assertEquals(list.toString(), 1, list.size());
    assertEquals(list.toString(), "foo", list.get(0));
    timeSource.sleep(2000);
    // there should be one sync data
    assertEquals(solrClient.docs.toString(), 1, solrClient.docs.size());
    String id = SolrRrdBackendFactory.ID_PREFIX + SolrRrdBackendFactory.ID_SEP + "foo";
    SolrInputDocument doc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    long timestamp = ((Date)doc.getFieldValue("timestamp")).getTime();
    timeSource.sleep(2000);
    SolrInputDocument newDoc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    assertEquals(newDoc.toString(), newDoc, doc);
    long firstTimestamp = TimeUnit.SECONDS.convert(timestamp, TimeUnit.MILLISECONDS);
    long lastTimestamp = firstTimestamp + 60;
    // update the db
    Sample s = db.createSample();
    for (int i = 0; i < 100; i++) {
      s.setTime(lastTimestamp);
      s.setValue("one", 1000 + i * 60);
      s.setValue("two", 100);
      s.update();
      lastTimestamp = lastTimestamp + 60;
    }
    timeSource.sleep(3000);
    newDoc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    assertFalse(newDoc.toString(), newDoc.equals(doc));
    long newTimestamp = ((Date)newDoc.getFieldValue("timestamp")).getTime();
    assertNotSame(newTimestamp, timestamp);
    FetchRequest fr = db.createFetchRequest(ConsolFun.AVERAGE, firstTimestamp + 60, lastTimestamp - 60, 60);
    FetchData fd = fr.fetchData();
    int rowCount = fd.getRowCount();
    double[] one = fd.getValues("one");
    assertEquals("one", 101, one.length);
    assertEquals(Double.NaN, one[0], 0.00001);
    assertEquals(Double.NaN, one[100], 0.00001);
    for (int i = 1; i < 100; i++) {
      assertEquals(1.0, one[i], 0.00001);
    }
    double[] two = fd.getValues("two");
    assertEquals(Double.NaN, two[100], 0.00001);
    for (int i = 1; i < 100; i++) {
      assertEquals("wrong value at pos " + i, 100.0, two[i], 0.00001);
    }
    db.close();

    // should still be listed
    list = factory.list(100);
    assertEquals(list.toString(), 1, list.size());
    assertEquals(list.toString(), "foo", list.get(0));

    // re-open read-write
    db = new RrdDb("solr:foo", factory);
    s = db.createSample();
    s.setTime(lastTimestamp);
    s.setValue("one", 7000);
    s.setValue("two", 100);
    s.update();
    timeSource.sleep(3000);
    // should update
    timestamp = newTimestamp;
    doc = newDoc;
    newDoc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    assertFalse(newDoc.toString(), newDoc.equals(doc));
    newTimestamp = ((Date)newDoc.getFieldValue("timestamp")).getTime();
    assertNotSame(newTimestamp, timestamp);
    fr = db.createFetchRequest(ConsolFun.AVERAGE, firstTimestamp + 60, lastTimestamp, 60);
    fd = fr.fetchData();
    rowCount = fd.getRowCount();
    one = fd.getValues("one");
    assertEquals("one", 102, one.length);
    assertEquals(Double.NaN, one[0], 0.00001);
    assertEquals(Double.NaN, one[101], 0.00001);
    for (int i = 1; i < 101; i++) {
      assertEquals(1.0, one[i], 0.00001);
    }
    two = fd.getValues("two");
    assertEquals(Double.NaN, two[101], 0.00001);
    for (int i = 1; i < 101; i++) {
      assertEquals(100.0, two[i], 0.00001);
    }

    db.close();

    // open a read-only version of the db
    RrdDb readOnly = new RrdDb("solr:foo", true, factory);
    s = readOnly.createSample();
    s.setTime(lastTimestamp + 120);
    s.setValue("one", 10000001);
    s.setValue("two", 100);
    s.update();
    // these updates should not be synced
    timeSource.sleep(3000);
    doc = newDoc;
    timestamp = newTimestamp;
    newDoc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    assertTrue(newDoc.toString(), newDoc.equals(doc));
    newTimestamp = ((Date)newDoc.getFieldValue("timestamp")).getTime();
    assertEquals(newTimestamp, timestamp);
    readOnly.close();
  }

}
