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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.MockSearchableSolrClient;
import org.apache.solr.util.TimeOut;
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
@LogLevel("org.apache.solr.metrics.rrd=DEBUG")
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

  private RrdDef createDef(long startTime) {
    RrdDef def = new RrdDef("solr:foo", 60);
    def.setStartTime(startTime);
    def.addDatasource("one", DsType.COUNTER, 120, Double.NaN, Double.NaN);
    def.addDatasource("two", DsType.GAUGE, 120, Double.NaN, Double.NaN);
    def.addArchive(ConsolFun.AVERAGE, 0.5, 1, 120); // 2 hours
    def.addArchive(ConsolFun.AVERAGE, 0.5, 10, 288); // 48 hours
    def.addArchive(ConsolFun.AVERAGE, 0.5, 60, 336); // 2 weeks
    def.addArchive(ConsolFun.AVERAGE, 0.5, 240, 180); // 2 months
    return def;
  }

  @Test
  //commented 9-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 28-June-2018
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 6-Sep-2018
  public void testBasic() throws Exception {
    long startTime = 1000000000;
    RrdDb db = new RrdDb(createDef(startTime), factory);
    long lastNumUpdates = solrClient.getNumUpdates();
    List<Pair<String, Long>> list = factory.list(100);
    assertEquals(list.toString(), 1, list.size());
    assertEquals(list.toString(), "foo", list.get(0).first());

    timeSource.sleep(4000);
    lastNumUpdates = waitForUpdates(lastNumUpdates);

    // wait until updates stop coming - the first update could have been partial
    lastNumUpdates = waitForUpdatesToStop(lastNumUpdates);

    // there should be one sync data
    assertEquals(solrClient.docs.toString(), 1, solrClient.docs.size());
    String id = SolrRrdBackendFactory.ID_PREFIX + SolrRrdBackendFactory.ID_SEP + "foo";
    SolrInputDocument doc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    long timestamp = (Long)doc.getFieldValue("timestamp_l");

    timeSource.sleep(4000);

    SolrInputDocument newDoc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    assertEquals(newDoc.toString(), newDoc, doc);
    // make sure the update doesn't race with the sampling boundaries
    long lastTime = startTime + 30;
    // update the db
    Sample s = db.createSample();
    for (int i = 0; i < 100; i++) {
      s.setTime(lastTime);
      s.setValue("one", 1000 + i * 60);
      s.setValue("two", 100);
      s.update();
      lastTime = lastTime + 60;
    }
    timeSource.sleep(3000);
    lastNumUpdates = waitForUpdates(lastNumUpdates);

    newDoc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    assertFalse(newDoc.toString(), newDoc.equals(doc));
    long newTimestamp = (Long)newDoc.getFieldValue("timestamp_l");
    assertNotSame(newTimestamp, timestamp);
    // don't race with the sampling boundary
    FetchRequest fr = db.createFetchRequest(ConsolFun.AVERAGE, startTime + 20, lastTime - 20, 60);
    FetchData fd = fr.fetchData();
    int rowCount = fd.getRowCount();
    double[] one = fd.getValues("one");
    double[] two = fd.getValues("two");
    String dump = dumpData(db, fd);
    assertEquals("one: " + dump, 101, one.length);
    assertEquals(dump, Double.NaN, one[0], 0.00001);
    assertEquals(dump, Double.NaN, one[100], 0.00001);
    for (int i = 1; i < 100; i++) {
      assertEquals(dump + "\npos=" + i, 1.0, one[i], 0.00001);
    }
    assertEquals("two: " + dump, Double.NaN, two[100], 0.00001);
    for (int i = 0; i < 100; i++) {
      assertEquals(dump + "\ntwo pos=" + i, 100.0, two[i], 0.00001);
    }
    db.close();

    // should still be listed
    list = factory.list(100);
    assertEquals(list.toString(), 1, list.size());
    assertEquals(list.toString(), "foo", list.get(0).first());

    lastNumUpdates = solrClient.getNumUpdates();

    // re-open read-write
    db = new RrdDb("solr:foo", factory);
    s = db.createSample();
    s.setTime(lastTime);
    s.setValue("one", 7000);
    s.setValue("two", 100);
    s.update();
    timeSource.sleep(3000);
    lastNumUpdates = waitForUpdates(lastNumUpdates);

    // should update
    timestamp = newTimestamp;
    doc = newDoc;
    newDoc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    assertFalse(newDoc.toString(), newDoc.equals(doc));
    newTimestamp = (Long)newDoc.getFieldValue("timestamp_l");
    assertNotSame(newTimestamp, timestamp);
    fr = db.createFetchRequest(ConsolFun.AVERAGE, startTime + 20, lastTime + 20, 60);
    fd = fr.fetchData();
    dump = dumpData(db, fd);
    rowCount = fd.getRowCount();
    one = fd.getValues("one");
    assertEquals("one: " + dump, 102, one.length);
    assertEquals(dump, Double.NaN, one[0], 0.01);
    assertEquals(dump, Double.NaN, one[101], 0.01);
    for (int i = 1; i < 101; i++) {
      assertEquals(dump, 1.0, one[i], 0.01);
    }
    two = fd.getValues("two");
    assertEquals("two: " + dump, Double.NaN, two[101], 0.001);
    for (int i = 1; i < 101; i++) {
      assertEquals(dump, 100.0, two[i], 0.001);
    }

    db.close();

    // open a read-only version of the db
    RrdDb readOnly = new RrdDb("solr:foo", true, factory);
    s = readOnly.createSample();
    s.setTime(lastTime + 120);
    s.setValue("one", 10000001);
    s.setValue("two", 100);
    s.update();
    // these updates should not be synced
    timeSource.sleep(3000);
    doc = newDoc;
    timestamp = newTimestamp;
    newDoc = solrClient.docs.get(CollectionAdminParams.SYSTEM_COLL).get(id);
    assertTrue(newDoc.toString(), newDoc.equals(doc));
    newTimestamp = (Long)newDoc.getFieldValue("timestamp_l");
    assertEquals(newTimestamp, timestamp);
    readOnly.close();
  }

  private String dumpData(RrdDb db, FetchData fd) throws Exception {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("dbLastUpdateTime", db.getLastUpdateTime());
    map.put("firstTimestamp", fd.getFirstTimestamp());
    map.put("lastTimestamp", fd.getLastTimestamp());
    map.put("timestamps", fd.getTimestamps());
    map.put("data", fd.dump());
    return Utils.toJSONString(map);
  }

  private long waitForUpdates(long lastNumUpdates) throws Exception {
    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, timeSource);
    while (!timeOut.hasTimedOut()) {
      timeOut.sleep(1000);
      if (solrClient.getNumUpdates() > lastNumUpdates) {
        return solrClient.getNumUpdates();
      }
    }
    if (solrClient.getNumUpdates() > lastNumUpdates) {
      return solrClient.getNumUpdates();
    }
    throw new Exception("time out waiting for updates");
  }


  private long waitForUpdatesToStop(long lastNumUpdates) throws Exception {
    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, timeSource);
    int stopped = 0;
    while (!timeOut.hasTimedOut()) {
      timeOut.sleep(1000);
      if (solrClient.getNumUpdates() > lastNumUpdates) {
        stopped = 0;
        lastNumUpdates = solrClient.getNumUpdates();
        continue;
      } else {
        stopped++;
        if (stopped > 2) {
          return lastNumUpdates;
        }
      }
      timeOut.sleep(1000);
    }
    throw new Exception("time out waiting for updates");
  }


}
