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
package org.apache.solr.morphlines.solr;

import java.io.File;
import java.util.Iterator;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.Test;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
@Slow
public class SolrMorphlineZkAliasTest extends AbstractSolrMorphlineZkTestBase {

  @Test
  public void test() throws Exception {

    CollectionAdminRequest.createAlias("aliascollection", "collection1")
        .process(cluster.getSolrClient());
    
    morphline = parse("test-morphlines" + File.separator + "loadSolrBasic", "aliascollection");
    Record record = new Record();
    record.put(Fields.ID, "id0-innsbruck");
    record.put("text", "mytext");
    record.put("user_screen_name", "foo");
    record.put("first_name", "Nadja"); // will be sanitized
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    Notifications.notifyBeginTransaction(morphline);
    assertTrue(morphline.process(record));
    
    record = new Record();
    record.put(Fields.ID, "id1-innsbruck");
    record.put("text", "mytext1");
    record.put("user_screen_name", "foo1");
    record.put("first_name", "Nadja1"); // will be sanitized
    assertTrue(morphline.process(record));
    
    Record expected = new Record();
    expected.put(Fields.ID, "id0-innsbruck");
    expected.put("text", "mytext");
    expected.put("user_screen_name", "foo");
    Iterator<Record> citer = collector.getRecords().iterator();
    assertEquals(expected, citer.next());
    
    Record expected2 = new Record();
    expected2.put(Fields.ID, "id1-innsbruck");
    expected2.put("text", "mytext1");
    expected2.put("user_screen_name", "foo1");
    assertEquals(expected2, citer.next());
    
    assertFalse(citer.hasNext());
    
    Notifications.notifyCommitTransaction(morphline);
    new UpdateRequest().commit(cluster.getSolrClient(), COLLECTION);
    
    QueryResponse rsp = cluster.getSolrClient()
        .query(COLLECTION, new SolrQuery("*:*").setRows(100000).addSort(Fields.ID, SolrQuery.ORDER.asc));
    //System.out.println(rsp);
    Iterator<SolrDocument> iter = rsp.getResults().iterator();
    assertEquals(expected.getFields(), next(iter));
    assertEquals(expected2.getFields(), next(iter));
    assertFalse(iter.hasNext());
    
    Notifications.notifyRollbackTransaction(morphline);
    Notifications.notifyShutdown(morphline);

  }

}
