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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
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
public class SolrMorphlineZkAvroTest extends AbstractSolrMorphlineZkTestBase {


  @Test
  public void test() throws Exception {

    Path avro = Paths.get(RESOURCES_DIR).resolve("test-documents").resolve("sample-statuses-20120906-141433-medium.avro");

    // load avro records via morphline and zk into solr
    morphline = parse("test-morphlines" + File.separator + "tutorialReadAvroContainer");    
    Record record = new Record();
    byte[] body = Files.readAllBytes(avro);
    record.put(Fields.ATTACHMENT_BODY, body);
    startSession();
    Notifications.notifyBeginTransaction(morphline);
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getNumStartEvents());
    
    Notifications.notifyCommitTransaction(morphline);
    new UpdateRequest().commit(cluster.getSolrClient(), COLLECTION);
    
    // fetch sorted result set from solr
    QueryResponse rsp = cluster.getSolrClient()
        .query(COLLECTION, new SolrQuery("*:*").setRows(100000).addSort("id", SolrQuery.ORDER.asc));
    assertEquals(2104, collector.getRecords().size());
    assertEquals(collector.getRecords().size(), rsp.getResults().size());
    
    Collections.sort(collector.getRecords(), (r1, r2) -> r1.get("id").toString().compareTo(r2.get("id").toString()));

    // fetch test input data and sort like solr result set
    List<GenericData.Record> records = new ArrayList<>();
    FileReader<GenericData.Record> reader = new DataFileReader(avro.toFile(), new GenericDatumReader());
    while (reader.hasNext()) {
      GenericData.Record expected = reader.next();
      records.add(expected);
    }
    assertEquals(collector.getRecords().size(), records.size());    
    Collections.sort(records, (r1, r2) -> r1.get("id").toString().compareTo(r2.get("id").toString()));
    
    Object lastId = null;
    for (int i = 0; i < records.size(); i++) {  
      //System.out.println("myrec" + i + ":" + records.get(i));      
      Object id = records.get(i);
      if (id != null && id.equals(lastId)) {
        throw new IllegalStateException("Detected duplicate id. Test input data must not contain duplicate ids!");        
      }
      lastId = id;
    }
    
    for (int i = 0; i < records.size(); i++) {  
      //System.out.println("myrsp" + i + ":" + rsp.getResults().get(i));      
    }    

    Iterator<SolrDocument> rspIter = rsp.getResults().iterator();
    for (int i = 0; i < records.size(); i++) {  
      // verify morphline spat out expected data
      Record actual = collector.getRecords().get(i);
      GenericData.Record expected = Objects.requireNonNull(records.get(i));
      assertTweetEquals(expected, actual, i);
      
      // verify Solr result set contains expected data
      actual = new Record();
      actual.getFields().putAll(next(rspIter));
      assertTweetEquals(expected, actual, i);
    }
    
    Notifications.notifyRollbackTransaction(morphline);
    Notifications.notifyShutdown(morphline);

  }
  
  private void assertTweetEquals(GenericData.Record expected, Record actual, int i) {
    Objects.requireNonNull(expected);
    Objects.requireNonNull(actual);
//    System.out.println("\n\nexpected: " + toString(expected));
//    System.out.println("actual:   " + actual);
    String[] fieldNames = new String[] { 
        "id", 
        "in_reply_to_status_id", 
        "in_reply_to_user_id", 
        "retweet_count",
        "text", 
        };
    for (String fieldName : fieldNames) {
      assertEquals(
          i + " fieldName: " + fieldName, 
          expected.get(fieldName).toString(), 
          actual.getFirstValue(fieldName).toString());
    }
  }

}
