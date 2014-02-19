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
import java.util.Arrays;

import org.apache.lucene.util.Constants;
import org.junit.BeforeClass;
import org.junit.Test;

import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

public class SolrMorphlineTest extends AbstractSolrMorphlineTestBase {

  @BeforeClass
  public static void beforeClass2() {
    assumeFalse("FIXME: This test fails under Java 8 due to the Saxon dependency - see SOLR-1301", Constants.JRE_IS_MINIMUM_JAVA8);
    assumeFalse("FIXME: This test fails under J9 due to the Saxon dependency - see SOLR-1301", System.getProperty("java.vm.info", "<?>").contains("IBM J9"));
  }
  
  @Test
  public void testLoadSolrBasic() throws Exception {
    //System.setProperty("ENV_SOLR_HOME", testSolrHome + "/collection1");
    morphline = createMorphline("test-morphlines/loadSolrBasic");    
    //System.clearProperty("ENV_SOLR_HOME");
    Record record = new Record();
    record.put(Fields.ID, "id0");
    record.put("first_name", "Nadja"); // will be sanitized
    startSession();
    Notifications.notifyBeginTransaction(morphline);
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getNumStartEvents());
    Notifications.notifyCommitTransaction(morphline);
    Record expected = new Record();
    expected.put(Fields.ID, "id0");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertEquals(1, queryResultSetSize("*:*"));
    Notifications.notifyRollbackTransaction(morphline);
    Notifications.notifyShutdown(morphline);
  }
    
  @Test
  public void testTokenizeText() throws Exception {
    morphline = createMorphline("test-morphlines" + File.separator + "tokenizeText");
    for (int i = 0; i < 3; i++) {
      Record record = new Record();
      record.put(Fields.MESSAGE, "Hello World!");
      record.put(Fields.MESSAGE, "\nFoo@Bar.com #%()123");
      Record expected = record.copy();
      expected.getFields().putAll("tokens", Arrays.asList("hello", "world", "foo", "bar.com", "123"));
      collector.reset();
      startSession();
      Notifications.notifyBeginTransaction(morphline);
      assertTrue(morphline.process(record));
      assertEquals(1, collector.getNumStartEvents());
      Notifications.notifyCommitTransaction(morphline);
      assertEquals(expected, collector.getFirstRecord());
    }
  }
    
}
