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

package org.apache.solr.update.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtomicUpdateBlockTest extends SolrTestCaseJ4 {

  private final static String VERSION = "_version_";
  private static String PREVIOUS_ENABLE_UPDATE_LOG_VALUE;

  @BeforeClass
  public static void beforeTests() throws Exception {
    PREVIOUS_ENABLE_UPDATE_LOG_VALUE = System.getProperty("enable.update.log");
    System.setProperty("enable.update.log", "true");
    initCore("solrconfig-update-processor-chains.xml", "schema-nest.xml"); // use "nest" schema
  }

  @AfterClass
  public static void afterTests() throws Exception {
    // restore enable.update.log
    PREVIOUS_ENABLE_UPDATE_LOG_VALUE = System.getProperty("enable.update.log");
    System.setProperty("enable.update.log", PREVIOUS_ENABLE_UPDATE_LOG_VALUE);
  }

  @Before
  public void before() {
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testMergeChildDoc() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat_ss", new String[]{"aaa", "ccc"});
    doc.setField("child", Collections.singletonList(sdoc("id", "2", "cat_ss", "child")));
    addDoc(adoc(doc), "nested-rtg");

    BytesRef rootDocId = new BytesRef("1");
    SolrCore core = h.getCore();
    SolrInputDocument block = RealTimeGetComponent.getInputDocument(core, rootDocId, true);
    // assert block doc has child docs
    assertTrue(block.containsKey("child"));

    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    // commit the changes
    assertU(commit());

    SolrInputDocument newChildDoc = sdoc("id", "3", "cat_ss", "child");
    SolrInputDocument addedDoc = sdoc("id", "1",
        "cat_ss", ImmutableMap.of("add", "bbb"),
        "child", ImmutableMap.of("add", sdocs(newChildDoc)));
    block = RealTimeGetComponent.getInputDocument(core, rootDocId, true);
    block.removeField(VERSION);
    SolrInputDocument preMergeDoc = new SolrInputDocument(block);
    AtomicUpdateDocumentMerger docMerger = new AtomicUpdateDocumentMerger(req());
    docMerger.merge(addedDoc, block);
    assertEquals("merged document should have the same id", preMergeDoc.getFieldValue("id"), block.getFieldValue("id"));
    assertDocContainsSubset(preMergeDoc, block);
    assertDocContainsSubset(addedDoc, block);
    assertDocContainsSubset(newChildDoc, (SolrInputDocument) ((List) block.getFieldValues("child")).get(1));
    assertEquals(doc.getFieldValue("id"), block.getFieldValue("id"));
  }

  @Test
  public void testBlockRealTimeGet() throws Exception {

    SolrInputDocument doc = sdoc("id", "1",
        "cat_ss", new String[] {"aaa", "ccc"},
        "child1", sdoc("id", "2", "cat_ss", "child")
    );
    json(doc);
    addDoc(adoc(doc), "nested-rtg");

    BytesRef rootDocId = new BytesRef("1");
    SolrCore core = h.getCore();
    SolrInputDocument block = RealTimeGetComponent.getInputDocument(core, rootDocId, true);
    // assert block doc has child docs
    assertTrue(block.containsKey("child1"));

    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    // commit the changes
    assertU(commit());

    SolrInputDocument committedBlock = RealTimeGetComponent.getInputDocument(core, rootDocId, true);
    BytesRef childDocId = new BytesRef("2");
    // ensure the whole block is returned when resolveBlock is true and id of a child doc is provided
    assertEquals(committedBlock.toString(), RealTimeGetComponent.getInputDocument(core, childDocId, true).toString());

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );

    doc = sdoc("id", "1",
        "cat_ss", ImmutableMap.of("add", "bbb"),
        "child2", ImmutableMap.of("add", sdoc("id", "3", "cat_ss", "child")));
    addAndGetVersion(doc, params("update.chain", "nested-rtg", "wt", "json"));


     assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
     ,"=={\"doc\":{'id':\"1\"" +
     ", cat_ss:[\"aaa\",\"ccc\",\"bbb\"], child1:{\"id\":\"2\",\"cat_ss\":[\"child\"]}" +
     "       }}"
     );

    assertU(commit());

    // a cut-n-paste of the first big query, but this time it will be retrieved from the index rather than the transaction log
    // this requires ChildDocTransformer to get the whole block, since the document is retrieved using an index lookup
    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={'doc':{'id':'1'" +
            ", cat_ss:[\"aaa\",\"ccc\",\"bbb\"], child1:{\"id\":\"2\",\"cat_ss\":[\"child\"]}" +
            "       }}"
    );

    doc = sdoc("id", "2",
        "child3", ImmutableMap.of("add", sdoc("id", "4", "cat_ss", "child")));
    addAndGetVersion(doc, params("update.chain", "nested-rtg", "wt", "json"));

    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, child3, [child]")
        ,"=={'doc':{'id':'1'" +
            ", cat_ss:[\"aaa\",\"ccc\",\"bbb\"], child1:{\"id\":\"2\",\"cat_ss\":[\"child\"], child3:{\"id\":\"4\",\"cat_ss\":[\"child\"]}}" +
            "       }}"
    );

    assertJQ(req("qt","/get", "id","2", "fl","id, cat_ss, child, child3, [child]")
        ,"=={'doc':{\"id\":\"2\",\"cat_ss\":[\"child\"], child3:{\"id\":\"4\",\"cat_ss\":[\"child\"]}}" +
            "       }}"
    );

    assertU(commit());

    // ensure the whole block has been committed correctly to the index.
    assertJQ(req("q","id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/cat_ss/[0]==\"aaa\"",
        "/response/docs/[0]/cat_ss/[1]==\"ccc\"",
        "/response/docs/[0]/cat_ss/[2]==\"bbb\"",
        "/response/docs/[0]/child1/id=='2'",
        "/response/docs/[0]/child1/cat_ss/[0]=='child'",
        "/response/docs/[0]/child1/child3/id=='4'",
        "/response/docs/[0]/child1/child3/cat_ss/[0]=='child'",
        "/response/docs/[0]/child2/id=='3'",
        "/response/docs/[0]/child2/cat_ss/[0]=='child'"
    );
  }

  private static void assertDocContainsSubset(SolrInputDocument subsetDoc, SolrInputDocument fullDoc) {
    for(SolrInputField field: subsetDoc) {
      String fieldName = field.getName();
      assertTrue("doc should contain field: " + fieldName, fullDoc.containsKey(fieldName));
      Object fullValue = fullDoc.getField(fieldName).getValue();
      if(fullValue instanceof Collection) {
        ((Collection) fullValue).containsAll(field.getValues());
      } else {
        assertEquals("docs should have the same value for field: " + fieldName, field.getValue(), fullValue);
      }
    }
  }
}
