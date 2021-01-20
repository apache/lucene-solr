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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.handler.component.RealTimeGetComponent.Resolution.DOC;
import static org.apache.solr.handler.component.RealTimeGetComponent.Resolution.ROOT_WITH_CHILDREN;

public class NestedAtomicUpdateTest extends SolrTestCaseJ4 {

  private final static String VERSION = "_version_";

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-tlog.xml", "schema-nest.xml"); // use "nest" schema
  }

  @Before
  public void before() {
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testMergeChildDoc() throws Exception {
    SolrInputDocument newChildDoc = sdoc("id", "3", "cat_ss", "child");
    SolrInputDocument addedDoc = sdoc("id", "1",
        "cat_ss", Collections.singletonMap("add", "bbb"),
        "child", Collections.singletonMap("add", sdocs(newChildDoc)));

    SolrInputDocument dummyBlock = sdoc("id", "1",
        "cat_ss", new ArrayList<>(Arrays.asList("aaa", "ccc")),
        "_root_", "1", "child", new ArrayList<>(sdocs(addedDoc)));
    dummyBlock.removeField(VERSION);

    SolrInputDocument preMergeDoc = new SolrInputDocument(dummyBlock);
    AtomicUpdateDocumentMerger docMerger = new AtomicUpdateDocumentMerger(req());
    docMerger.merge(addedDoc, dummyBlock);
    assertEquals("merged document should have the same id", preMergeDoc.getFieldValue("id"), dummyBlock.getFieldValue("id"));
    assertDocContainsSubset(preMergeDoc, dummyBlock);
    assertDocContainsSubset(addedDoc, dummyBlock);
    assertDocContainsSubset(newChildDoc, (SolrInputDocument) ((List) dummyBlock.getFieldValues("child")).get(1));
    assertEquals(dummyBlock.getFieldValue("id"), dummyBlock.getFieldValue("id"));
  }

  @Test
  public void testBlockAtomicInplaceUpdates() throws Exception {
    SolrInputDocument doc = sdoc("id", "1", "string_s", "root");
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "id:1", "fl", "*"),
        "//*[@numFound='1']",
        "//doc[1]/str[@name='id']=1"
    );

    List<SolrInputDocument> docs = IntStream.range(10, 20).mapToObj(x -> sdoc("id", String.valueOf(x), "string_s",
        "child", "inplace_updatable_int", "0")).collect(Collectors.toList());
    doc = sdoc("id", "1", "children", Collections.singletonMap("add", docs));
    addAndGetVersion(doc, params("wt", "json", "_route_", "1"));

    assertU(commit());


    assertQ(req("q", "_root_:1", "fl", "*", "rows", "11"),
        "//*[@numFound='11']"
    );

    assertQ(req("q", "string_s:child", "fl", "*"),
        "//*[@numFound='10']",
        "*[count(//str[@name='string_s'][.='child'])=10]"
    );

    for(int i = 10; i < 20; ++i) {
      doc = sdoc("id", String.valueOf(i), "inplace_updatable_int", Collections.singletonMap("inc", "1"));
      addAndGetVersion(doc, params("wt", "json", "_route_", "1"));
      assertU(commit());
    }

    for(int i = 10; i < 20; ++i) {
      doc = sdoc("id", String.valueOf(i), "inplace_updatable_int", Collections.singletonMap("inc", "1"));
      addAndGetVersion(doc, params("wt", "json", "_route_", "1"));
      assertU(commit());
    }

    // ensure updates work when block has more than 10 children
    for(int i = 10; i < 20; ++i) {
      docs = IntStream.range(i * 10, (i * 10) + 5).mapToObj(x -> sdoc("id", String.valueOf(x), "string_s", "grandChild")).collect(Collectors.toList());
      doc = sdoc("id", String.valueOf(i), "grandChildren", Collections.singletonMap("add", docs));
      addAndGetVersion(doc, params("wt", "json", "_route_", "1"));
      assertU(commit());
    }

    for(int i =10; i < 20; ++i) {
      doc = sdoc("id", String.valueOf(i), "inplace_updatable_int", Collections.singletonMap("inc", "1"));
      addAndGetVersion(doc, params("wt", "json", "_route_", "1"));
      assertU(commit());
    }

    assertQ(req("q", "-_root_:*", "fl", "*"),
        "//*[@numFound='0']"
    );

    assertQ(req("q", "string_s:grandChild", "fl", "*", "rows", "50"),
        "//*[@numFound='50']"
    );

    assertQ(req("q", "string_s:child", "fl", "*"),
        "//*[@numFound='10']",
        "*[count(//str[@name='string_s'][.='child'])=10]");

    assertJQ(req("q", "id:1", "fl", "*,[child limit=-1]"),
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/children/[0]/id=='10'",
        "/response/docs/[0]/children/[0]/inplace_updatable_int==3",
        "/response/docs/[0]/children/[0]/grandChildren/[0]/id=='100'",
        "/response/docs/[0]/children/[0]/grandChildren/[4]/id=='104'",
        "/response/docs/[0]/children/[9]/id=='19'"
    );

  }

  @Test
  public void testBlockAtomicQuantities() throws Exception {
    SolrInputDocument doc = sdoc("id", "1", "string_s", "root");
    assertU(adoc(doc));

    assertU(commit());

    assertQ(req("q", "id:1", "fl", "*"),
        "//*[@numFound='1']",
        "//doc[1]/str[@name='id']=1"
    );

    List<SolrInputDocument> docs = IntStream.range(10, 20).mapToObj(x -> sdoc("id", String.valueOf(x), "string_s", "child")).collect(Collectors.toList());
    doc = sdoc("id", "1", "children", Collections.singletonMap("add", docs));
    addAndGetVersion(doc, params("wt", "json"));

    assertU(commit());


    assertQ(req("q", "_root_:1", "fl", "*", "rows", "11"),
        "//*[@numFound='11']"
    );

    assertQ(req("q", "string_s:child", "fl", "*"),
        "//*[@numFound='10']",
        "*[count(//str[@name='string_s'][.='child'])=10]"
    );

    // ensure updates work when block has more than 10 children
    for(int i = 10; i < 20; ++i) {
      docs = IntStream.range(i * 10, (i * 10) + 5).mapToObj(x -> sdoc("id", String.valueOf(x), "string_s", "grandChild")).collect(Collectors.toList());
      doc =
          sdoc(
              "id",
              String.valueOf(i),
              "_root_", //shows we can specify the root field here instead of params
              "1",
              "grandChildren",
              Collections.singletonMap("add", docs));
      addAndGetVersion(doc, params("wt", "json")); // no _route_ with root
      assertU(commit());
    }

    assertQ(req("q", "string_s:grandChild", "fl", "*", "rows", "50"),
        "//*[@numFound='50']",
        "*[count(//str[@name='string_s'][.='grandChild'])=50]");

    assertQ(req("q", "string_s:child", "fl", "*"),
        "//*[@numFound='10']",
        "*[count(//str[@name='string_s'][.='child'])=10]");
  }

  @Test
  public void testBlockAtomicStack() throws Exception {
    SolrInputDocument doc = sdoc("id", "1", "child1", sdocs(sdoc("id", "2", "child_s", "child")));
    assertU(adoc(doc));

    assertU(commit());

    assertJQ(req("q","id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/child1/[0]/id=='2'",
        "/response/docs/[0]/child1/[0]/child_s=='child'"
    );

    doc = sdoc("id", "1", "child1", Collections.singletonMap("add", sdocs(sdoc("id", "3", "child_s", "child"))));
    addAndGetVersion(doc, params("wt", "json"));

    assertU(commit());

    assertJQ(req("q","id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/child1/[0]/id=='2'",
        "/response/docs/[0]/child1/[0]/child_s=='child'",
        "/response/docs/[0]/child1/[1]/id=='3'",
        "/response/docs/[0]/child1/[0]/child_s=='child'"
    );

    doc = sdoc("id", "2",
        "grandChild", Collections.singletonMap("add", sdocs(sdoc("id", "4", "child_s", "grandChild"), sdoc("id", "5", "child_s", "grandChild"))));
    addAndGetVersion(doc, params("wt", "json", "_route_", "1"));

    assertU(commit());

    assertJQ(req("q","id:1", "fl", "*, [child]", "sort", "id asc"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/child1/[0]/id=='2'",
        "/response/docs/[0]/child1/[0]/child_s=='child'",
        "/response/docs/[0]/child1/[1]/id=='3'",
        "/response/docs/[0]/child1/[1]/child_s=='child'",
        "/response/docs/[0]/child1/[0]/grandChild/[0]/id=='4'",
        "/response/docs/[0]/child1/[0]/grandChild/[0]/child_s=='grandChild'"
    );

    doc = sdoc("id", "1",
        "child2", Collections.singletonMap("add", sdocs(sdoc("id", "8", "child_s", "child"))));
    addAndGetVersion(doc, params("wt", "json"));

    assertU(commit());

    assertJQ(req("q","id:1", "fl", "*, [child]", "sort", "id asc"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/child1/[0]/id=='2'",
        "/response/docs/[0]/child1/[0]/child_s=='child'",
        "/response/docs/[0]/child1/[1]/id=='3'",
        "/response/docs/[0]/child1/[1]/child_s=='child'",
        "/response/docs/[0]/child1/[0]/grandChild/[0]/id=='4'",
        "/response/docs/[0]/child1/[0]/grandChild/[0]/child_s=='grandChild'",
        "/response/docs/[0]/child2/[0]/id=='8'",
        "/response/docs/[0]/child2/[0]/child_s=='child'"
    );

    doc = sdoc("id", "1",
        "new_s", Collections.singletonMap("add", "new string"));
    addAndGetVersion(doc, params("wt", "json"));

    assertU(commit());

    // ensure the whole block has been committed correctly to the index.
    assertJQ(req("q","id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/child1/[0]/id=='2'",
        "/response/docs/[0]/child1/[0]/child_s=='child'",
        "/response/docs/[0]/child1/[1]/id=='3'",
        "/response/docs/[0]/child1/[1]/child_s=='child'",
        "/response/docs/[0]/child1/[0]/grandChild/[0]/id=='4'",
        "/response/docs/[0]/child1/[0]/grandChild/[0]/child_s=='grandChild'",
        "/response/docs/[0]/child1/[0]/grandChild/[1]/id=='5'",
        "/response/docs/[0]/child1/[0]/grandChild/[1]/child_s=='grandChild'",
        "/response/docs/[0]/new_s=='new string'",
        "/response/docs/[0]/child2/[0]/id=='8'",
        "/response/docs/[0]/child2/[0]/child_s=='child'"
    );

  }

  @Test
  public void testBlockAtomicAdd() throws Exception {

    final SolrInputDocument sdoc2 = sdoc("id", "2", "cat_ss", "child");
    SolrInputDocument doc = sdoc("id", "1",
        "cat_ss", new String[] {"aaa", "ccc"},
        "child1", sdoc2
    );
    assertU(adoc(doc));

    BytesRef rootDocId = new BytesRef("1");
    SolrCore core = h.getCore();
    SolrInputDocument block = RealTimeGetComponent.getInputDocument(core, rootDocId, rootDocId, null, null, ROOT_WITH_CHILDREN);
    // assert block doc has child docs
    assertTrue(block.containsKey("child1"));

    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    // commit the changes
    assertU(commit());

    BytesRef childDocId = new BytesRef("2");
    assertEquals(sdoc2.toString(), removeSpecialFields(
        RealTimeGetComponent.getInputDocument(core, childDocId, rootDocId, null, null, DOC)
    ).toString());

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );

    doc = sdoc("id", "1",
        "cat_ss", Collections.singletonMap("add", "bbb"),
        "child2", Collections.singletonMap("add", sdoc("id", "3", "cat_ss", "child")));
    addAndGetVersion(doc, params("wt", "json"));


     assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, child2, [child]")
     ,"=={\"doc\":{'id':\"1\"" +
     ", cat_ss:[\"aaa\",\"ccc\",\"bbb\"], child2:{\"id\":\"3\", \"cat_ss\": [\"child\"]}," +
     "child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}]" +
     "       }}"
     );

    assertU(commit());

    // a cut-n-paste of the first big query, but this time it will be retrieved from the index rather than the transaction log
    // this requires ChildDocTransformer to get the whole block, since the document is retrieved using an index lookup
    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, child2, [child]")
        , "=={\"doc\":{'id':\"1\"" +
            ", cat_ss:[\"aaa\",\"ccc\",\"bbb\"], child2:{\"id\":\"3\", \"cat_ss\": [\"child\"]}," +
            "child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}]" +
            "       }}"
    );

    doc = sdoc("id", "2",
        "child3", Collections.singletonMap("add", sdoc("id", "4", "cat_ss", "grandChild")));
    addAndGetVersion(doc, params("wt", "json", "_route_", "1"));

    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, child2, child3, [child]")
        ,"=={'doc':{'id':'1'" +
            ", cat_ss:[\"aaa\",\"ccc\",\"bbb\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"], child3:{\"id\":\"4\",\"cat_ss\":[\"grandChild\"]}}]," +
            "child2:{\"id\":\"3\", \"cat_ss\": [\"child\"]}" +
            "       }}"
    );

    assertJQ(req("qt","/get", "id","2", "fl","id, cat_ss, child, child3, [child]")
        ,"=={'doc':{\"id\":\"2\",\"cat_ss\":[\"child\"], child3:{\"id\":\"4\",\"cat_ss\":[\"grandChild\"]}}" +
            "       }}"
    );

    assertU(commit());

    //add greatGrandChild
    doc = sdoc("id", "4",
        "child4", Collections.singletonMap("add", sdoc("id", "5", "cat_ss", "greatGrandChild")));
    addAndGetVersion(doc, params("wt", "json", "_route_", "1"));

    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, child2, child3, child4, [child]")
        ,"=={'doc':{'id':'1'" +
            ", cat_ss:[\"aaa\",\"ccc\",\"bbb\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"], child3:{\"id\":\"4\",\"cat_ss\":[\"grandChild\"]," +
            " child4:{\"id\":\"5\",\"cat_ss\":[\"greatGrandChild\"]}}}], child2:{\"id\":\"3\", \"cat_ss\": [\"child\"]}" +
            "       }}"
    );

    assertJQ(req("qt","/get", "id","4", "fl","id, cat_ss, child4, [child]")
        ,"=={'doc':{\"id\":\"4\",\"cat_ss\":[\"grandChild\"], child4:{\"id\":\"5\",\"cat_ss\":[\"greatGrandChild\"]}}" +
            "       }}"
    );

    assertU(commit());

    //add another greatGrandChild
    doc = sdoc("id", "4",
        "child4", Collections.singletonMap("add", sdoc("id", "6", "cat_ss", "greatGrandChild")));
    addAndGetVersion(doc, params("wt", "json", "_route_", "1"));

    assertU(commit());

    assertJQ(req("qt","/get", "id","4", "fl","id, cat_ss, child4, [child]")
        ,"=={'doc':{\"id\":\"4\",\"cat_ss\":[\"grandChild\"], child4:[{\"id\":\"5\",\"cat_ss\":[\"greatGrandChild\"]}," +
            "{\"id\":\"6\", \"cat_ss\":[\"greatGrandChild\"]}]}" +
            "       }}"
    );

    //add another child field name
    doc = sdoc("id", "1",
        "child5", Collections.singletonMap("add", sdocs(sdoc("id", "7", "cat_ss", "child"),
            sdoc("id", "8", "cat_ss", "child")
        ))
    );
    addAndGetVersion(doc, params("wt", "json"));

    assertU(commit());

    doc = sdoc("id", "1",
        "new_s", Collections.singletonMap("add", "new string"));
    addAndGetVersion(doc, params("wt", "json"));

    assertU(commit());


    // ensure the whole block has been committed correctly to the index.
    assertJQ(req("q","id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/cat_ss/[0]==\"aaa\"",
        "/response/docs/[0]/cat_ss/[1]==\"ccc\"",
        "/response/docs/[0]/cat_ss/[2]==\"bbb\"",
        "/response/docs/[0]/child1/[0]/id=='2'",
        "/response/docs/[0]/child1/[0]/cat_ss/[0]=='child'",
        "/response/docs/[0]/child1/[0]/child3/id=='4'",
        "/response/docs/[0]/child1/[0]/child3/cat_ss/[0]=='grandChild'",
        "/response/docs/[0]/child1/[0]/child3/child4/[0]/id=='5'",
        "/response/docs/[0]/child1/[0]/child3/child4/[0]/cat_ss/[0]=='greatGrandChild'",
        "/response/docs/[0]/child1/[0]/child3/child4/[1]/id=='6'",
        "/response/docs/[0]/child1/[0]/child3/child4/[1]/cat_ss/[0]=='greatGrandChild'",
        "/response/docs/[0]/child2/id=='3'",
        "/response/docs/[0]/child2/cat_ss/[0]=='child'",
        "/response/docs/[0]/child5/[0]/id=='7'",
        "/response/docs/[0]/child5/[0]/cat_ss/[0]=='child'",
        "/response/docs/[0]/child5/[1]/id=='8'",
        "/response/docs/[0]/child5/[1]/cat_ss/[0]=='child'",
        "/response/docs/[0]/new_s=='new string'"
    );
  }

  @Test
  public void testBlockAtomicSet() throws Exception {
    SolrInputDocument sdoc2 = sdoc("id", "2", "cat_ss", "child");
    SolrInputDocument doc = sdoc("id", "1",
        "cat_ss", new String[] {"aaa", "ccc"},
        "child1", Collections.singleton(sdoc2)
    );
    assertU(adoc(doc));

    BytesRef rootDocId = new BytesRef("1");
    SolrCore core = h.getCore();
    SolrInputDocument block = RealTimeGetComponent.getInputDocument(core, rootDocId, rootDocId, null, null, ROOT_WITH_CHILDREN);
    // assert block doc has child docs
    assertTrue(block.containsKey("child1"));

    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    // commit the changes
    assertU(commit());

    BytesRef childDocId = new BytesRef("2");
    assertEquals(sdoc2.toString(), removeSpecialFields(
        RealTimeGetComponent.getInputDocument(core, childDocId, rootDocId, null, null, DOC)
    ).toString());

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );

    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={\"doc\":{'id':\"1\"" +
            ", cat_ss:[\"aaa\",\"ccc\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}]" +
            "       }}"
    );

    assertU(commit());

    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={\"doc\":{'id':\"1\"" +
            ", cat_ss:[\"aaa\",\"ccc\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}]" +
            "       }}"
    );

    doc = sdoc("id", "1",
        "cat_ss", Collections.singletonMap("set", Arrays.asList("aaa", "bbb")),
        "child1", Collections.singletonMap("set", sdoc("id", "3", "cat_ss", "child")));
    addAndGetVersion(doc, params("wt", "json", "_route_", "1"));


    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={\"doc\":{'id':\"1\"" +
            ", cat_ss:[\"aaa\",\"bbb\"], child1:{\"id\":\"3\",\"cat_ss\":[\"child\"]}" +
            "       }}"
    );

    assertU(commit());

    // a cut-n-paste of the first big query, but this time it will be retrieved from the index rather than the transaction log
    // this requires ChildDocTransformer to get the whole block, since the document is retrieved using an index lookup
    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={'doc':{'id':'1'" +
            ", cat_ss:[\"aaa\",\"bbb\"], child1:{\"id\":\"3\",\"cat_ss\":[\"child\"]}" +
            "       }}"
    );

    doc = sdoc("id", "3",
        "child2", Collections.singletonMap("set", sdoc("id", "4", "cat_ss", "child")));
    addAndGetVersion(doc, params("wt", "json", "_route_", "1"));

    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, child2, [child]")
        ,"=={'doc':{'id':'1'" +
            ", cat_ss:[\"aaa\",\"bbb\"], child1:{\"id\":\"3\",\"cat_ss\":[\"child\"], child2:{\"id\":\"4\",\"cat_ss\":[\"child\"]}}" +
            "       }}"
    );

    assertJQ(req("qt","/get", "id","3", "fl","id, cat_ss, child, child2, [child]")
        ,"=={'doc':{\"id\":\"3\",\"cat_ss\":[\"child\"], child2:{\"id\":\"4\",\"cat_ss\":[\"child\"]}}" +
            "       }}"
    );

    assertU(commit());

    // ensure the whole block has been committed correctly to the index.
    assertJQ(req("q","id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/cat_ss/[0]==\"aaa\"",
        "/response/docs/[0]/cat_ss/[1]==\"bbb\"",
        "/response/docs/[0]/child1/id=='3'",
        "/response/docs/[0]/child1/cat_ss/[0]=='child'",
        "/response/docs/[0]/child1/child2/id=='4'",
        "/response/docs/[0]/child1/child2/cat_ss/[0]=='child'"
    );
  }

  @Test
  public void testAtomicUpdateDeleteNoRootField() throws Exception {
    SolrInputDocument doc = sdoc("id", "1",
        "cat_ss", new String[]{"aaa", "bbb"});
    assertU(adoc(doc));

    assertJQ(req("q", "id:1")
        , "/response/numFound==0"
    );

    // commit the changes
    assertU(commit());

    assertJQ(req("q", "id:1"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/cat_ss/[0]==\"aaa\"",
        "/response/docs/[0]/cat_ss/[1]==\"bbb\""
    );

    doc = sdoc("id", "1",
        "child1", Collections.singletonMap("add", sdoc("id", "2", "cat_ss", "child")));
    addAndGetVersion(doc, params("wt", "json"));

    // commit the changes
    assertU(commit());

    // assert that doc with id:1 was removed even though it did not have _root_:1 since it was not indexed with child documents.
    assertJQ(req("q", "id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/cat_ss/[0]==\"aaa\"",
        "/response/docs/[0]/cat_ss/[1]==\"bbb\"",
        "/response/docs/[0]/child1/id==\"2\"",
        "/response/docs/[0]/child1/cat_ss/[0]==\"child\""
    );


  }

  @Test
  public void testBlockAtomicRemove() throws Exception {
    SolrInputDocument sdoc2 = sdoc("id", "2", "cat_ss", "child");
    SolrInputDocument doc = sdoc("id", "1",
        "cat_ss", new String[] {"aaa", "ccc"},
        "child1", sdocs(sdoc2, sdoc("id", "3", "cat_ss", "child"))
    );
    assertU(adoc(doc));

    BytesRef rootDocId = new BytesRef("1");
    SolrCore core = h.getCore();
    SolrInputDocument block = RealTimeGetComponent.getInputDocument(core, rootDocId, rootDocId, null, null, ROOT_WITH_CHILDREN);
    // assert block doc has child docs
    assertTrue(block.containsKey("child1"));

    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    // commit the changes
    assertU(commit());

    BytesRef childDocId = new BytesRef("2");
    assertEquals(sdoc2.toString(), removeSpecialFields(
        RealTimeGetComponent.getInputDocument(core, childDocId, rootDocId, null, null, DOC)
    ).toString());

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );

    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={\"doc\":{'id':\"1\"" +
            ", cat_ss:[\"aaa\",\"ccc\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}, {\"id\":\"3\",\"cat_ss\":[\"child\"]}]" +
            "       }}"
    );

    assertU(commit());

    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={\"doc\":{'id':\"1\"" +
            ", cat_ss:[\"aaa\",\"ccc\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}, {\"id\":\"3\",\"cat_ss\":[\"child\"]}]" +
            "       }}"
    );

    doc = sdoc("id", "1",
        "child1", Collections.singletonMap("remove", sdoc("id", "3", "cat_ss", "child")));
    addAndGetVersion(doc, params("wt", "json"));


    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={\"doc\":{'id':\"1\"" +
            ", cat_ss:[\"aaa\",\"ccc\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}]" +
            "       }}"
    );

    assertU(commit());

    // a cut-n-paste of the first big query, but this time it will be retrieved from the index rather than the transaction log
    // this requires ChildDocTransformer to get the whole block, since the document is retrieved using an index lookup
    assertJQ(req("qt","/get", "id","1", "fl","id, cat_ss, child1, [child]")
        ,"=={'doc':{'id':'1'" +
            ", cat_ss:[\"aaa\",\"ccc\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}]" +
            "       }}"
    );

    // ensure the whole block has been committed correctly to the index.
    assertJQ(req("q","id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/cat_ss/[0]==\"aaa\"",
        "/response/docs/[0]/cat_ss/[1]==\"ccc\"",
        "/response/docs/[0]/child1/[0]/id=='2'",
        "/response/docs/[0]/child1/[0]/cat_ss/[0]=='child'"
    );
  }

  @Test
  public void testBlockAtomicSetToNull() throws Exception {
    testBlockAtomicSetToNullOrEmpty(false);
  }

  @Test
  public void testBlockAtomicSetToEmpty() throws Exception {
    testBlockAtomicSetToNullOrEmpty(true);
  }

  private void testBlockAtomicSetToNullOrEmpty(boolean empty) throws Exception {
    // latlon field is included to ensure reading from LatLonDocValuesField is working due to atomic update.
    // See SOLR-13966 for further details.
    SolrInputDocument sdoc2 = sdoc("id", "2", "cat_ss", "child");
    SolrInputDocument doc = sdoc("id", "1", "latlon", "0,0",
        "cat_ss", new String[] {"aaa", "ccc"},
        "child1", sdocs(sdoc2, sdoc("id", "3", "cat_ss", "child")));
    assertU(adoc(doc));

    BytesRef rootDocId = new BytesRef("1");
    SolrCore core = h.getCore();
    SolrInputDocument block = RealTimeGetComponent.getInputDocument(core, rootDocId, rootDocId, null, null, ROOT_WITH_CHILDREN);
    // assert block doc has child docs
    assertTrue(block.containsKey("child1"));

    assertJQ(req("q", "id:1"), "/response/numFound==0");

    // commit the changes
    assertU(commit());

    BytesRef childDocId = new BytesRef("2");
    assertEquals(sdoc2.toString(), removeSpecialFields(
        RealTimeGetComponent.getInputDocument(core, childDocId, rootDocId, null, null, DOC)
    ).toString());

    assertJQ(req("q", "id:1"), "/response/numFound==1");

    assertJQ(req("qt", "/get", "id", "1", "fl", "id, latlon, cat_ss, child1, [child]"),
        "=={\"doc\":{'id':\"1\", \"latlon\":\"0,0\"" +
            ", cat_ss:[\"aaa\",\"ccc\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}, {\"id\":\"3\",\"cat_ss\":[\"child\"]}]}}");

    assertU(commit());

    assertJQ(req("qt", "/get", "id", "1", "fl", "id, latlon, cat_ss, child1, [child]"),
        "=={\"doc\":{'id':\"1\", \"latlon\":\"0,0\"" +
            ", cat_ss:[\"aaa\",\"ccc\"], child1:[{\"id\":\"2\",\"cat_ss\":[\"child\"]}, {\"id\":\"3\",\"cat_ss\":[\"child\"]}]}}");

    doc = sdoc("id", "1", "child1", Collections.singletonMap("set", empty ? new ArrayList<>() : null));
    addAndGetVersion(doc, params("wt", "json"));

    assertJQ(req("qt", "/get", "id", "1", "fl", "id, latlon, cat_ss, child1, [child]"),
        "=={\"doc\":{'id':\"1\", \"latlon\":\"0,0\", cat_ss:[\"aaa\",\"ccc\"]}}");

    assertU(commit());

    // a cut-n-paste of the first big query, but this time it will be retrieved from the index rather than the
    // transaction log
    // this requires ChildDocTransformer to get the whole block, since the document is retrieved using an index lookup
    assertJQ(req("qt", "/get", "id", "1", "fl", "id, latlon, cat_ss, child1, [child]"),
        "=={\"doc\":{'id':\"1\", \"latlon\":\"0,0\", cat_ss:[\"aaa\",\"ccc\"]}}");

    // ensure the whole block has been committed correctly to the index.
    assertJQ(req("q", "id:1", "fl", "*, [child]"),
        "/response/numFound==1",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/latlon=='0,0'",
        "/response/docs/[0]/cat_ss/[0]==\"aaa\"",
        "/response/docs/[0]/cat_ss/[1]==\"ccc\"");
  }

  public void testIncorrectlyUpdateChildDoc() throws Exception {
    SolrInputDocument doc = sdoc("id", "1",
        "child", sdoc("id", "2"));
    assertU(adoc(doc));
    assertU(commit());

    // did not add _root_ like we should have
    SolrException e = expectThrows(SolrException.class, () -> {
      addAndGetVersion(
          sdoc("id", "2", "grandchild", Collections.singletonMap("set", sdoc("id", "3"))), null);
    });
    assertTrue(e.toString(), e.getMessage().contains("Attempted an atomic/partial update to a " +
        "child doc without indicating the _root_ somehow."));
  }

  private SolrInputDocument removeSpecialFields(SolrInputDocument doc) {
    final Iterator<SolrInputField> fieldIter = doc.iterator();
    while (fieldIter.hasNext()) {
      SolrInputField field =  fieldIter.next();
      if (field.getName().matches("^_.*_$")) {
        fieldIter.remove();
      }
    }
    return doc;
  }

  @SuppressWarnings({"unchecked"})
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
