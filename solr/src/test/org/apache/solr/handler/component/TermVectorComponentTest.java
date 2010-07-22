package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.TermVectorParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
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


/**
 *
 *
 **/
public class TermVectorComponentTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");

    assertNull(h.validateUpdate(adoc("id", "0",
            "test_posofftv", "This is a title and another title",
            "test_basictv", "This is a title and another title",
            "test_notv", "This is a title and another title",
            "test_postv", "This is a title and another title",
            "test_offtv", "This is a title and another title"
    )));
    assertNull(h.validateUpdate(adoc("id", "1",
            "test_posofftv", "The quick reb fox jumped over the lazy brown dogs.",
            "test_basictv", "The quick reb fox jumped over the lazy brown dogs.",
            "test_notv", "The quick reb fox jumped over the lazy brown dogs.",
            "test_postv", "The quick reb fox jumped over the lazy brown dogs.",
            "test_offtv", "The quick reb fox jumped over the lazy brown dogs."
    )));
    assertNull(h.validateUpdate(adoc("id", "2",
            "test_posofftv", "This is a document",
            "test_basictv", "This is a document",
            "test_notv", "This is a document",
            "test_postv", "This is a document",
            "test_offtv", "This is a document"
    )));
    assertNull(h.validateUpdate(adoc("id", "3",
            "test_posofftv", "another document",
            "test_basictv", "another document",
            "test_notv", "another document",
            "test_postv", "another document",
            "test_offtv", "another document"
    )));
    //bunch of docs that are variants on blue
    assertNull(h.validateUpdate(adoc("id", "4",
            "test_posofftv", "blue",
            "test_basictv", "blue",
            "test_notv", "blue",
            "test_postv", "blue",
            "test_offtv", "blue"
    )));
    assertNull(h.validateUpdate(adoc("id", "5",
            "test_posofftv", "blud",
            "test_basictv", "blud",
            "test_notv", "blud",
            "test_postv", "blud",
            "test_offtv", "blud"
    )));
    assertNull(h.validateUpdate(adoc("id", "6",
            "test_posofftv", "boue",
            "test_basictv", "boue",
            "test_notv", "boue",
            "test_postv", "boue",
            "test_offtv", "boue"
    )));
    assertNull(h.validateUpdate(adoc("id", "7",
            "test_posofftv", "glue",
            "test_basictv", "glue",
            "test_notv", "glue",
            "test_postv", "glue",
            "test_offtv", "glue"
    )));
    assertNull(h.validateUpdate(adoc("id", "8",
            "test_posofftv", "blee",
            "test_basictv", "blee",
            "test_notv", "blee",
            "test_postv", "blee",
            "test_offtv", "blee"
    )));
    assertNull(h.validateUpdate(adoc("id", "9",
            "test_posofftv", "blah",
            "test_basictv", "blah",
            "test_notv", "blah",
            "test_postv", "blah",
            "test_offtv", "blah"
    )));

    assertNull(h.validateUpdate(commit()));
  }

  @Test
  public void testBasics() throws Exception {
    SolrCore core = h.getCore();
    SearchComponent tvComp = core.getSearchComponent("tvComponent");
    assertTrue("tvComp is null and it shouldn't be", tvComp != null);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, "id:0");
    params.add(CommonParams.QT, "tvrh");
    params.add(TermVectorParams.TF, "true");
    params.add(TermVectorComponent.COMPONENT_NAME, "true");
    SolrRequestHandler handler = core.getRequestHandler("tvrh");
    SolrQueryResponse rsp;
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    NamedList values = rsp.getValues();
    NamedList termVectors = (NamedList) values.get(TermVectorComponent.TERM_VECTORS);
    assertTrue("termVectors is null and it shouldn't be", termVectors != null);
    if (VERBOSE) System.out.println("TVs:" + termVectors);
    NamedList doc = (NamedList) termVectors.getVal(0);
    assertTrue("doc is null and it shouldn't be", doc != null);
    assertEquals(doc.size(), 5);
    NamedList field = (NamedList) doc.get("test_posofftv");
    assertTrue("field is null and it shouldn't be", field != null);
    assertTrue(field.size() + " does not equal: " + 2, field.size() == 2);
    NamedList titl = (NamedList) field.get("titl");
    assertTrue("titl is null and it shouldn't be", titl != null);
    assertTrue(titl.get("tf") + " does not equal: " + 2, ((Integer) titl.get("tf")) == 2);
    //there should not be any positions or offsets
    NamedList positions = (NamedList) titl.get("positions");
    assertTrue("positions is not null and it should be", positions == null);
    NamedList offsets = (NamedList) titl.get("offsets");
    assertTrue("offsets is not null and it should be", offsets == null);
    String uniqueKeyFieldName = (String) termVectors.getVal(1);
    assertTrue("uniqueKeyFieldName is null and it shouldn't be", uniqueKeyFieldName != null);
    assertTrue(uniqueKeyFieldName + " is not equal to " + "id", uniqueKeyFieldName.equals("id") == true);

  }

  @Test
  public void testOptions() throws Exception {
    SolrCore core = h.getCore();
    SearchComponent tvComp = core.getSearchComponent("tvComponent");
    assertTrue("tvComp is null and it shouldn't be", tvComp != null);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, "id:0");
    params.add(CommonParams.QT, "tvrh");
    params.add(TermVectorParams.TF, "true");
    params.add(TermVectorParams.DF, "true");
    params.add(TermVectorParams.OFFSETS, "true");
    params.add(TermVectorParams.POSITIONS, "true");
    params.add(TermVectorParams.TF_IDF, "true");
    params.add(TermVectorComponent.COMPONENT_NAME, "true");

    SolrRequestHandler handler = core.getRequestHandler("tvrh");
    SolrQueryResponse rsp;
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    NamedList values = rsp.getValues();
    NamedList termVectors = (NamedList) values.get(TermVectorComponent.TERM_VECTORS);
    assertTrue("termVectors is null and it shouldn't be", termVectors != null);
    // System.out.println("TVs: " + termVectors);
    NamedList doc = (NamedList) termVectors.getVal(0);
    assertTrue("doc is null and it shouldn't be", doc != null);
    assertEquals(doc.size(), 5);
    NamedList offtv = (NamedList) doc.get("test_posofftv");
    assertTrue("offtv is null and it shouldn't be", offtv != null);
    assertTrue("offtv Size: " + offtv.size() + " is not: " + 2, offtv.size() == 2);
    NamedList another = (NamedList) offtv.get("anoth");
    NamedList offsets = (NamedList) another.get("offsets");
    assertTrue("offsets is null and it shouldn't be", offsets != null);
    assertTrue("offsets Size: " + offsets.size() + " is not greater than: " + 0, offsets.size() > 0);
    NamedList pos = (NamedList) another.get("positions");
    assertTrue("pos is null and it shouldn't be", pos != null);
    assertTrue("pos Size: " + pos.size() + " is not greater than: " + 0, pos.size() > 0);
    Integer df = (Integer) another.get("df");
    assertTrue("df is null and it shouldn't be", df != null);
    assertTrue(df + " does not equal: " + 2, df == 2);
    Double tfIdf = (Double) another.get("tf-idf");
    assertTrue("tfIdf is null and it shouldn't be", tfIdf != null);
    assertTrue(tfIdf + " does not equal: " + 0.5, tfIdf == 0.5);
  }

  /*
<field name="test_basictv" type="text" termVectors="true"/>
   <field name="test_notv" type="text" termVectors="false"/>
   <field name="test_postv" type="text" termVectors="true" termPositions="true"/>
   <field name="test_offtv" type="text" termVectors="true" termOffsets="true"/>
   <field name="test_posofftv" type="text" termVectors="true"
     termPositions="true" termOffsets="true"/>
   */
  @Test
  public void testPerField() throws Exception {
    SolrCore core = h.getCore();
    SearchComponent tvComp = core.getSearchComponent("tvComponent");
    assertTrue("tvComp is null and it shouldn't be", tvComp != null);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, "id:0");
    params.add(CommonParams.QT, "tvrh");
    params.add(TermVectorParams.FIELDS, "test_basictv,test_notv,test_postv,test_offtv,test_posofftv");
    params.add(TermVectorParams.TF, "true");
    params.add(TermVectorParams.DF, "true");
    params.add(TermVectorParams.OFFSETS, "true");
    params.add(TermVectorParams.POSITIONS, "true");
    params.add(TermVectorParams.TF_IDF, "true");
    params.add(TermVectorComponent.COMPONENT_NAME, "true");
    //per field
    params.add("f.test_posofftv." + TermVectorParams.POSITIONS, "false");
    params.add("f.test_offtv." + TermVectorParams.OFFSETS, "false");
    params.add("f.test_basictv." + TermVectorParams.DF, "false");
    params.add("f.test_basictv." + TermVectorParams.TF, "false");
    params.add("f.test_basictv." + TermVectorParams.TF_IDF, "false");
    SolrRequestHandler handler = core.getRequestHandler("tvrh");
    SolrQueryResponse rsp;
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    NamedList values = rsp.getValues();
    NamedList termVectors = (NamedList) values.get(TermVectorComponent.TERM_VECTORS);
    assertTrue("termVectors is null and it shouldn't be", termVectors != null);
    if (VERBOSE) System.out.println("TVs: " + termVectors);
    NamedList doc = (NamedList) termVectors.get("doc-0");
    assertTrue("doc is null and it shouldn't be", doc != null);
    assertEquals(doc.size(), 5);
    NamedList vec;
    NamedList another;
    NamedList offsets;
    NamedList pos;
    Integer df;
    Double val;
    vec = (NamedList) doc.get("test_posofftv");
    assertNotNull(vec);
    assertEquals(vec.size(), 2);
    another = (NamedList) vec.get("anoth");
    offsets = (NamedList) another.get("offsets");
    assertNotNull(offsets);
    assertTrue(offsets.size() > 0);
    pos = (NamedList) another.get("positions");
    //positions should be null, since we turned them off
    assertNull(pos);
    df = (Integer) another.get("df");
    assertNotNull(df);
    assertTrue(df == 2);
    val = (Double) another.get("tf-idf");
    assertTrue("tfIdf is null and it shouldn't be", val != null);
    assertTrue(val + " does not equal: " + 0.5, val == 0.5);
    //Try out the other fields, too
    vec = (NamedList) doc.get("test_offtv");
    assertNotNull(vec);
    assertEquals(vec.size(), 2);
    another = (NamedList) vec.get("anoth");
    offsets = (NamedList) another.get("offsets");
    assertNull(offsets);
    pos = (NamedList) another.get("positions");
    //positions should be null, since we turned them off
    assertNull(vec.toString(), pos);
    df = (Integer) another.get("df");
    assertNotNull(df);
    assertTrue(df == 2);
    val = (Double) another.get("tf-idf");
    assertTrue("tfIdf is null and it shouldn't be", val != null);
    assertTrue(val + " does not equal: " + 0.5, val == 0.5);
    vec = (NamedList) doc.get("test_basictv");
    assertNotNull(vec);
    assertEquals(vec.size(), 2);
    another = (NamedList) vec.get("anoth");
    offsets = (NamedList) another.get("offsets");
    assertNull(offsets);
    pos = (NamedList) another.get("positions");
    assertNull(pos);
    df = (Integer) another.get("df");
    assertNull(df);
    val = (Double) another.get("tf-idf");
    assertNull(val);
    val = (Double) another.get("tf");
    assertNull(val);
    //Now validate we have error messages
    NamedList warnings = (NamedList) termVectors.get("warnings");
    assertNotNull(warnings);
    List<String> theList;
    theList = (List<String>) warnings.get("noTermVectors");
    assertNotNull(theList);
    assertEquals(theList.size(), 1);
    theList = (List<String>) warnings.get("noPositions");
    assertNotNull(theList);
    assertEquals(theList.size(), 2);
    theList = (List<String>) warnings.get("noOffsets");
    assertNotNull(theList);
    assertEquals(theList.size(), 2);
  }

  @Test
  public void testNoFields() throws Exception {
    ignoreException("undefined field: foo");
    SolrCore core = h.getCore();
    SearchComponent tvComp = core.getSearchComponent("tvComponent");
    assertTrue("tvComp is null and it shouldn't be", tvComp != null);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, "id:0");
    params.add(CommonParams.QT, "tvrh");
    params.add(TermVectorParams.TF, "true");
    //Pass in a field that doesn't exist on the doc, thus, no vectors should be returned
    params.add(TermVectorParams.FIELDS, "foo");
    params.add(TermVectorComponent.COMPONENT_NAME, "true");
    SolrRequestHandler handler = core.getRequestHandler("tvrh");
    SolrQueryResponse rsp;
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    handler.handleRequest(new LocalSolrQueryRequest(core, params), rsp);
    Exception exception = rsp.getException();
    assertNotNull(exception);
    resetExceptionIgnores();
  }




  @Test
  public void testDistributed() throws Exception {
    SolrCore core = h.getCore();
    TermVectorComponent tvComp = (TermVectorComponent) core.getSearchComponent("tvComponent");
    assertTrue("tvComp is null and it shouldn't be", tvComp != null);
    ModifiableSolrParams params = new ModifiableSolrParams();
    ResponseBuilder rb = new ResponseBuilder();
    rb.stage = ResponseBuilder.STAGE_GET_FIELDS;
    rb.shards = new String[]{"localhost:0", "localhost:1", "localhost:2", "localhost:3"};//we don't actually call these, since we are going to invoke distributedProcess directly
    rb.resultIds = new HashMap<Object, ShardDoc>();
    rb.components = new ArrayList<SearchComponent>();
    rb.components.add(tvComp);
    params.add(CommonParams.Q, "id:0");
    params.add(CommonParams.QT, "tvrh");
    params.add(TermVectorParams.TF, "true");
    params.add(TermVectorParams.DF, "true");
    params.add(TermVectorParams.OFFSETS, "true");
    params.add(TermVectorParams.POSITIONS, "true");
    params.add(TermVectorComponent.COMPONENT_NAME, "true");
    rb.req = new LocalSolrQueryRequest(core, params);
    rb.outgoing = new ArrayList<ShardRequest>();
    //one doc per shard, but make sure there are enough docs to go around
    for (int i = 0; i < rb.shards.length; i++){
      ShardDoc doc = new ShardDoc();
      doc.id = i; //must be a valid doc that was indexed.
      doc.score = 1 - (i / (float)rb.shards.length);
      doc.positionInResponse = i;
      doc.shard = rb.shards[i];
      doc.orderInShard = 0;
      rb.resultIds.put(doc.id, doc);
    }

    int result = tvComp.distributedProcess(rb);
    assertTrue(result + " does not equal: " + ResponseBuilder.STAGE_DONE, result == ResponseBuilder.STAGE_DONE);
    //one outgoing per shard
    assertTrue("rb.outgoing Size: " + rb.outgoing.size() + " is not: " + rb.shards.length, rb.outgoing.size() == rb.shards.length);
    for (ShardRequest request : rb.outgoing) {
      ModifiableSolrParams solrParams = request.params;
      log.info("Shard: " + Arrays.asList(request.shards) + " Params: " + solrParams);
    }
  }

}





/*
* <field name="test_basictv" type="text" termVectors="true"/>
   <field name="test_notv" type="text" termVectors="false"/>
   <field name="test_postv" type="text" termVectors="true" termPositions="true"/>
   <field name="test_offtv" type="text" termVectors="true" termOffsets="true"/>
   <field name="test_posofftv" type="text" termVectors="true"
     termPositions="true" termOffsets="true"/>
*
* */
