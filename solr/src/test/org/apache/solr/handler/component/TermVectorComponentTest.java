package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.TermVectorParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

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

    assertU(adoc("id", "0",
            "test_posofftv", "This is a title and another title",
            "test_basictv", "This is a title and another title",
            "test_notv", "This is a title and another title",
            "test_postv", "This is a title and another title",
            "test_offtv", "This is a title and another title"
    ));
    assertU(adoc("id", "1",
            "test_posofftv", "The quick reb fox jumped over the lazy brown dogs.",
            "test_basictv", "The quick reb fox jumped over the lazy brown dogs.",
            "test_notv", "The quick reb fox jumped over the lazy brown dogs.",
            "test_postv", "The quick reb fox jumped over the lazy brown dogs.",
            "test_offtv", "The quick reb fox jumped over the lazy brown dogs."
    ));
    assertU(adoc("id", "2",
            "test_posofftv", "This is a document",
            "test_basictv", "This is a document",
            "test_notv", "This is a document",
            "test_postv", "This is a document",
            "test_offtv", "This is a document"
    ));
    assertU(adoc("id", "3",
            "test_posofftv", "another document",
            "test_basictv", "another document",
            "test_notv", "another document",
            "test_postv", "another document",
            "test_offtv", "another document"
    ));
    //bunch of docs that are variants on blue
    assertU(adoc("id", "4",
            "test_posofftv", "blue",
            "test_basictv", "blue",
            "test_notv", "blue",
            "test_postv", "blue",
            "test_offtv", "blue"
    ));
    assertU(adoc("id", "5",
            "test_posofftv", "blud",
            "test_basictv", "blud",
            "test_notv", "blud",
            "test_postv", "blud",
            "test_offtv", "blud"
    ));
    assertU(adoc("id", "6",
            "test_posofftv", "boue",
            "test_basictv", "boue",
            "test_notv", "boue",
            "test_postv", "boue",
            "test_offtv", "boue"
    ));
    assertU(adoc("id", "7",
            "test_posofftv", "glue",
            "test_basictv", "glue",
            "test_notv", "glue",
            "test_postv", "glue",
            "test_offtv", "glue"
    ));
    assertU(adoc("id", "8",
            "test_posofftv", "blee",
            "test_basictv", "blee",
            "test_notv", "blee",
            "test_postv", "blee",
            "test_offtv", "blee"
    ));
    assertU(adoc("id", "9",
            "test_posofftv", "blah",
            "test_basictv", "blah",
            "test_notv", "blah",
            "test_postv", "blah",
            "test_offtv", "blah"
    ));

    assertNull(h.validateUpdate(commit()));
  }

  static String tv = "tvrh";

  @Test
  public void testBasics() throws Exception {
    assertJQ(req("json.nl","map", "qt",tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true", TermVectorParams.TF, "true")
       ,"/termVectors=={'doc-0':{'uniqueKey':'0'," +
            " 'test_basictv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_offtv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_posofftv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_postv':{'anoth':{'tf':1},'titl':{'tf':2}}}," +
            " 'uniqueKeyFieldName':'id'}"
    );
  }

  @Test
  public void testOptions() throws Exception {
    assertJQ(req("json.nl","map", "qt",tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"
       , TermVectorParams.TF, "true", TermVectorParams.DF, "true", TermVectorParams.OFFSETS, "true", TermVectorParams.POSITIONS, "true", TermVectorParams.TF_IDF, "true")
       ,"/termVectors/doc-0/test_posofftv/anoth=={'tf':1, 'offsets':{'start':20, 'end':27}, 'positions':{'position':1}, 'df':2, 'tf-idf':0.5}"
    );
    
    assertJQ(req("json.nl","map", "qt",tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"
        , TermVectorParams.ALL, "true")
        ,"/termVectors/doc-0/test_posofftv/anoth=={'tf':1, 'offsets':{'start':20, 'end':27}, 'positions':{'position':1}, 'df':2, 'tf-idf':0.5}"
     );
    
    // test each combination at random
    final List<String> list = new ArrayList<String>();
    list.addAll(Arrays.asList("json.nl","map", "qt",tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"));
    String[][] options = new String[][] { { TermVectorParams.TF, "'tf':1" },
        { TermVectorParams.OFFSETS, "'offsets':{'start':20, 'end':27}" },
        { TermVectorParams.POSITIONS, "'positions':{'position':1}" },
        { TermVectorParams.DF, "'df':2" },
        { TermVectorParams.TF_IDF, "'tf-idf':0.5" } };
    StringBuilder expected = new StringBuilder("/termVectors/doc-0/test_posofftv/anoth=={");
    boolean first = true;
    for (int i = 0; i < options.length; i++) {
      final boolean use = random.nextBoolean();
      if (use) {
        if (!first) {
          expected.append(", ");
        }
        first = false;
        expected.append(options[i][1]);
        
      }
      list.add(options[i][0]);
      list.add(use ? "true" : "false");
    }
    
    expected.append("}");
    assertJQ(req(list.toArray(new String[0])), expected.toString());
  }

  @Test
  public void testPerField() throws Exception {
    assertJQ(req("json.nl","map", "qt",tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"
        ,TermVectorParams.TF, "true", TermVectorParams.DF, "true", TermVectorParams.OFFSETS, "true", TermVectorParams.POSITIONS, "true", TermVectorParams.TF_IDF, "true"
        ,TermVectorParams.FIELDS, "test_basictv,test_notv,test_postv,test_offtv,test_posofftv"
        ,"f.test_posofftv." + TermVectorParams.POSITIONS, "false"
        ,"f.test_offtv." + TermVectorParams.OFFSETS, "false"
        ,"f.test_basictv." + TermVectorParams.DF, "false"
        ,"f.test_basictv." + TermVectorParams.TF, "false"
        ,"f.test_basictv." + TermVectorParams.TF_IDF, "false"
        )
    ,"/termVectors/doc-0/test_basictv=={'anoth':{},'titl':{}}"
    ,"/termVectors/doc-0/test_postv/anoth=={'tf':1, 'positions':{'position':1}, 'df':2, 'tf-idf':0.5}"
    ,"/termVectors/doc-0/test_offtv/anoth=={'tf':1, 'df':2, 'tf-idf':0.5}"
    ,"/termVectors/warnings=={ 'noTermVectors':['test_notv'], 'noPositions':['test_basictv', 'test_offtv'], 'noOffsets':['test_basictv', 'test_postv']}"
    );
  }


  // TODO: this test is really fragile since it pokes around in solr's guts and makes many assumptions.
  // it should be rewritten to use the real distributed interface
  @Test
  public void testDistributed() throws Exception {
    SolrCore core = h.getCore();
    TermVectorComponent tvComp = (TermVectorComponent) core.getSearchComponent("tvComponent");
    assertTrue("tvComp is null and it shouldn't be", tvComp != null);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, "id:0");
    params.add(CommonParams.QT, "tvrh");
    params.add(TermVectorParams.TF, "true");
    params.add(TermVectorParams.DF, "true");
    params.add(TermVectorParams.OFFSETS, "true");
    params.add(TermVectorParams.POSITIONS, "true");
    params.add(TermVectorComponent.COMPONENT_NAME, "true");

    ResponseBuilder rb = new ResponseBuilder(new LocalSolrQueryRequest(core, params), new SolrQueryResponse(), (List)Arrays.asList(tvComp));
    rb.stage = ResponseBuilder.STAGE_GET_FIELDS;
    rb.shards = new String[]{"localhost:0", "localhost:1", "localhost:2", "localhost:3"};//we don't actually call these, since we are going to invoke distributedProcess directly
    rb.resultIds = new HashMap<Object, ShardDoc>();

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

    rb.req.close();
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
