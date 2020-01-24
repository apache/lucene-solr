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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.TermVectorParams;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 *
 *
 **/
public class TermVectorComponentTest extends SolrTestCaseJ4 {

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  // ensure that we operate correctly with all valid combinations of the uniqueKey being
  // stored and/or in docValues.
  @BeforeClass
  public static void beforeClass() throws Exception {
    switch (random().nextInt(3)) {
      case 0:
        System.setProperty("solr.tests.id.stored", "true");
        System.setProperty("solr.tests.id.docValues", "true");
        break;
      case 1:
        System.setProperty("solr.tests.id.stored", "true");
        System.setProperty("solr.tests.id.docValues", "false");
        break;
      case 2:
        System.setProperty("solr.tests.id.stored", "false");
        System.setProperty("solr.tests.id.docValues", "true");
        break;
      default:
        fail("Bad random number generatged not between 0-2 iunclusive");
        break;
    }
    initCore("solrconfig.xml", "schema.xml");
  }

  static String tv = "/tvrh";

  @Test
  public void testCanned() throws Exception {

    clearIndex();
    assertU(adoc("id", "0",
        "test_posoffpaytv", "This is a title and another title",
        "test_posofftv", "This is a title and another title",
        "test_basictv", "This is a title and another title",
        "test_notv", "This is a title and another title",
        "test_postv", "This is a title and another title",
        "test_offtv", "This is a title and another title"
    ));
    assertU(adoc("id", "1",
        "test_posoffpaytv", "The quick reb fox jumped over the lazy brown dogs.",
        "test_posofftv", "The quick reb fox jumped over the lazy brown dogs.",
        "test_basictv", "The quick reb fox jumped over the lazy brown dogs.",
        "test_notv", "The quick reb fox jumped over the lazy brown dogs.",
        "test_postv", "The quick reb fox jumped over the lazy brown dogs.",
        "test_offtv", "The quick reb fox jumped over the lazy brown dogs."
    ));
    assertU(adoc("id", "2",
        "test_posoffpaytv", "This is a document",
        "test_posofftv", "This is a document",
        "test_basictv", "This is a document",
        "test_notv", "This is a document",
        "test_postv", "This is a document",
        "test_offtv", "This is a document"
    ));
    assertU(adoc("id", "3",
        "test_posoffpaytv", "another document",
        "test_posofftv", "another document",
        "test_basictv", "another document",
        "test_notv", "another document",
        "test_postv", "another document",
        "test_offtv", "another document"
    ));
    //bunch of docs that are variants on blue
    assertU(adoc("id", "4",
        "test_posoffpaytv", "blue",
        "test_posofftv", "blue",
        "test_basictv", "blue",
        "test_notv", "blue",
        "test_postv", "blue",
        "test_offtv", "blue"
    ));
    assertU(adoc("id", "5",
        "test_posoffpaytv", "blud",
        "test_posofftv", "blud",
        "test_basictv", "blud",
        "test_notv", "blud",
        "test_postv", "blud",
        "test_offtv", "blud"
    ));
    assertU(adoc("id", "6",
        "test_posoffpaytv", "boue",
        "test_posofftv", "boue",
        "test_basictv", "boue",
        "test_notv", "boue",
        "test_postv", "boue",
        "test_offtv", "boue"
    ));
    assertU(adoc("id", "7",
        "test_posoffpaytv", "glue",
        "test_posofftv", "glue",
        "test_basictv", "glue",
        "test_notv", "glue",
        "test_postv", "glue",
        "test_offtv", "glue"
    ));
    assertU(adoc("id", "8",
        "test_posoffpaytv", "blee",
        "test_posofftv", "blee",
        "test_basictv", "blee",
        "test_notv", "blee",
        "test_postv", "blee",
        "test_offtv", "blee"
    ));
    assertU(adoc("id", "9",
        "test_posoffpaytv", "blah",
        "test_posofftv", "blah",
        "test_basictv", "blah",
        "test_notv", "blah",
        "test_postv", "blah",
        "test_offtv", "blah"
    ));

    assertNull(h.validateUpdate(commit()));
    doBasics();
    doOptions();
    doPerField();
    doPayloads();
  }


  private void doBasics() throws Exception {
    assertJQ(req("json.nl", "map", "qt", tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true", TermVectorParams.TF, "true")
        , "/termVectors=={'0':{'uniqueKey':'0'," +
            " 'test_basictv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_offtv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_posofftv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_posoffpaytv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_postv':{'anoth':{'tf':1},'titl':{'tf':2}}}}"
    );
    // tv.fl diff from fl
    assertJQ(req("json.nl", "map",
        "qt", tv,
        "q", "id:0",
        "fl", "*,score",
        "tv.fl", "test_basictv,test_offtv",
        TermVectorComponent.COMPONENT_NAME, "true",
        TermVectorParams.TF, "true")
        , "/termVectors=={'0':{'uniqueKey':'0'," +
            " 'test_basictv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_offtv':{'anoth':{'tf':1},'titl':{'tf':2}}}}"
    );
    // multi-valued tv.fl 
    assertJQ(req("json.nl", "map",
        "qt", tv,
        "q", "id:0",
        "fl", "*,score",
        "tv.fl", "test_basictv",
        "tv.fl", "test_offtv",
        TermVectorComponent.COMPONENT_NAME, "true",
        TermVectorParams.TF, "true")
        , "/termVectors=={'0':{'uniqueKey':'0'," +
            " 'test_basictv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_offtv':{'anoth':{'tf':1},'titl':{'tf':2}}}}"
    );
    // re-use fl glob
    assertJQ(req("json.nl", "map",
        "qt", tv,
        "q", "id:0",
        "fl", "*,score",
        TermVectorComponent.COMPONENT_NAME, "true",
        TermVectorParams.TF, "true")
        , "/termVectors=={'0':{'uniqueKey':'0'," +
            " 'test_basictv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_offtv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_posofftv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_posoffpaytv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_postv':{'anoth':{'tf':1},'titl':{'tf':2}}}}"
    );
    // re-use fl, ignore things we can't handle
    assertJQ(req("json.nl", "map",
        "qt", tv,
        "q", "id:0",
        "fl", "score,test_basictv,[docid],test_postv,val:sum(3,4)",
        TermVectorComponent.COMPONENT_NAME, "true",
        TermVectorParams.TF, "true")
        , "/termVectors=={'0':{'uniqueKey':'0'," +
            " 'test_basictv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_postv':{'anoth':{'tf':1},'titl':{'tf':2}}}}"
    );
    // re-use (multi-valued) fl, ignore things we can't handle
    assertJQ(req("json.nl", "map",
        "qt", tv,
        "q", "id:0",
        "fl", "score,test_basictv",
        "fl", "[docid],test_postv,val:sum(3,4)",
        TermVectorComponent.COMPONENT_NAME, "true",
        TermVectorParams.TF, "true")
        , "/termVectors=={'0':{'uniqueKey':'0'," +
            " 'test_basictv':{'anoth':{'tf':1},'titl':{'tf':2}}," +
            " 'test_postv':{'anoth':{'tf':1},'titl':{'tf':2}}}}"
    );

  }

  private void doOptions() throws Exception {
    assertJQ(req("json.nl", "map", "qt", tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"
        , TermVectorParams.TF, "true", TermVectorParams.DF, "true", TermVectorParams.OFFSETS, "true", TermVectorParams.POSITIONS, "true", TermVectorParams.TF_IDF, "true")
        , "/termVectors/0/test_posofftv/anoth=={'tf':1, 'offsets':{'start':20, 'end':27}, 'positions':{'position':5}, 'df':2, 'tf-idf':0.5}"
    );

    assertJQ(req("json.nl", "map", "qt", tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"
        , TermVectorParams.ALL, "true")
        , "/termVectors/0/test_posofftv/anoth=={'tf':1, 'offsets':{'start':20, 'end':27}, 'positions':{'position':5}, 'df':2, 'tf-idf':0.5}"
    );

    // test each combination at random
    final List<String> list = new ArrayList<>();
    list.addAll(Arrays.asList("json.nl", "map", "qt", tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"));
    String[][] options = new String[][]{{TermVectorParams.TF, "'tf':1"},
        {TermVectorParams.OFFSETS, "'offsets':{'start':20, 'end':27}"},
        {TermVectorParams.POSITIONS, "'positions':{'position':5}"},
        {TermVectorParams.DF, "'df':2"},
        {TermVectorParams.TF_IDF, "'tf-idf':0.5"}};
    StringBuilder expected = new StringBuilder("/termVectors/0/test_posofftv/anoth=={");
    boolean first = true;
    for (int i = 0; i < options.length; i++) {
      final boolean use = random().nextBoolean();
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

  private void doPerField() throws Exception {
    assertJQ(req("json.nl", "map", "qt", tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"
        , TermVectorParams.TF, "true", TermVectorParams.DF, "true", TermVectorParams.OFFSETS, "true", TermVectorParams.POSITIONS, "true", TermVectorParams.TF_IDF, "true"
        , TermVectorParams.FIELDS, "test_basictv,test_notv,test_postv,test_offtv,test_posofftv,test_posoffpaytv"
        , "f.test_posoffpaytv." + TermVectorParams.PAYLOADS, "false"
        , "f.test_posofftv." + TermVectorParams.POSITIONS, "false"
        , "f.test_offtv." + TermVectorParams.OFFSETS, "false"
        , "f.test_basictv." + TermVectorParams.DF, "false"
        , "f.test_basictv." + TermVectorParams.TF, "false"
        , "f.test_basictv." + TermVectorParams.TF_IDF, "false"
        )
        , "/termVectors/0/test_basictv=={'anoth':{},'titl':{}}"
        , "/termVectors/0/test_postv/anoth=={'tf':1, 'positions':{'position':5}, 'df':2, 'tf-idf':0.5}"
        , "/termVectors/0/test_offtv/anoth=={'tf':1, 'df':2, 'tf-idf':0.5}"
        , "/termVectors/warnings=={ 'noTermVectors':['test_notv'], 'noPositions':['test_basictv', 'test_offtv'], 'noOffsets':['test_basictv', 'test_postv']}"
    );
  }

  private void doPayloads() throws Exception {
    // This field uses TokenOffsetPayloadTokenFilter, which
    // stuffs start (20) and end offset (27) into the
    // payload:
    assertJQ(req("json.nl", "map", "qt", tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true"
        , TermVectorParams.TF, "true", TermVectorParams.DF, "true", TermVectorParams.OFFSETS, "true", TermVectorParams.POSITIONS, "true", TermVectorParams.TF_IDF, "true",
        TermVectorParams.PAYLOADS, "true")
        , "/termVectors/0/test_posoffpaytv/anoth=={'tf':1, 'offsets':{'start':20, 'end':27}, 'positions':{'position':5}, 'payloads':{'payload': 'AAAAFAAAABs='}, 'df':2, 'tf-idf':0.5}"
    );
  }

  @Test
  public void testNoVectors() throws Exception {
    clearIndex();
    assertU(adoc("id", "0"));
    assertU(adoc("id", "1"));
    assertU(adoc("id", "2"));
    assertU(adoc("id", "3"));
    assertNull(h.validateUpdate(commit()));

    // Kind of an odd test, but we just want to know if we don't generate an NPE when there is nothing to give back in the term vectors.
    assertJQ(req("json.nl", "map", "qt", tv, "q", "id:0", TermVectorComponent.COMPONENT_NAME, "true", TermVectorParams.TF, "true")
        , "/termVectors=={'0':{'uniqueKey':'0'}}}"
    );


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
