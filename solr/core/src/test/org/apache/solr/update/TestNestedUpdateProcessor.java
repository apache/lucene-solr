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

package org.apache.solr.update;

import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.processor.NestedUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestNestedUpdateProcessor extends SolrTestCaseJ4 {

  private static final char PATH_SEP_CHAR = '/';
  private static final char NUM_SEP_CHAR = '#';
  private static final String SINGLE_VAL_CHAR = "";
  private static final String grandChildId = "4";
  private static final String secondChildList = "anotherChildList";
  private static final String jDoc = "{\n" +
      "    \"add\": {\n" +
      "        \"doc\": {\n" +
      "            \"id\": \"1\",\n" +
      "            \"children\": [\n" +
      "                {\n" +
      "                    \"id\": \"2\",\n" +
      "                    \"foo_s\": \"Yaz\"\n" +
      "                    \"grandChild\": \n" +
      "                          {\n" +
      "                             \"id\": \""+ grandChildId + "\",\n" +
      "                             \"foo_s\": \"Jazz\"\n" +
      "                          },\n" +
      "                },\n" +
      "                {\n" +
      "                    \"id\": \"3\",\n" +
      "                    \"foo_s\": \"Bar\"\n" +
      "                }\n" +
      "            ]\n" +
                   secondChildList + ": [{\"id\": \"4\", \"last_s\": \"Smith\"}],\n" +
      "        }\n" +
      "    }\n" +
      "}";

  private static final String errDoc = "{\n" +
      "    \"add\": {\n" +
      "        \"doc\": {\n" +
      "            \"id\": \"1\",\n" +
      "            \"children" + PATH_SEP_CHAR + "a\": [\n" +
      "                {\n" +
      "                    \"id\": \"2\",\n" +
      "                    \"foo_s\": \"Yaz\"\n" +
      "                    \"grandChild\": \n" +
      "                          {\n" +
      "                             \"id\": \""+ grandChildId + "\",\n" +
      "                             \"foo_s\": \"Jazz\"\n" +
      "                          },\n" +
      "                },\n" +
      "                {\n" +
      "                    \"id\": \"3\",\n" +
      "                    \"foo_s\": \"Bar\"\n" +
      "                }\n" +
      "            ]\n" +
      "        }\n" +
      "    }\n" +
      "}";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-nest.xml");
  }

  @Before
  public void before() throws Exception {
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testDeeplyNestedURPGrandChild() throws Exception {
    final String[] tests = {
        "/response/docs/[0]/id=='4'",
        "/response/docs/[0]/" + IndexSchema.NEST_PATH_FIELD_NAME + "=='/children#0/grandChild#'"
    };
    indexSampleData(jDoc);

    assertJQ(req("q", IndexSchema.NEST_PATH_FIELD_NAME + ":*/grandChild",
        "fl","*, _nest_path_",
        "sort","id desc",
        "wt","json"),
        tests);
  }

  @Test
  public void testNumberInName() throws Exception {
    // child named "grandChild99"  (has a number in it)
    indexSampleData(jDoc.replace("grandChild", "grandChild99"));
    //assertQ(req("qt", "/terms", "terms", "true", "terms.fl", IndexSchema.NEST_PATH_FIELD_NAME), "false"); // for debugging

    // find it
    assertJQ(req("q", "{!field f=" + IndexSchema.NEST_PATH_FIELD_NAME + "}/children/grandChild99"),
        "/response/numFound==1");
    // should *NOT* find it; different number
    assertJQ(req("q", "{!field f=" + IndexSchema.NEST_PATH_FIELD_NAME + "}/children/grandChild22"),
        "/response/numFound==0");

  }

  @Test
  public void testDeeplyNestedURPChildren() throws Exception {
    final String[] childrenTests = {
        "/response/docs/[0]/id=='2'",
        "/response/docs/[1]/id=='3'",
        "/response/docs/[0]/" + IndexSchema.NEST_PATH_FIELD_NAME + "=='/children#0'",
        "/response/docs/[1]/" + IndexSchema.NEST_PATH_FIELD_NAME + "=='/children#1'"
    };
    indexSampleData(jDoc);

    assertJQ(req("q", IndexSchema.NEST_PATH_FIELD_NAME + ":\\/children",
        "fl","*, _nest_path_",
        "sort","id asc",
        "wt","json"),
        childrenTests);

    assertJQ(req("q", IndexSchema.NEST_PATH_FIELD_NAME + ":\\/anotherChildList",
        "fl","*, _nest_path_",
        "sort","id asc",
        "wt","json"),
        "/response/docs/[0]/id=='4'",
        "/response/docs/[0]/" + IndexSchema.NEST_PATH_FIELD_NAME + "=='/anotherChildList#0'");
  }

  @Test
  public void testDeeplyNestedURPSanity() throws Exception {
    SolrInputDocument docHierarchy = sdoc("id", "1", "children", sdocs(sdoc("id", "2", "name_s", "Yaz"),
        sdoc("id", "3", "name_s", "Jazz", "grandChild", sdoc("id", "4", "name_s", "Gaz"))), "lonelyChild", sdoc("id", "5", "name_s", "Loner"));
    UpdateRequestProcessor nestedUpdate = new NestedUpdateProcessorFactory().getInstance(req(), null, null);
    AddUpdateCommand cmd = new AddUpdateCommand(req());
    cmd.solrDoc = docHierarchy;
    nestedUpdate.processAdd(cmd);
    cmd.clear();

    List children = (List) docHierarchy.get("children").getValues();

    SolrInputDocument firstChild = (SolrInputDocument) children.get(0);
    assertEquals("SolrInputDocument(fields: [id=2, name_s=Yaz, _nest_path_=/children#0, _nest_parent_=1])", firstChild.toString());

    SolrInputDocument secondChild = (SolrInputDocument) children.get(1);
    assertEquals("SolrInputDocument(fields: [id=3, name_s=Jazz, grandChild=SolrInputDocument(fields: [id=4, name_s=Gaz, _nest_path_=/children#1/grandChild#, _nest_parent_=3]), _nest_path_=/children#1, _nest_parent_=1])", secondChild.toString());

    SolrInputDocument grandChild = (SolrInputDocument)((SolrInputDocument) children.get(1)).get("grandChild").getValue();
    assertEquals("SolrInputDocument(fields: [id=4, name_s=Gaz, _nest_path_=/children#1/grandChild#, _nest_parent_=3])", grandChild.toString());

    SolrInputDocument singularChild = (SolrInputDocument) docHierarchy.get("lonelyChild").getValue();
    assertEquals("SolrInputDocument(fields: [id=5, name_s=Loner, _nest_path_=/lonelyChild#, _nest_parent_=1])", singularChild.toString());
  }

  @Test
  public void testDeeplyNestedURPChildrenWoId() throws Exception {
    final String rootId = "1";
    final String childKey = "grandChild";
    final String expectedId = rootId + "/children#1/" + childKey + NUM_SEP_CHAR + SINGLE_VAL_CHAR;
    SolrInputDocument noIdChildren = sdoc("id", rootId, "children", sdocs(sdoc("name_s", "Yaz"), sdoc("name_s", "Jazz", childKey, sdoc("name_s", "Gaz"))));
    UpdateRequestProcessor nestedUpdate = new NestedUpdateProcessorFactory().getInstance(req(), null, null);
    AddUpdateCommand cmd = new AddUpdateCommand(req());
    cmd.solrDoc = noIdChildren;
    nestedUpdate.processAdd(cmd);
    cmd.clear();
    List children = (List) noIdChildren.get("children").getValues();
    SolrInputDocument idLessChild = (SolrInputDocument)((SolrInputDocument) children.get(1)).get(childKey).getValue();
    assertTrue("Id less child did not get an Id", idLessChild.containsKey("id"));
    assertEquals("Id less child was assigned an unexpected id", expectedId, idLessChild.getFieldValue("id").toString());
  }

  @Test
  public void testDeeplyNestedURPFieldNameException() throws Exception {
    final String errMsg = "contains: '" + PATH_SEP_CHAR + "' , which is reserved for the nested URP";
    thrown.expect(SolrException.class);
    indexSampleData(errDoc);
    thrown.expectMessage(errMsg);
  }

  private void indexSampleData(String cmd) throws Exception {
    updateJ(cmd, null);
    assertU(commit());
  }
}
