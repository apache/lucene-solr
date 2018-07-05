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
  private static final char NUM_SEP_CHAR = ',';
  private static final String[] childrenIds = { "2", "3" };
  private static final String grandChildId = "4";
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
      "        }\n" +
      "    }\n" +
      "}";

  private static final String noIdChildren = "{\n" +
      "    \"add\": {\n" +
      "        \"doc\": {\n" +
      "            \"id\": \"1\",\n" +
      "            \"children\": [\n" +
      "                {\n" +
      "                    \"foo_s\": \"Yaz\"\n" +
      "                    \"grandChild\": \n" +
      "                          {\n" +
      "                             \"foo_s\": \"Jazz\"\n" +
      "                          },\n" +
      "                },\n" +
      "                {\n" +
      "                    \"foo_s\": \"Bar\"\n" +
      "                }\n" +
      "            ]\n" +
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
    initCore("solrconfig-update-processor-chains.xml", "schema15.xml");
  }

  @Before
  public void before() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testDeeplyNestedURPGrandChild() throws Exception {
    indexSampleData(jDoc);

    assertJQ(req("q", IndexSchema.NEST_PATH_FIELD_NAME + ":*" + PATH_SEP_CHAR + "grandChild" + NUM_SEP_CHAR + "*" + NUM_SEP_CHAR,
        "fl","*",
        "sort","id desc",
        "wt","json"),
        "/response/docs/[0]/id=='" + grandChildId + "'");
  }

  @Test
  public void testDeeplyNestedURPChildren() throws Exception {
    final String[] childrenTests = {"/response/docs/[0]/id=='" + childrenIds[0] + "'", "/response/docs/[1]/id=='" + childrenIds[1] + "'"};
    indexSampleData(jDoc);

    assertJQ(req("q", IndexSchema.NEST_PATH_FIELD_NAME + ":children" + NUM_SEP_CHAR + "*" + NUM_SEP_CHAR,
        "fl","*",
        "sort","id asc",
        "wt","json"),
        childrenTests);
  }

  @Test
  public void testDeeplyNestedURPChildrenWoId() throws Exception {
    final String rootId = "1";
    final String childKey = "grandChild";
    final String expectedId = rootId + PATH_SEP_CHAR + "children" + NUM_SEP_CHAR + "1" + PATH_SEP_CHAR + childKey + NUM_SEP_CHAR + "0";
    SolrInputDocument noIdChildren = sdoc("id", rootId, "children", sdocs(sdoc("name_s", "Yaz"), sdoc("name_s", "Jazz", childKey, sdoc("names_s", "Gaz"))));
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
    updateJ(cmd, params("update.chain", "nested"));
    assertU(commit());
  }
}
