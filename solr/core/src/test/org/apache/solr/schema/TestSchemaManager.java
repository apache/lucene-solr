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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.CommandOperation;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

public class TestSchemaManager extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-tiny.xml");
  }

  @Test
  public void testParsing() throws IOException {
    String x = "{\n" +
        " 'add-field' : {\n" +
        "              'name':'a',\n" +
        "              'type': 'string',\n" +
        "              'stored':true,\n" +
        "              'indexed':false\n" +
        "              },\n" +
        " 'add-field' : {\n" +
        "              'name':'b',\n" +
        "              'type': 'string',\n" +
        "              'stored':true,\n" +
        "              'indexed':false\n" +
        "              }\n" +
        "\n" +
        "}";

    List<CommandOperation> ops = CommandOperation.parse(new StringReader(json(x)));
    assertEquals(2,ops.size());
    assertTrue( CommandOperation.captureErrors(ops).isEmpty());

    x = " {'add-field' : [{\n" +
        "                       'name':'a1',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':false\n" +
        "                      },\n" +
        "                      {\n" +
        "                       'name':'a2',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':true\n" +
        "                      }]\n" +
        "      }";
    ops = CommandOperation.parse(new StringReader(json(x)));
    assertEquals(2,ops.size());
    assertTrue(CommandOperation.captureErrors(ops).isEmpty());
  }
}
