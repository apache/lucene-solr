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
package org.apache.solr;

import org.apache.solr.schema.SchemaField;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_START;

import org.junit.Before;
import org.junit.After;

/**
 * Tests that cursor requests fail unless the IndexSchema defines a uniqueKey.
 */
public class TestCursorMarkWithoutUniqueKey extends SolrTestCaseJ4 {

  public final static String TEST_SOLRCONFIG_NAME = "solrconfig-minimal.xml";
  public final static String TEST_SCHEMAXML_NAME = "schema-minimal.xml";

  @Before
  public void beforeSetupCore() throws Exception {
    System.setProperty("solr.test.useFilterForSortedQuery", Boolean.toString(random().nextBoolean()));
    System.setProperty("solr.test.useFilterForSortedQuery", Boolean.toString(random().nextBoolean()));
    initCore(TEST_SOLRCONFIG_NAME, TEST_SCHEMAXML_NAME);
    SchemaField uniqueKeyField = h.getCore().getLatestSchema().getUniqueKeyField();
    assertNull("This test requires that the schema not have a uniquekey field -- someone violated that in " + TEST_SCHEMAXML_NAME, uniqueKeyField);
  }

  @After
  public void afterDestroyCore() throws Exception {
    deleteCore();
  }
  

  public void test() throws Exception {

    assertU(adoc("fld", "val"));
    assertU(commit());

    try {
      ignoreException("Cursor functionality is not available unless the IndexSchema defines a uniqueKey field");
      expectThrows(RuntimeException.class,
          "No exception when querying with a cursorMark with no uniqueKey defined.",
          () -> assertQ(req("q", "*:*", "sort", "fld desc", "cursorMark", CURSOR_MARK_START))
      );
    } finally {
      unIgnoreException("Cursor functionality is not available unless the IndexSchema defines a uniqueKey field");
    }
  }
}
