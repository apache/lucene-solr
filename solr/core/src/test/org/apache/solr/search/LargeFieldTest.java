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

package org.apache.solr.search;

import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LazyDocument;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.IndexSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LargeFieldTest extends SolrTestCaseJ4 {

  private static final String ID_FLD = "str"; // TODO alter underlying schema to be 'id'
  private static final String LAZY_FIELD = "lazyField";
  private static final String BIG_FIELD = "bigField";

  @BeforeClass
  @SuppressWarnings({"unchecked"})
  public static void initManagedSchemaCore() throws Exception {
    // This testing approach means no schema file or per-test temp solr-home!
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("managed.schema.resourceName", "schema-one-field-no-dynamic-field-unique-key.xml");
    System.setProperty("enable.update.log", "false");
    System.setProperty("documentCache.enabled", "true");
    System.setProperty("enableLazyFieldLoading", "true");

    initCore("solrconfig-managed-schema.xml", "ignoredSchemaName");

    // TODO SOLR-10229 will make this easier
    boolean PERSIST_FALSE = false; // don't write to test resource dir
    IndexSchema schema = h.getCore().getLatestSchema();
    schema = schema.addFieldTypes(Collections.singletonList(
        schema.newFieldType("textType", "solr.TextField", // redundant; TODO improve api
            map("name", "textType",   "class", "solr.TextField",
                "analyzer", map("class", "org.apache.lucene.analysis.standard.StandardAnalyzer")))),
        PERSIST_FALSE);
    schema = schema.addFields(Arrays.asList(
        schema.newField(LAZY_FIELD, "textType", map()),
        schema.newField(BIG_FIELD, "textType", map("large", true))),
        Collections.emptyMap(),
        PERSIST_FALSE);

    h.getCore().setLatestSchema(schema);
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("documentCache.enabled");
    System.clearProperty("enableLazyFieldLoading");
  }

  @Test
  public void test() throws Exception {
    // add just one document (docid 0)
    assertU(adoc(ID_FLD, "101", LAZY_FIELD, "lzy", BIG_FIELD, "big document field one"));
    assertU(commit());

    // trigger the ID_FLD to get into the doc cache; don't reference other fields
    assertQ(req("q", "101", "df", ID_FLD, "fl", ID_FLD)); // eager load ID_FLD; rest are lazy

    // fetch the document; we know it will be from the documentCache, docId 0
    final Document d = h.getCore().withSearcher(searcher -> searcher.doc(0));

    assertEager(d, ID_FLD);
    assertLazyNotLoaded(d, LAZY_FIELD);
    assertLazyNotLoaded(d, BIG_FIELD);

    assertQ(req("q", "101", "df", ID_FLD, "fl", LAZY_FIELD)); // trigger load of LAZY_FIELD

    assertEager(d, ID_FLD);
    assertLazyLoaded(d, LAZY_FIELD); // loaded now
    assertLazyNotLoaded(d, BIG_FIELD); // because big fields are handled separately

    assertQ(req("q", "101", "df", ID_FLD, "fl", BIG_FIELD)); // trigger load of BIG_FIELD

    assertEager(d, ID_FLD);
    assertLazyLoaded(d, LAZY_FIELD);
    assertLazyLoaded(d, BIG_FIELD); // loaded now
  }

  private void assertEager(Document d, String fieldName) {
    assertFalse( d.getField(fieldName) instanceof LazyDocument.LazyField);
  }

  private void assertLazyNotLoaded(Document d, String fieldName) {
    IndexableField field = d.getField(fieldName);
    if (fieldName == BIG_FIELD) {
      assertTrue(field instanceof SolrDocumentFetcher.LargeLazyField);
      assertFalse(((SolrDocumentFetcher.LargeLazyField)field).hasBeenLoaded());
    } else {
      assertTrue(field instanceof LazyDocument.LazyField);
      assertFalse(((LazyDocument.LazyField)field).hasBeenLoaded());
    }
  }

  private void assertLazyLoaded(Document d, String fieldName) {
    IndexableField field = d.getField(fieldName);
    if (fieldName == BIG_FIELD) {
      assertTrue(field instanceof SolrDocumentFetcher.LargeLazyField);
      assertTrue(((SolrDocumentFetcher.LargeLazyField)field).hasBeenLoaded());
    } else {
      assertTrue(field instanceof LazyDocument.LazyField);
      assertTrue(((LazyDocument.LazyField)field).hasBeenLoaded());
    }
  }
}
