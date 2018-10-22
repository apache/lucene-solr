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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;

/**
 * Added in SOLR-10047
 */
public class TestHalfAndHalfDocValues extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    // we need consistent segments that aren't merged because we want to have
    // segments with and without docvalues
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());

    initCore("solrconfig-basic.xml", "schema-docValues.xml");

    // sanity check our schema meets our expectations
    final IndexSchema schema = h.getCore().getLatestSchema();
    for (String f : new String[]{"floatdv", "intdv", "doubledv", "longdv", "datedv", "stringdv", "booldv"}) {
      final SchemaField sf = schema.getField(f);
      assertFalse(f + " is multiValued, test is useless, who changed the schema?",
          sf.multiValued());
      assertFalse(f + " is indexed, test is useless, who changed the schema?",
          sf.indexed());
      assertTrue(f + " has no docValues, test is useless, who changed the schema?",
          sf.hasDocValues());
    }
  }

  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
  }

  public void testHalfAndHalfDocValues() throws Exception {
    // Insert two docs without docvalues
    String fieldname = "string_add_dv_later";
    assertU(adoc("id", "3", fieldname, "c"));
    assertU(commit());
    assertU(adoc("id", "1", fieldname, "a"));
    assertU(commit());


    try (SolrCore core = h.getCoreInc()) {
      assertFalse(core.getLatestSchema().getField(fieldname).hasDocValues());
      // Add docvalues to the field type
      IndexSchema schema = core.getLatestSchema();
      SchemaField oldField = schema.getField(fieldname);
      int newProperties = oldField.getProperties() | SchemaField.DOC_VALUES;

      SchemaField sf = new SchemaField(fieldname, oldField.getType(), newProperties, null);
      schema.getFields().put(fieldname, sf);

      // Insert a new doc with docvalues
      assertU(adoc("id", "2", fieldname, "b"));
      assertU(commit());


      // Check there are a mix of segments with and without docvalues
      final RefCounted<SolrIndexSearcher> searcherRef = core.openNewSearcher(true, true);
      final SolrIndexSearcher searcher = searcherRef.get();
      try {
        final DirectoryReader topReader = searcher.getRawReader();

        //Assert no merges

        assertEquals(3, topReader.numDocs());
        assertEquals(3, topReader.leaves().size());

        final FieldInfos infos = FieldInfos.getMergedFieldInfos(topReader);
        //The global field type should have docValues because a document with dvs was added
        assertEquals(DocValuesType.SORTED, infos.fieldInfo(fieldname).getDocValuesType());

        for (LeafReaderContext ctx : topReader.leaves()) {
          LeafReader r = ctx.reader();
          //Make sure there were no merges
          assertEquals(1, r.numDocs());
          Document doc = r.document(0);
          String id = doc.getField("id").stringValue();

          if (id.equals("1") || id.equals("3")) {
            assertEquals(DocValuesType.NONE, r.getFieldInfos().fieldInfo(fieldname).getDocValuesType());
          } else {
            assertEquals(DocValuesType.SORTED, r.getFieldInfos().fieldInfo(fieldname).getDocValuesType());
          }

        }
      } finally {
        searcherRef.decref();
      }
    }

    // Assert sort order is correct
    assertQ(req("q", "string_add_dv_later:*", "sort", "string_add_dv_later asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]",
        "//result/doc[3]/str[@name='id'][.=3]"
    );
  }

}
