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

import java.io.File;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Requests to open a new searcher w/o any underlying change to the index exposed 
 * by the current searcher should result in the same searcher being re-used.
 *
 * Likewise, if there <em>is</em> in fact an underlying index change, we want to 
 * assert that a new searcher will in fact be opened.
 */
public class TestSearcherReuse extends SolrTestCaseJ4 {

  private static File solrHome;

  private static final String collection = "collection1";
  private static final String confPath = collection + "/conf";

  /**
   * We're using a Managed schema so we can confirm that opening a new searcher 
   * after a schema modification results in getting a new searcher with the new 
   * schema linked to it.
   */
  @BeforeClass
  private static void setupTempDirAndCoreWithManagedSchema() throws Exception {
    solrHome = createTempDir().toFile();
    solrHome = solrHome.getAbsoluteFile();

    File confDir = new File(solrHome, confPath);
    File testHomeConfDir = new File(TEST_HOME(), confPath);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig-managed-schema.xml"), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig.snippet.randomindexconfig.xml"), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-id-and-version-fields-only.xml"), confDir);

    // initCore will trigger an upgrade to managed schema, since the solrconfig has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-id-and-version-fields-only.xml", 
             solrHome.getPath());
  }

  @AfterClass
  private static void afterClass() throws Exception {
    solrHome = null;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    assertU(delQ("*:*"));
    optimize();
    assertU(commit());
  }

  public void test() throws Exception {

    // seed some docs & segments
    int numDocs = atLeast(1);
    for (int i = 1; i <= numDocs; i++) {
      // NOTE: starting at "1", we'll use id=0 later
      assertU(adoc("id", ""+i));
      if (random().nextBoolean()) {
        assertU(commit());
      }
    }

    // with random merge policies, a regular commit can cause a segment to be flushed that can kick off a background merge
    // that can cause a later commit to actually see changes and open a new searcher.  This should not be possible with optimize
    assertU(optimize());

    // seed a single query into the cache
    assertQ(req("*:*"), "//*[@numFound='"+numDocs+"']");

    final SolrQueryRequest baseReq = req("q","foo");
    try {
      // we make no index changes in this block, so the searcher should always be the same
      // NOTE: we *have* to call getSearcher() in advance, it's a delayed binding
      final SolrIndexSearcher expectedSearcher = getMainSearcher(baseReq);

      assertU(commit());
      assertSearcherHasNotChanged(expectedSearcher);

      assertU(commit("openSearcher","true"));
      assertSearcherHasNotChanged(expectedSearcher);

      assertU(commit("softCommit","true"));
      assertSearcherHasNotChanged(expectedSearcher);

      assertU(commit("softCommit","true","openSearcher","true"));
      assertSearcherHasNotChanged(expectedSearcher);

      assertU(delQ("id:match_no_documents"));
      assertU(commit("softCommit","true","openSearcher","true"));
      assertSearcherHasNotChanged(expectedSearcher);

      assertU(delI("0")); // no doc has this id, yet
      assertU(commit("softCommit","true","openSearcher","true"));
      assertSearcherHasNotChanged(expectedSearcher);

    } finally {
      baseReq.close();
    }

    // now do a variety of things that *should* always guarantee a new searcher
    SolrQueryRequest beforeReq;

    beforeReq = req("q","foo");
    try {
      // NOTE: we *have* to call getSearcher() in advance: delayed binding
      SolrIndexSearcher before = getMainSearcher(beforeReq);

      assertU(delI("1"));
      assertU(commit());
      assertSearcherHasChanged(before);
    } finally {
      beforeReq.close();
    }
    
    beforeReq = req("q","foo");
    try {
      // NOTE: we *have* to call getSearcher() in advance: delayed binding
      SolrIndexSearcher before = getMainSearcher(beforeReq);

      assertU(adoc("id", "0"));
      assertU(commit());
      assertSearcherHasChanged(before);
    } finally {
      beforeReq.close();
    }

    beforeReq = req("q","foo");
    try {
      // NOTE: we *have* to call getSearcher() in advance: delayed binding
      SolrIndexSearcher before = getMainSearcher(beforeReq);

      assertU(delQ("id:[0 TO 5]"));
      assertU(commit());
      assertSearcherHasChanged(before);
    } finally {
      beforeReq.close();
    }

    beforeReq = req("q","foo");
    try {
      // NOTE: we *have* to call getSearcher() in advance: delayed binding
      SolrIndexSearcher before = getMainSearcher(beforeReq);

      // create a new field & add it.
      assertTrue("schema not mutable", beforeReq.getSchema().isMutable());
      ManagedIndexSchema oldSchema = (ManagedIndexSchema) beforeReq.getSchema();
      SchemaField newField = oldSchema.newField
        ("hoss", "string", Collections.<String,Object>emptyMap());
      IndexSchema newSchema = oldSchema.addField(newField);
      h.getCore().setLatestSchema(newSchema);

      // sanity check, later asserts assume this
      assertNotSame(oldSchema, newSchema); 

      // the schema has changed - but nothing has requested a new Searcher yet
      assertSearcherHasNotChanged(before);

      // only now should we get a new searcher...
      assertU(commit("softCommit","true","openSearcher","true"));
      assertSearcherHasChanged(before);

      // sanity that opening the new searcher was useful to get new schema...
      SolrQueryRequest afterReq = req("q","foo");
      try {
        assertSame(newSchema, afterReq.getSchema());
        assertSame(newSchema, getMainSearcher(afterReq).getSchema());
      } finally {
        afterReq.close();
      }

    } finally {
      beforeReq.close();
    }

  }
 
  /**
   * Helper method to get the searcher from a request, and assert that it's the main searcher
   */
  public static SolrIndexSearcher getMainSearcher(SolrQueryRequest req) {
    SolrIndexSearcher s = req.getSearcher();
    assertMainSearcher(s);
    return s;
  }

  /**
   * Sanity check that we didn't get a realtime (non-caching) searcher
   */
  public static void assertMainSearcher(SolrIndexSearcher s) {
    assertTrue("Searcher isn't 'main': " + s.toString(),
               // TODO brittle, better solution?
               s.toString().contains(" main{"));
    assertTrue("Searcher is non-caching", s.isCachingEnabled());
  }
 
  /**
   * Given an existing searcher, creates a new SolrRequest, and verifies that the 
   * searcher in that request is <b>not</b> the same as the previous searcher -- 
   * cleaningly closing the new SolrRequest either way.
   */
  public static void assertSearcherHasChanged(SolrIndexSearcher previous) {
    SolrQueryRequest req = req("*:*");
    try {
      SolrIndexSearcher newSearcher = getMainSearcher(req);
      assertNotSame(previous, newSearcher);
    } finally {
      req.close();
    }
  }

  /**
   * Given an existing searcher, creates a new SolrRequest, and verifies that the 
   * searcher in that request is the same as the expected searcher -- cleaningly 
   * closing the new SolrRequest either way.
   */
  public static void assertSearcherHasNotChanged(SolrIndexSearcher expected) {
    SolrQueryRequest req = req("*:*");
    try {
      SolrIndexSearcher newSearcher = getMainSearcher(req);
      assertSame(expected, newSearcher);
    } finally {
      req.close();
    }
  }

}
