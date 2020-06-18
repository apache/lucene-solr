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

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexSplitterTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  File indexDir1 = null, indexDir2 = null, indexDir3 = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
   // System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.directoryFactory", "solr.NRTCachingDirectoryFactory");
    System.setProperty("solr.tests.lockType", DirectoryFactory.LOCK_TYPE_SIMPLE);
    initCore("solrconfig.xml", "schema15.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
    indexDir1 = createTempDir("_testSplit1").toFile();
    indexDir2 = createTempDir("_testSplit2").toFile();
    indexDir3 = createTempDir("_testSplit3").toFile();
    h.getCoreContainer().getAllowPaths().addAll(Sets.newHashSet(indexDir1.toPath(), indexDir2.toPath(), indexDir3.toPath()));
  }

  @Test
  public void testSplitByPaths() throws Exception {
    doTestSplitByPaths(SolrIndexSplitter.SplitMethod.REWRITE);
  }

  @Test
  public void testSplitByPathsLink() throws Exception {
    doTestSplitByPaths(SolrIndexSplitter.SplitMethod.LINK);
  }

  private void doTestSplitByPaths(SolrIndexSplitter.SplitMethod splitMethod) throws Exception {
    LocalSolrQueryRequest request = null;
    try {
      // add two docs
      String id1 = "dorothy";
      assertU(adoc("id", id1));
      String id2 = "kansas";
      assertU(adoc("id", id2));
      assertU(commit());
      assertJQ(req("q", "*:*"), "/response/numFound==2");

      // find minHash/maxHash hash ranges
      List<DocRouter.Range> ranges = getRanges(id1, id2);

      request = lrf.makeRequest("q", "dummy");
      SolrQueryResponse rsp = new SolrQueryResponse();
      SplitIndexCommand command = new SplitIndexCommand(request, rsp,
          Lists.newArrayList(indexDir1.getAbsolutePath(), indexDir2.getAbsolutePath()), null, ranges, new PlainIdRouter(), null, null, splitMethod);
      doSplit(command);

      Directory directory = h.getCore().getDirectoryFactory().get(indexDir1.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      DirectoryReader reader = DirectoryReader.open(directory);
      assertEquals("id:dorothy should be present in split index1", 1, reader.docFreq(new Term("id", "dorothy")));
      assertEquals("id:kansas should not be present in split index1", 0, reader.docFreq(new Term("id", "kansas")));
      assertEquals("split index1 should have only one document", 1, reader.numDocs());
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = h.getCore().getDirectoryFactory().get(indexDir2.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      reader = DirectoryReader.open(directory);
      assertEquals("id:dorothy should not be present in split index2", 0, reader.docFreq(new Term("id", "dorothy")));
      assertEquals("id:kansas should be present in split index2", 1, reader.docFreq(new Term("id", "kansas")));
      assertEquals("split index2 should have only one document", 1, reader.numDocs());
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
    } finally {
      if (request != null) request.close(); // decrefs the searcher
    }
  }

  private void doSplit(SplitIndexCommand command) throws Exception {
    NamedList<Object> results = new NamedList<>();
    new SolrIndexSplitter(command).split(results);
    command.rsp.addResponse(results);
  }
  
  // SOLR-5144
  public void testSplitDeletes() throws Exception {
    doTestSplitDeletes(SolrIndexSplitter.SplitMethod.REWRITE);
  }

  public void testSplitDeletesLink() throws Exception {
    doTestSplitDeletes(SolrIndexSplitter.SplitMethod.LINK);
  }

  private void doTestSplitDeletes(SolrIndexSplitter.SplitMethod splitMethod) throws Exception {
    LocalSolrQueryRequest request = null;
    try {
      // add two docs
      String id1 = "dorothy";
      assertU(adoc("id", id1));
      String id2 = "kansas";
      assertU(adoc("id", id2));
      assertU(commit());
      assertJQ(req("q", "*:*"), "/response/numFound==2");
      assertU(delI(id2)); // delete id2
      assertU(commit());


      // find minHash/maxHash hash ranges
      List<DocRouter.Range> ranges = getRanges(id1, id2);

      request = lrf.makeRequest("q", "dummy");
      SolrQueryResponse rsp = new SolrQueryResponse();

      SplitIndexCommand command = new SplitIndexCommand(request, rsp,
          Lists.newArrayList(indexDir1.getAbsolutePath(), indexDir2.getAbsolutePath()), null, ranges, new PlainIdRouter(), null, null, splitMethod);
      doSplit(command);

      Directory directory = h.getCore().getDirectoryFactory().get(indexDir1.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      DirectoryReader reader = DirectoryReader.open(directory);
      assertEquals("id:dorothy should be present in split index1", 1, reader.docFreq(new Term("id", "dorothy")));
      assertEquals("id:kansas should not be present in split index1", 0, reader.docFreq(new Term("id", "kansas")));
      assertEquals("split index1 should have only one document", 1, reader.numDocs());
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = h.getCore().getDirectoryFactory().get(indexDir2.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      reader = DirectoryReader.open(directory);
      assertEquals(0, reader.numDocs()); // should be empty
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
    } finally {
      if (request != null) request.close(); // decrefs the searcher
    }
  }

  @Test
  public void testSplitByCores() throws Exception {
    doTestSplitByCores(SolrIndexSplitter.SplitMethod.REWRITE);
  }

  @Test
  public void testSplitByCoresLink() throws Exception {
    doTestSplitByCores(SolrIndexSplitter.SplitMethod.LINK);
  }

  private void doTestSplitByCores(SolrIndexSplitter.SplitMethod splitMethod) throws Exception {
    // add three docs and 1 delete
    String id1 = "dorothy";
    assertU(adoc("id", id1));
    String id2 = "kansas";
    assertU(adoc("id", id2));
    String id3 = "wizard";
    assertU(adoc("id", id3));
    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==3");
    assertU(delI("wizard"));
    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==2");
    List<DocRouter.Range> ranges = getRanges(id1, id2);

    SolrCore core1 = null, core2 = null;
    try {

      core1 = h.getCoreContainer().create("split1",
          ImmutableMap.of("dataDir", indexDir1.getAbsolutePath(), "configSet", "cloud-minimal"));
      core2 = h.getCoreContainer().create("split2",
          ImmutableMap.of("dataDir", indexDir2.getAbsolutePath(), "configSet", "cloud-minimal"));

      LocalSolrQueryRequest request = null;
      try {
        request = lrf.makeRequest("q", "dummy");
        SolrQueryResponse rsp = new SolrQueryResponse();
        SplitIndexCommand command = new SplitIndexCommand(request, rsp, null, Lists.newArrayList(core1, core2), ranges,
            new PlainIdRouter(), null, null, splitMethod);
        doSplit(command);
      } finally {
        if (request != null) request.close();
      }
      @SuppressWarnings("resource")
      final EmbeddedSolrServer server1 = new EmbeddedSolrServer(h.getCoreContainer(), "split1");
      @SuppressWarnings("resource")
      final EmbeddedSolrServer server2 = new EmbeddedSolrServer(h.getCoreContainer(), "split2");
      server1.commit(true, true);
      server2.commit(true, true);
      assertEquals("id:dorothy should be present in split index1", 1, server1.query(new SolrQuery("id:dorothy")).getResults().getNumFound());
      assertEquals("id:kansas should not be present in split index1", 0, server1.query(new SolrQuery("id:kansas")).getResults().getNumFound());
      assertEquals("id:dorothy should not be present in split index2", 0, server2.query(new SolrQuery("id:dorothy")).getResults().getNumFound());
      assertEquals("id:kansas should be present in split index2", 1, server2.query(new SolrQuery("id:kansas")).getResults().getNumFound());
    } finally {
      h.getCoreContainer().unload("split2");
      h.getCoreContainer().unload("split1");
    }
  }

  @Test
  public void testSplitAlternately() throws Exception {
    doTestSplitAlternately(SolrIndexSplitter.SplitMethod.REWRITE);
  }

  @Test
  public void testSplitAlternatelyLink() throws Exception {
    doTestSplitAlternately(SolrIndexSplitter.SplitMethod.LINK);
  }

  private void doTestSplitAlternately(SolrIndexSplitter.SplitMethod splitMethod) throws Exception {
    LocalSolrQueryRequest request = null;
    Directory directory = null;
    try {
      // add an even number of docs
      int max = (1 + random().nextInt(10)) * 3;
      log.info("Adding {} number of documents", max);
      for (int i = 0; i < max; i++) {
        assertU(adoc("id", String.valueOf(i)));
      }
      assertU(commit());

      request = lrf.makeRequest("q", "dummy");
      SolrQueryResponse rsp = new SolrQueryResponse();
      SplitIndexCommand command = new SplitIndexCommand(request, rsp,
          Lists.newArrayList(indexDir1.getAbsolutePath(), indexDir2.getAbsolutePath(), indexDir3.getAbsolutePath()),
          null, null, new PlainIdRouter(), null, null, splitMethod);
      doSplit(command);

      directory = h.getCore().getDirectoryFactory().get(indexDir1.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      DirectoryReader reader = DirectoryReader.open(directory);
      int numDocs1 = reader.numDocs();
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = h.getCore().getDirectoryFactory().get(indexDir2.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      reader = DirectoryReader.open(directory);
      int numDocs2 = reader.numDocs();
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = h.getCore().getDirectoryFactory().get(indexDir3.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      reader = DirectoryReader.open(directory);
      int numDocs3 = reader.numDocs();
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = null;
      assertEquals("split indexes lost some documents!", max, numDocs1 + numDocs2 + numDocs3);
      assertEquals("split index1 has wrong number of documents", max / 3, numDocs1);
      assertEquals("split index2 has wrong number of documents", max / 3, numDocs2);
      assertEquals("split index3 has wrong number of documents", max / 3, numDocs3);
    } finally {
      if (request != null) request.close(); // decrefs the searcher
      if (directory != null)  {
        // perhaps an assert failed, release the directory
        h.getCore().getDirectoryFactory().release(directory);
      }
    }
  }

  @Test
  public void testSplitByRouteKey() throws Exception {
    doTestSplitByRouteKey(SolrIndexSplitter.SplitMethod.REWRITE);
  }

  @Test
  public void testSplitByRouteKeyLink() throws Exception  {
    doTestSplitByRouteKey(SolrIndexSplitter.SplitMethod.LINK);
  }

  private void doTestSplitByRouteKey(SolrIndexSplitter.SplitMethod splitMethod) throws Exception  {
    File indexDir = createTempDir().toFile();

    CompositeIdRouter r1 = new CompositeIdRouter();
    String splitKey = "sea-line!";
    String key2 = "soul-raising!";

    // murmur2 has a collision on the above two keys
    assertEquals(r1.keyHashRange(splitKey), r1.keyHashRange(key2));

    /*
    More strings with collisions on murmur2 for future reference:
    "Drava" "dessert spoon"
    "Bighorn" "pleasure lover"
    "attributable to" "second edition"
    "sea-line" "soul-raising"
    "lift direction" "testimony meeting"
     */

    for (int i=0; i<10; i++)  {
      assertU(adoc("id", splitKey + i));
      assertU(adoc("id", key2 + i));
    }
    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==20");

    DocRouter.Range splitKeyRange = r1.keyHashRange(splitKey);

    LocalSolrQueryRequest request = null;
    Directory directory = null;
    try {
      request = lrf.makeRequest("q", "dummy");
      SolrQueryResponse rsp = new SolrQueryResponse();
      SplitIndexCommand command = new SplitIndexCommand(request, rsp,
          Lists.newArrayList(indexDir.getAbsolutePath()), null, Lists.newArrayList(splitKeyRange),
          new CompositeIdRouter(), null, splitKey, splitMethod);
      doSplit(command);
      directory = h.getCore().getDirectoryFactory().get(indexDir.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      DirectoryReader reader = DirectoryReader.open(directory);
      assertEquals("split index has wrong number of documents", 10, reader.numDocs());
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = null;
    } finally {
      if (request != null)  {
        request.close();
      }
      if (directory != null)  {
        h.getCore().getDirectoryFactory().release(directory);
      }
    }
  }

  private List<DocRouter.Range> getRanges(String id1, String id2) throws UnsupportedEncodingException {
    // find minHash/maxHash hash ranges
    byte[] bytes = id1.getBytes(StandardCharsets.UTF_8);
    int minHash = Hash.murmurhash3_x86_32(bytes, 0, bytes.length, 0);
    bytes = id2.getBytes(StandardCharsets.UTF_8);
    int maxHash = Hash.murmurhash3_x86_32(bytes, 0, bytes.length, 0);

    if (minHash > maxHash)  {
      int temp = maxHash;
      maxHash = minHash;
      minHash = temp;
    }

    PlainIdRouter router = new PlainIdRouter();
    DocRouter.Range fullRange = new DocRouter.Range(minHash, maxHash);
    return router.partitionRange(2, fullRange);
  }
}
