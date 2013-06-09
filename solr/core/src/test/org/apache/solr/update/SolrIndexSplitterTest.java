package org.apache.solr.update;

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

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.util.Hash;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class SolrIndexSplitterTest extends SolrTestCaseJ4 {
  File indexDir1 = null, indexDir2 = null, indexDir3 = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
    indexDir1 = new File(TEMP_DIR, this.getClass().getName()
        + "_testSplit1");
    indexDir2 = new File(TEMP_DIR, this.getClass().getName()
        + "_testSplit2");
    indexDir3 = new File(TEMP_DIR, this.getClass().getName()
        + "_testSplit3");

    if (indexDir1.exists()) {
      FileUtils.deleteDirectory(indexDir1);
    }
    assertTrue("Failed to mkdirs indexDir1 for split index", indexDir1.mkdirs());

    if (indexDir2.exists()) {
      FileUtils.deleteDirectory(indexDir2);
    }
    assertTrue("Failed to mkdirs indexDir2 for split index", indexDir2.mkdirs());

    if (indexDir3.exists()) {
      FileUtils.deleteDirectory(indexDir3);
    }
    assertTrue("Failed to mkdirs indexDir3 for split index", indexDir3.mkdirs());
  }

  @Test
  public void testSplitByPaths() throws Exception {
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

      SplitIndexCommand command = new SplitIndexCommand(request,
          Lists.newArrayList(indexDir1.getAbsolutePath(), indexDir2.getAbsolutePath()), null, ranges, new PlainIdRouter());
      new SolrIndexSplitter(command).split();

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

  @Test
  public void testSplitByCores() throws Exception {
    // add two docs
    String id1 = "dorothy";
    assertU(adoc("id", id1));
    String id2 = "kansas";
    assertU(adoc("id", id2));
    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==2");
    List<DocRouter.Range> ranges = getRanges(id1, id2);

    SolrCore core1 = null, core2 = null;
    try {
      CoreDescriptor dcore1 = new CoreDescriptor(h.getCoreContainer(), "split1", h.getCore().getCoreDescriptor().getInstanceDir());
      dcore1.setDataDir(indexDir1.getAbsolutePath());
      dcore1.setSchemaName("schema12.xml");
      
      if (h.getCoreContainer().getZkController() != null) {
        h.getCoreContainer().preRegisterInZk(dcore1);
      }
      
      core1 = h.getCoreContainer().create(dcore1);
      h.getCoreContainer().register(core1, false);

      CoreDescriptor dcore2 = new CoreDescriptor(h.getCoreContainer(), "split2", h.getCore().getCoreDescriptor().getInstanceDir());
      dcore2.setDataDir(indexDir2.getAbsolutePath());
      dcore2.setSchemaName("schema12.xml");
      
      if (h.getCoreContainer().getZkController() != null) {
        h.getCoreContainer().preRegisterInZk(dcore2);
      }
      core2 = h.getCoreContainer().create(dcore2);
      h.getCoreContainer().register(core2, false);

      LocalSolrQueryRequest request = null;
      try {
        request = lrf.makeRequest("q", "dummy");

        SplitIndexCommand command = new SplitIndexCommand(request, null, Lists.newArrayList(core1, core2), ranges, new PlainIdRouter());
        new SolrIndexSplitter(command).split();
      } finally {
        if (request != null) request.close();
      }
      EmbeddedSolrServer server1 = new EmbeddedSolrServer(h.getCoreContainer(), "split1");
      EmbeddedSolrServer server2 = new EmbeddedSolrServer(h.getCoreContainer(), "split2");
      server1.commit(true, true);
      server2.commit(true, true);
      assertEquals("id:dorothy should be present in split index1", 1, server1.query(new SolrQuery("id:dorothy")).getResults().getNumFound());
      assertEquals("id:kansas should not be present in split index1", 0, server1.query(new SolrQuery("id:kansas")).getResults().getNumFound());
      assertEquals("id:dorothy should not be present in split index2", 0, server2.query(new SolrQuery("id:dorothy")).getResults().getNumFound());
      assertEquals("id:kansas should be present in split index2", 1, server2.query(new SolrQuery("id:kansas")).getResults().getNumFound());
    } finally {
      h.getCoreContainer().remove("split2");
      h.getCoreContainer().remove("split1");
      if (core2 != null) core2.close();
      if (core1 != null) core1.close();
    }
  }

  @Test
  public void testSplitAlternately() throws Exception {
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

      SplitIndexCommand command = new SplitIndexCommand(request,
          Lists.newArrayList(indexDir1.getAbsolutePath(), indexDir2.getAbsolutePath(), indexDir3.getAbsolutePath()), null, null, new PlainIdRouter());
      new SolrIndexSplitter(command).split();

      directory = h.getCore().getDirectoryFactory().get(indexDir1.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      DirectoryReader reader = DirectoryReader.open(directory);
      assertEquals("split index1 has wrong number of documents", max / 3, reader.numDocs());
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = h.getCore().getDirectoryFactory().get(indexDir2.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      reader = DirectoryReader.open(directory);
      assertEquals("split index2 has wrong number of documents", max / 3, reader.numDocs());
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = h.getCore().getDirectoryFactory().get(indexDir3.getAbsolutePath(),
          DirectoryFactory.DirContext.DEFAULT, h.getCore().getSolrConfig().indexConfig.lockType);
      reader = DirectoryReader.open(directory);
      assertEquals("split index3 has wrong number of documents", max / 3, reader.numDocs());
      reader.close();
      h.getCore().getDirectoryFactory().release(directory);
      directory = null;
    } finally {
      if (request != null) request.close(); // decrefs the searcher
      if (directory != null)  {
        // perhaps an assert failed, release the directory
        h.getCore().getDirectoryFactory().release(directory);
      }
    }
  }

  private List<DocRouter.Range> getRanges(String id1, String id2) throws UnsupportedEncodingException {
    // find minHash/maxHash hash ranges
    byte[] bytes = id1.getBytes("UTF-8");
    int minHash = Hash.murmurhash3_x86_32(bytes, 0, bytes.length, 0);
    bytes = id2.getBytes("UTF-8");
    int maxHash = Hash.murmurhash3_x86_32(bytes, 0, bytes.length, 0);

    PlainIdRouter router = new PlainIdRouter();
    DocRouter.Range fullRange = new DocRouter.Range(minHash, maxHash);
    return router.partitionRange(2, fullRange);
  }
}
