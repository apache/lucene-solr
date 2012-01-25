/**
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

package org.apache.solr.core;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.admin.ShowFileRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrIndexConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

public class TestConfig extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-termindex.xml","schema-reversed.xml");
  }

  @Test
  public void testLib() throws IOException {
    SolrResourceLoader loader = h.getCore().getResourceLoader();
    InputStream data = null;
    String[] expectedFiles = new String[] { "empty-file-main-lib.txt",
            "empty-file-a1.txt",
            "empty-file-a2.txt",
            "empty-file-b1.txt",
            "empty-file-b2.txt",
            "empty-file-c1.txt" };
    for (String f : expectedFiles) {
      data = loader.openResource(f);
      assertNotNull("Should have found file " + f, data);
      data.close();
    }
    String[] unexpectedFiles = new String[] { "empty-file-c2.txt",
            "empty-file-d2.txt" };
    for (String f : unexpectedFiles) {
      data = null;
      try {
        data = loader.openResource(f);
      } catch (Exception e) { /* :NOOP: (un)expected */ }
      assertNull("should not have been able to find " + f, data);
    }
  }

  @Test
  public void testJavaProperty() {
    // property values defined in build.xml

    String s = solrConfig.get("propTest");
    assertEquals("prefix-proptwo-suffix", s);

    s = solrConfig.get("propTest/@attr1", "default");
    assertEquals("propone-${literal}", s);

    s = solrConfig.get("propTest/@attr2", "default");
    assertEquals("default-from-config", s);

    s = solrConfig.get("propTest[@attr2='default-from-config']", "default");
    assertEquals("prefix-proptwo-suffix", s);

    NodeList nl = (NodeList) solrConfig.evaluate("propTest", XPathConstants.NODESET);
    assertEquals(1, nl.getLength());
    assertEquals("prefix-proptwo-suffix", nl.item(0).getTextContent());

    Node node = solrConfig.getNode("propTest", true);
    assertEquals("prefix-proptwo-suffix", node.getTextContent());
  }

  @Test
  public void testLucene23Upgrades() throws Exception {
    double bufferSize = solrConfig.getDouble("indexDefaults/ramBufferSizeMB");
    assertTrue(bufferSize + " does not equal: " + 32, bufferSize == 32);
    String mergePolicy = solrConfig.get("indexDefaults/mergePolicy/@class");
    assertEquals(TieredMergePolicy.class.getName(), mergePolicy);
    String mergeSched = solrConfig.get("indexDefaults/mergeScheduler/@class");
    assertTrue(mergeSched + " is not equal to " + SolrIndexConfig.DEFAULT_MERGE_SCHEDULER_CLASSNAME, mergeSched.equals(SolrIndexConfig.DEFAULT_MERGE_SCHEDULER_CLASSNAME) == true);
  }

  // sometime if the config referes to old things, it must be replaced with new stuff
  @Test
  public void testAutomaticDeprecationSupport() {
    // make sure the "admin/file" handler is registered
    ShowFileRequestHandler handler = (ShowFileRequestHandler) h.getCore().getRequestHandler("/admin/file");
    assertTrue("file handler should have been automatically registered", handler != null);

    //System.out.println( handler.getHiddenFiles() );
    // should not contain: <gettableFiles>solrconfig.xml scheam.xml admin-extra.html</gettableFiles>
    assertFalse(handler.getHiddenFiles().contains("scheam.xml".toUpperCase(Locale.ENGLISH)));
    assertTrue(handler.getHiddenFiles().contains("PROTWORDS.TXT"));
  }

  @Test
  public void testTermIndexInterval() throws Exception {
    IndexWriter writer = ((DirectUpdateHandler2)h.getCore().getUpdateHandler()).getSolrCoreState().getIndexWriter(h.getCore());
    int interval = writer.getConfig().getTermIndexInterval();
    assertEquals(256, interval);
  }

  @Test
  public void testTermIndexDivisor() throws Exception {
    IndexReaderFactory irf = h.getCore().getIndexReaderFactory();
    StandardIndexReaderFactory sirf = (StandardIndexReaderFactory) irf;
    assertEquals(12, sirf.termInfosIndexDivisor);
    SolrQueryRequest req = req();
    assertEquals(12, req.getSearcher().getIndexReader().getTermInfosIndexDivisor());
    req.close();
  }


}


