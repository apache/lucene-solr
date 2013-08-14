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

package org.apache.solr.core;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.admin.ShowFileRequestHandler;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrIndexConfig;
import org.apache.solr.util.RefCounted;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
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
    initCore("solrconfig-test-misc.xml","schema-reversed.xml");
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
  public void testDisableRequetsHandler() throws Exception {
    assertNull(h.getCore().getRequestHandler("disabled"));
    assertNotNull(h.getCore().getRequestHandler("enabled"));
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

  // sometime if the config referes to old things, it must be replaced with new stuff
  @Test
  public void testAutomaticDeprecationSupport() {
    // make sure the "admin/file" handler is registered
    ShowFileRequestHandler handler = (ShowFileRequestHandler) h.getCore().getRequestHandler("/admin/file");
    assertTrue("file handler should have been automatically registered", handler != null);

    //System.out.println( handler.getHiddenFiles() );
    // should not contain: <gettableFiles>solrconfig.xml schema.xml admin-extra.html</gettableFiles>
    assertFalse(handler.getHiddenFiles().contains("schema.xml".toUpperCase(Locale.ROOT)));
    assertTrue(handler.getHiddenFiles().contains("PROTWORDS.TXT"));
  }

  @Test
  public void testTermIndexDivisor() throws Exception {
    IndexReaderFactory irf = h.getCore().getIndexReaderFactory();
    StandardIndexReaderFactory sirf = (StandardIndexReaderFactory) irf;
    assertEquals(12, sirf.termInfosIndexDivisor);
  }

  // If defaults change, add test methods to cover each version
  @Test
  public void testDefaults() throws Exception {

    SolrConfig sc = new SolrConfig(new SolrResourceLoader("solr/collection1"), "solrconfig-defaults.xml", null);
    SolrIndexConfig sic = sc.indexConfig;
    assertEquals("default ramBufferSizeMB", 100.0D, sic.ramBufferSizeMB, 0.0D);
    assertEquals("default LockType", SolrIndexConfig.LOCK_TYPE_NATIVE, sic.lockType);
    assertEquals("default useCompoundFile", false, sic.useCompoundFile);

    IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema("schema.xml", solrConfig);
    IndexWriterConfig iwc = sic.toIndexWriterConfig(indexSchema);

    assertNotNull("null mp", iwc.getMergePolicy());
    assertTrue("mp is not TMP", iwc.getMergePolicy() instanceof TieredMergePolicy);

    assertNotNull("null ms", iwc.getMergeScheduler());
    assertTrue("ms is not CMS", iwc.getMergeScheduler() instanceof ConcurrentMergeScheduler);
  }


  // sanity check that sys propertis are working as expected
  public void testSanityCheckTestSysPropsAreUsed() throws Exception {

    SolrConfig sc = new SolrConfig(new SolrResourceLoader("solr/collection1"), "solrconfig-basic.xml", null);
    SolrIndexConfig sic = sc.indexConfig;

    assertEquals("ramBufferSizeMB sysprop", 
                 Double.parseDouble(System.getProperty("solr.tests.ramBufferSizeMB")), 
                                    sic.ramBufferSizeMB, 0.0D);
    assertEquals("useCompoundFile sysprop", 
                 Boolean.parseBoolean(System.getProperty("useCompoundFile")), sic.useCompoundFile);
  }

}


