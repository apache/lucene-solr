package org.apache.solr.util;

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

import java.io.File;
import java.nio.file.Path;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;
import org.xml.sax.InputSource;

public class TestSystemIdResolver extends LuceneTestCase {
  
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.allow.unsafe.resourceloading", "true");
  }

  public void tearDown() throws Exception {
    System.clearProperty("solr.allow.unsafe.resourceloading");
    super.tearDown();
  }

  private void assertEntityResolving(SystemIdResolver resolver, String expectedSystemId, String base, String systemId) throws Exception {
    final InputSource is = resolver.resolveEntity(null, null, base, systemId);
    try {
      assertEquals("Resolved SystemId does not match", expectedSystemId, is.getSystemId());
    } finally {
      IOUtils.closeQuietly(is.getByteStream());
    }
  }
  
  public void testResolving() throws Exception {
    final Path testHome = SolrTestCaseJ4.getFile("solr/collection1").getParentFile().toPath();
    final ResourceLoader loader = new SolrResourceLoader(testHome.resolve("collection1"), this.getClass().getClassLoader());
    final SystemIdResolver resolver = new SystemIdResolver(loader);
    final String fileUri = new File(testHome+"/crazy-path-to-config.xml").toURI().toASCIIString();
    
    assertEquals("solrres:/test.xml", SystemIdResolver.createSystemIdFromResourceName("test.xml"));
    assertEquals("solrres://@/usr/local/etc/test.xml", SystemIdResolver.createSystemIdFromResourceName("/usr/local/etc/test.xml"));
    assertEquals("solrres://@/test.xml", SystemIdResolver.createSystemIdFromResourceName(File.separatorChar+"test.xml"));
    
    // check relative URI resolving
    assertEquals("solrres:/test.xml", resolver.resolveRelativeURI("solrres:/base.xml", "test.xml").toASCIIString());
    assertEquals("solrres://@/etc/test.xml",
      resolver.resolveRelativeURI("solrres://@/usr/local/etc/base.xml", "../../../etc/test.xml").toASCIIString());
    // special case: if relative URI starts with "/" convert to an absolute solrres://@/-URI
    assertEquals("solrres://@/a/test.xml", resolver.resolveRelativeURI("solrres:/base.xml", "/a/test.xml").toASCIIString());
    // test, that resolving works if somebody uses an absolute file:-URI in a href attribute, it should be preserved
    assertEquals(fileUri, resolver.resolveRelativeURI("solrres:/base.xml", fileUri).toASCIIString());
    assertEquals("solrres:/base.xml", resolver.resolveRelativeURI(fileUri, "solrres:/base.xml").toASCIIString());
    
    // do some real resolves to InputStreams with real existing files
    assertEntityResolving(resolver, "solrres:/schema.xml", "solrres:/solrconfig.xml", "schema.xml");
    assertEntityResolving(resolver, "solrres:/org/apache/solr/util/TestSystemIdResolver.class",
      "solrres:/org/apache/solr/util/RTimer.class", "TestSystemIdResolver.class");
    assertEntityResolving(resolver, SystemIdResolver.createSystemIdFromResourceName(testHome+"/collection1/conf/schema.xml"),
      SystemIdResolver.createSystemIdFromResourceName(testHome+"/collection1/conf/solrconfig.xml"), "schema.xml");
    assertEntityResolving(resolver, SystemIdResolver.createSystemIdFromResourceName(testHome+"/crazy-path-to-schema.xml"),
      SystemIdResolver.createSystemIdFromResourceName(testHome+"/crazy-path-to-config.xml"), "crazy-path-to-schema.xml");
    
    // test, that resolving works if somebody uses an absolute file:-URI in a href attribute, the resolver should return null (default fallback)
    assertNull(resolver.resolveEntity(null, null, "solrres:/solrconfig.xml", fileUri));
  }

}
