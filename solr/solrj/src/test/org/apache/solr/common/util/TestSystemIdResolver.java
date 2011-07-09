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

package org.apache.solr.common.util;

import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.common.ResourceLoader;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;

import java.io.File;
import org.xml.sax.InputSource;
import org.apache.commons.io.IOUtils;

public class TestSystemIdResolver extends LuceneTestCase {
  
  private void assertEntityResolving(SystemIdResolver resolver, String expectedSystemId, String base, String systemId) throws Exception {
    final InputSource is = resolver.resolveEntity(null, null, base, systemId);
    try {
      assertEquals("Resolved SystemId does not match", expectedSystemId, is.getSystemId());
    } finally {
      IOUtils.closeQuietly(is.getByteStream());
    }
  }
  
  public void testResolving() throws Exception {
    final ResourceLoader loader = new SolrResourceLoader(SolrTestCaseJ4.TEST_HOME(), this.getClass().getClassLoader());
    final SystemIdResolver resolver = new SystemIdResolver(loader);
    final String fileUri = new File(SolrTestCaseJ4.TEST_HOME()+"/crazy-path-to-config.xml").toURI().toASCIIString();
    
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
    
    // do some real resolves to I nputStreams with real existing files
    assertEntityResolving(resolver, "solrres:/schema.xml", "solrres:/solrconfig.xml", "schema.xml");
    assertEntityResolving(resolver, "solrres:/org/apache/solr/common/util/TestSystemIdResolver.class",
      "solrres:/org/apache/solr/common/ResourceLoader.class", "util/TestSystemIdResolver.class");
    assertEntityResolving(resolver, SystemIdResolver.createSystemIdFromResourceName(SolrTestCaseJ4.TEST_HOME()+"/conf/schema.xml"),
      SystemIdResolver.createSystemIdFromResourceName(SolrTestCaseJ4.TEST_HOME()+"/conf/solrconfig.xml"), "schema.xml");
    assertEntityResolving(resolver, SystemIdResolver.createSystemIdFromResourceName(SolrTestCaseJ4.TEST_HOME()+"/crazy-path-to-schema.xml"),
      SystemIdResolver.createSystemIdFromResourceName(SolrTestCaseJ4.TEST_HOME()+"/crazy-path-to-config.xml"), "crazy-path-to-schema.xml");
    
    // test, that resolving works if somebody uses an absolute file:-URI in a href attribute, the resolver should return null (default fallback)
    assertNull(resolver.resolveEntity(null, null, "solrres:/solrconfig.xml", fileUri));
  }

}
