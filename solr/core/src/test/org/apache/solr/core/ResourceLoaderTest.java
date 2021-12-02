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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.CharacterCodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.junit.After;

import static org.apache.solr.core.SolrResourceLoader.*;
import static org.hamcrest.core.Is.is;

public class ResourceLoaderTest extends SolrTestCaseJ4 {
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    setUnsafeResourceLoading(false);
  }

  public void testInstanceDir() throws Exception {
    final Path dir = createTempDir();
    try (SolrResourceLoader loader = new SolrResourceLoader(dir.toAbsolutePath())) {
      assertThat(loader.getInstancePath(), is(dir.toAbsolutePath()));
    }
  }

  public void testEscapeInstanceDir() throws Exception {

    Path temp = createTempDir("testEscapeInstanceDir");
    Files.write(temp.resolve("dummy.txt"), new byte[]{});
    Path instanceDir = temp.resolve("instance");
    Files.createDirectories(instanceDir.resolve("conf"));

    setUnsafeResourceLoading(false);
    try (SolrResourceLoader loader = new SolrResourceLoader(instanceDir)) {
      // Path traversal
      assertTrue(assertThrows(IOException.class, () ->
          loader.openResource("../../dummy.txt").close()).getMessage().contains("Can't find resource"));
      assertNull(loader.resourceLocation("../../dummy.txt"));

      // UNC paths
      assertTrue(assertThrows(SolrResourceNotFoundException.class, () ->
          loader.openResource("\\\\192.168.10.10\\foo").close()).getMessage().contains("Resource '\\\\192.168.10.10\\foo' could not be loaded."));
      assertNull(loader.resourceLocation("\\\\192.168.10.10\\foo"));
    }

    setUnsafeResourceLoading(true);
    try (SolrResourceLoader loader = new SolrResourceLoader(instanceDir)) {
      // Path traversal - unsafe but allowed
      loader.openResource("../../dummy.txt").close();
      assertNotNull(loader.resourceLocation("../../dummy.txt"));

      // UNC paths never allowed
      assertTrue(assertThrows(SolrResourceNotFoundException.class, () ->
          loader.openResource("\\\\192.168.10.10\\foo").close()).getMessage().contains("Resource '\\\\192.168.10.10\\foo' could not be loaded."));
      assertNull(loader.resourceLocation("\\\\192.168.10.10\\foo"));
    }
  }

  private void setUnsafeResourceLoading(boolean unsafe) {
    if (unsafe) {
      System.setProperty(SOLR_ALLOW_UNSAFE_RESOURCELOADING_PARAM, "true");
    } else {
      System.clearProperty(SOLR_ALLOW_UNSAFE_RESOURCELOADING_PARAM);
    }
  }

  @SuppressWarnings({"unchecked"})
  public void testAwareCompatibility() throws Exception {
    
    final Class<?> clazz1 = ResourceLoaderAware.class;
    // Check ResourceLoaderAware valid objects
    assertAwareCompatibility(clazz1, new NGramFilterFactory(map("minGramSize", "1", "maxGramSize", "2")));
    assertAwareCompatibility(clazz1, new KeywordTokenizerFactory(new HashMap<>()));
    
    // Make sure it throws an error for invalid objects
    Object[] invalid = new Object[] {
        // new NGramTokenFilter( null ),
        "hello", 12.3f,
        new LukeRequestHandler(),
        new JSONResponseWriter()
    };
    for( Object obj : invalid ) {
      expectThrows(SolrException.class, () -> assertAwareCompatibility(clazz1, obj));
    }
    

    final Class<?> clazz2 = SolrCoreAware.class;
    // Check ResourceLoaderAware valid objects
    assertAwareCompatibility(clazz2, new LukeRequestHandler());
    assertAwareCompatibility(clazz2, new FacetComponent());
    assertAwareCompatibility(clazz2, new JSONResponseWriter());
    
    // Make sure it throws an error for invalid objects
    invalid = new Object[] {
        new NGramFilterFactory(map("minGramSize", "1", "maxGramSize", "2")),
        "hello",   12.3f ,
        new KeywordTokenizerFactory(new HashMap<>())
    };
    for( Object obj : invalid ) {
      expectThrows(SolrException.class, () -> assertAwareCompatibility(clazz2, obj));
    }
  }
  
  public void testBOMMarkers() throws Exception {
    final String fileWithBom = "stopwithbom.txt";
    SolrResourceLoader loader = new SolrResourceLoader(TEST_PATH().resolve("collection1"));

    // preliminary sanity check
    InputStream bomStream = loader.openResource(fileWithBom);
    try {
      final byte[] bomExpected = new byte[] { -17, -69, -65 };
      final byte[] firstBytes = new byte[3];
      
      assertEquals("Should have been able to read 3 bytes from bomStream",
                   3, bomStream.read(firstBytes));

      assertTrue("This test only works if " + fileWithBom + 
                 " contains a BOM -- it appears someone removed it.", 
                 Arrays.equals(bomExpected, firstBytes));
    } finally {
      try { bomStream.close(); } catch (Exception e) { /* IGNORE */ }
    }

    // now make sure getLines skips the BOM...
    List<String> lines = loader.getLines(fileWithBom);
    assertEquals(1, lines.size());
    assertEquals("BOMsAreEvil", lines.get(0));

    loader.close();
  }

  public void testWrongEncoding() throws Exception {
    String wrongEncoding = "stopwordsWrongEncoding.txt";
    try(SolrResourceLoader loader = new SolrResourceLoader(TEST_PATH().resolve("collection1"))) {
      // ensure we get our exception
      SolrException thrown = expectThrows(SolrException.class, () -> loader.getLines(wrongEncoding));
      assertTrue(thrown.getCause() instanceof CharacterCodingException);
    }
  }

  public void testClassLoaderLibs() throws Exception {
    Path tmpRoot = createTempDir("testClassLoaderLibs");

    Path lib = tmpRoot.resolve("lib");
    Files.createDirectories(lib);

    try (JarOutputStream os = new JarOutputStream(Files.newOutputStream(lib.resolve("jar1.jar")))) {
      os.putNextEntry(new JarEntry("aLibFile"));
      os.closeEntry();
    }

    Path otherLib = tmpRoot.resolve("otherLib");
    Files.createDirectories(otherLib);

    try (JarOutputStream os = new JarOutputStream(Files.newOutputStream(otherLib.resolve("jar2.jar")))) {
      os.putNextEntry(new JarEntry("explicitFile"));
      os.closeEntry();
    }
    try (JarOutputStream os = new JarOutputStream(Files.newOutputStream(otherLib.resolve("jar3.jar")))) {
      os.putNextEntry(new JarEntry("otherFile"));
      os.closeEntry();
    }

    SolrResourceLoader loader = new SolrResourceLoader(tmpRoot);
    loader.addToClassLoader(SolrResourceLoader.getURLs(lib));

    // check "lib/aLibFile"
    assertNotNull(loader.getClassLoader().getResource("aLibFile"));

    // add inidividual jars from other paths
    loader.addToClassLoader(Collections.singletonList(otherLib.resolve("jar2.jar").toUri().toURL()));

    assertNotNull(loader.getClassLoader().getResource("explicitFile"));
    assertNull(loader.getClassLoader().getResource("otherFile"));

    // add all jars from another path
    loader.addToClassLoader(SolrResourceLoader.getURLs(otherLib));
    assertNotNull(loader.getClassLoader().getResource("otherFile"));
    loader.close();
  }
  
  @Deprecated
  public static final class DeprecatedTokenFilterFactory extends TokenFilterFactory {

    public DeprecatedTokenFilterFactory(Map<String,String> args) {
      super(args);
    }

    @Override
    public TokenStream create(TokenStream input) {
      return null;
    }
    
  }

  @SuppressWarnings({"rawtypes", "deprecation"})
  public void testLoadDeprecatedFactory() throws Exception {
    SolrResourceLoader loader = new SolrResourceLoader(Paths.get("solr/collection1"));
    // ensure we get our exception
    loader.newInstance(DeprecatedTokenFilterFactory.class.getName(), TokenFilterFactory.class, null,
        new Class[] { Map.class }, new Object[] { new HashMap<String,String>() });
    // TODO: How to check that a warning was printed to log file?
    loader.close();    
  }
}
