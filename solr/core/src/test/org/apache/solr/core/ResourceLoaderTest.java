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

import junit.framework.Assert;

import org.apache.lucene.util.Constants;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.util._TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class ResourceLoaderTest extends SolrTestCaseJ4 
{
  public void testInstanceDir() throws Exception {
    SolrResourceLoader loader = new SolrResourceLoader(null);
    String instDir = loader.getInstanceDir();
    assertTrue(instDir + " is not equal to " + "solr/", instDir.equals("solr/") == true);
    loader.close();

    loader = new SolrResourceLoader("solr");
    instDir = loader.getInstanceDir();
    assertTrue(instDir + " is not equal to " + "solr/", instDir.equals("solr" + File.separator) == true);
    loader.close();
  }

  public void testEscapeInstanceDir() throws Exception {
    File temp = _TestUtil.getTempDir("testEscapeInstanceDir");
    try {
      temp.mkdirs();
      new File(temp, "dummy.txt").createNewFile();
      File instanceDir = new File(temp, "instance");
      instanceDir.mkdir();
      new File(instanceDir, "conf").mkdir();
      SolrResourceLoader loader = new SolrResourceLoader(instanceDir.getAbsolutePath());
      try {
        loader.openResource("../../dummy.txt").close();
        fail();
      } catch (IOException ioe) {
        assertTrue(ioe.getMessage().startsWith("For security reasons, SolrResourceLoader"));
      }
      loader.close();
    } finally {
      _TestUtil.rmDir(temp);
    }
  }

  public void testAwareCompatibility() throws Exception
  {
    SolrResourceLoader loader = new SolrResourceLoader( "." );
    
    Class<?> clazz = ResourceLoaderAware.class;
    // Check ResourceLoaderAware valid objects
    loader.assertAwareCompatibility( clazz, new NGramFilterFactory(new HashMap<String,String>()) );
    loader.assertAwareCompatibility( clazz, new KeywordTokenizerFactory(new HashMap<String,String>()) );
    
    // Make sure it throws an error for invalid objects
    Object[] invalid = new Object[] {
        // new NGramTokenFilter( null ),
        "hello",  new Float( 12.3f ),
        new LukeRequestHandler(),
        new JSONResponseWriter()
    };
    for( Object obj : invalid ) {
      try {
        loader.assertAwareCompatibility( clazz, obj );
        Assert.fail( "Should be invalid class: "+obj + " FOR " + clazz );
      }
      catch( SolrException ex ) { } // OK
    }
    

    clazz = SolrCoreAware.class;
    // Check ResourceLoaderAware valid objects
    loader.assertAwareCompatibility( clazz, new LukeRequestHandler() );
    loader.assertAwareCompatibility( clazz, new FacetComponent() );
    loader.assertAwareCompatibility( clazz, new JSONResponseWriter() );
    
    // Make sure it throws an error for invalid objects
    invalid = new Object[] {
        new NGramFilterFactory(new HashMap<String,String>()),
        "hello",  new Float( 12.3f ),
        new KeywordTokenizerFactory(new HashMap<String,String>())
    };
    for( Object obj : invalid ) {
      try {
        loader.assertAwareCompatibility( clazz, obj );
        Assert.fail( "Should be invalid class: "+obj + " FOR " + clazz );
      }
      catch( SolrException ex ) { } // OK
    }
    
    loader.close();
  }
  
  public void testBOMMarkers() throws Exception {
    final String fileWithBom = "stopwithbom.txt";
    SolrResourceLoader loader = new SolrResourceLoader("solr/collection1");

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
    SolrResourceLoader loader = new SolrResourceLoader("solr/collection1");
    // ensure we get our exception
    try {
      loader.getLines(wrongEncoding);
      fail();
    } catch (SolrException expected) {
      assertTrue(expected.getCause() instanceof CharacterCodingException);
    }
    loader.close();
  }

  public void testClassLoaderLibs() throws Exception {
    assumeTrue("needs URLClassLoader.close() support", Constants.WINDOWS == false || Constants.JRE_IS_MINIMUM_JAVA7);
    File tmpRoot = _TestUtil.getTempDir("testClassLoaderLibs");

    File lib = new File(tmpRoot, "lib");
    lib.mkdirs();

    JarOutputStream jar1 = new JarOutputStream(new FileOutputStream(new File(lib, "jar1.jar")));
    jar1.putNextEntry(new JarEntry("aLibFile"));
    jar1.closeEntry();
    jar1.close();

    File otherLib = new File(tmpRoot, "otherLib");
    otherLib.mkdirs();

    JarOutputStream jar2 = new JarOutputStream(new FileOutputStream(new File(otherLib, "jar2.jar")));
    jar2.putNextEntry(new JarEntry("explicitFile"));
    jar2.closeEntry();
    jar2.close();
    JarOutputStream jar3 = new JarOutputStream(new FileOutputStream(new File(otherLib, "jar3.jar")));
    jar3.putNextEntry(new JarEntry("otherFile"));
    jar3.closeEntry();
    jar3.close();

    SolrResourceLoader loader = new SolrResourceLoader(tmpRoot.getAbsolutePath());

    // ./lib is accessible by default
    assertNotNull(loader.getClassLoader().getResource("aLibFile"));

    // file filter works (and doesn't add other files in the same dir)
    final File explicitFileJar = new File(otherLib, "jar2.jar").getAbsoluteFile();
    loader.addToClassLoader("otherLib",
        new FileFilter() {
          @Override
          public boolean accept(File pathname) {
            return pathname.equals(explicitFileJar);
          }
        }, false);
    assertNotNull(loader.getClassLoader().getResource("explicitFile"));
    assertNull(loader.getClassLoader().getResource("otherFile"));


    // null file filter means accept all (making otherFile accessible)
    loader.addToClassLoader("otherLib", null, false);
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
  
  public void testLoadDeprecatedFactory() throws Exception {
    SolrResourceLoader loader = new SolrResourceLoader("solr/collection1");
    // ensure we get our exception
    loader.newInstance(DeprecatedTokenFilterFactory.class.getName(), TokenFilterFactory.class, null,
        new Class[] { Map.class }, new Object[] { new HashMap<String,String>() });
    // TODO: How to check that a warning was printed to log file?
    loader.close();    
  }
}
