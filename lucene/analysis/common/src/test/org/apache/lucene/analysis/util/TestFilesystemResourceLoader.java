package org.apache.lucene.analysis.util;

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestFilesystemResourceLoader extends LuceneTestCase {
  
  private void assertNotFound(ResourceLoader rl) throws Exception {
    try {
      IOUtils.closeWhileHandlingException(rl.openResource("/this-directory-really-really-really-should-not-exist/foo/bar.txt"));
      fail("The resource does not exist, should fail!");
    } catch (IOException ioe) {
      // pass
    }
    try {
      rl.newInstance("org.apache.lucene.analysis.FooBarFilterFactory", TokenFilterFactory.class);
      fail("The class does not exist, should fail!");
    } catch (RuntimeException iae) {
      // pass
    }
  }
  
  private void assertClasspathDelegation(ResourceLoader rl) throws Exception {
    // try a stopwords file from classpath
    CharArraySet set = WordlistLoader.getSnowballWordSet(
      new InputStreamReader(rl.openResource("org/apache/lucene/analysis/snowball/english_stop.txt"), IOUtils.CHARSET_UTF_8),
      TEST_VERSION_CURRENT
    );
    assertTrue(set.contains("you"));
    // try to load a class; we use string comparison because classloader may be different...
    assertEquals("org.apache.lucene.analysis.util.RollingCharBuffer",
        rl.newInstance("org.apache.lucene.analysis.util.RollingCharBuffer", Object.class).getClass().getName());
    // theoretically classes should also be loadable:
    IOUtils.closeWhileHandlingException(rl.openResource("java/lang/String.class"));
  }
  
  public void testBaseDir() throws Exception {
    final File base = _TestUtil.getTempDir("fsResourceLoaderBase").getAbsoluteFile();
    try {
      base.mkdirs();
      Writer os = new OutputStreamWriter(new FileOutputStream(new File(base, "template.txt")), IOUtils.CHARSET_UTF_8);
      try {
        os.write("foobar\n");
      } finally {
        IOUtils.closeWhileHandlingException(os);
      }
      
      ResourceLoader rl = new FilesystemResourceLoader(base);
      assertEquals("foobar", WordlistLoader.getLines(rl.openResource("template.txt"), IOUtils.CHARSET_UTF_8).get(0));
      // Same with full path name:
      String fullPath = new File(base, "template.txt").toString();
      assertEquals("foobar",
          WordlistLoader.getLines(rl.openResource(fullPath), IOUtils.CHARSET_UTF_8).get(0));
      assertClasspathDelegation(rl);
      assertNotFound(rl);
      
      // now use RL without base dir:
      rl = new FilesystemResourceLoader();
      assertEquals("foobar",
          WordlistLoader.getLines(rl.openResource(new File(base, "template.txt").toString()), IOUtils.CHARSET_UTF_8).get(0));
      assertClasspathDelegation(rl);
      assertNotFound(rl);
    } finally {
      _TestUtil.rmDir(base);
    }
  }
  
  public void testDelegation() throws Exception {
    ResourceLoader rl = new FilesystemResourceLoader(null, new StringMockResourceLoader("foobar\n"));
    assertEquals("foobar", WordlistLoader.getLines(rl.openResource("template.txt"), IOUtils.CHARSET_UTF_8).get(0));
  }
  
}
