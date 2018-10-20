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
package org.apache.lucene.analysis.util;


import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestFilesystemResourceLoader extends LuceneTestCase {
  
  private void assertNotFound(ResourceLoader rl) throws Exception {
    // the resource does not exist, should fail!
    expectThrows(IOException.class, () -> {
      IOUtils.closeWhileHandlingException(rl.openResource("this-directory-really-really-really-should-not-exist/foo/bar.txt"));
    });

    // the class does not exist, should fail!
    expectThrows(RuntimeException.class, () -> {
      rl.newInstance("org.apache.lucene.analysis.FooBarFilterFactory", TokenFilterFactory.class);
    });
  }
  
  private void assertClasspathDelegation(ResourceLoader rl) throws Exception {
    // try a stopwords file from classpath
    CharArraySet set = WordlistLoader.getSnowballWordSet(
      new InputStreamReader(rl.openResource("org/apache/lucene/analysis/snowball/english_stop.txt"), StandardCharsets.UTF_8)
    );
    assertTrue(set.contains("you"));
    // try to load a class; we use string comparison because classloader may be different...
    assertEquals("org.apache.lucene.analysis.util.RollingCharBuffer",
        rl.newInstance("org.apache.lucene.analysis.util.RollingCharBuffer", Object.class).getClass().getName());
  }
  
  public void testBaseDir() throws Exception {
    final Path base = createTempDir("fsResourceLoaderBase");
    Writer os = Files.newBufferedWriter(base.resolve("template.txt"), StandardCharsets.UTF_8);
    try {
      os.write("foobar\n");
    } finally {
      IOUtils.closeWhileHandlingException(os);
    }
      
    ResourceLoader rl = new FilesystemResourceLoader(base);
    assertEquals("foobar", WordlistLoader.getLines(rl.openResource("template.txt"), StandardCharsets.UTF_8).get(0));
    // Same with full path name:
    String fullPath = base.resolve("template.txt").toAbsolutePath().toString();
    assertEquals("foobar",
                 WordlistLoader.getLines(rl.openResource(fullPath), StandardCharsets.UTF_8).get(0));
    assertClasspathDelegation(rl);
    assertNotFound(rl);
  }
  
  public void testDelegation() throws Exception {
    ResourceLoader rl = new FilesystemResourceLoader(createTempDir("empty"), new StringMockResourceLoader("foobar\n"));
    assertEquals("foobar", WordlistLoader.getLines(rl.openResource("template.txt"), StandardCharsets.UTF_8).get(0));
  }
  
}
