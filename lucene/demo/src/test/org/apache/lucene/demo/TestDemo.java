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
package org.apache.lucene.demo;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Path;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;

public class TestDemo extends LuceneTestCase {

  private void testOneSearch(Path indexPath, String query, int expectedHitCount) throws Exception {
    PrintStream outSave = System.out;
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      PrintStream fakeSystemOut = new PrintStream(bytes, false, Charset.defaultCharset().name());
      System.setOut(fakeSystemOut);
      SearchFiles.main(new String[] {"-query", query, "-index", indexPath.toString()});
      fakeSystemOut.flush();
      String output = bytes.toString(Charset.defaultCharset().name()); // intentionally use default encoding
      assertTrue("output=" + output, output.contains(expectedHitCount + " total matching documents"));
    } finally {
      System.setOut(outSave);
    }
  }

  public void testIndexSearch() throws Exception {
    Path dir = getDataPath("test-files/docs");
    Path indexDir = createTempDir("ContribDemoTest");
    IndexFiles.main(new String[] { "-create", "-docs", dir.toString(), "-index", indexDir.toString()});
    testOneSearch(indexDir, "apache", 3);
    testOneSearch(indexDir, "patent", 8);
    testOneSearch(indexDir, "lucene", 0);
    testOneSearch(indexDir, "gnu", 6);
    testOneSearch(indexDir, "derivative", 8);
    testOneSearch(indexDir, "license", 13);
  }
}
