package org.apache.lucene.demo;

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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.apache.lucene.util.LuceneTestCase;

public class TestDemo extends LuceneTestCase {
  // LUCENE-589
  public void testUnicodeHtml() throws Exception {
    File dir = getDataFile("test-files/html");
    File indexDir = new File(TEMP_DIR, "demoIndex");
    IndexHTML.main(new String[] { "-create", "-index", indexDir.getPath(), dir.getPath() });
    File queries = getDataFile("test-files/queries.txt");
    PrintStream outSave = System.out;
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      PrintStream fakeSystemOut = new PrintStream(bytes);
      System.setOut(fakeSystemOut);
      SearchFiles.main(new String[] { "-index", indexDir.getPath(), "-queries", queries.getPath()});
      fakeSystemOut.flush();
      String output = bytes.toString(); // intentionally use default encoding
      assertTrue(output.contains("1 total matching documents"));
    } finally {
      System.setOut(outSave);
    }
  }
}
