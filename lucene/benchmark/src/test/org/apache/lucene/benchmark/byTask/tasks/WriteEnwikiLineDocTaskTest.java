package org.apache.lucene.benchmark.byTask.tasks;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

/** Tests the functionality of {@link WriteEnwikiLineDocTask}. */
public class WriteEnwikiLineDocTaskTest extends BenchmarkTestCase {

  
  // class has to be public so that Class.forName.newInstance() will work
  /** Interleaves category docs with regular docs */
  public static final class WriteLineCategoryDocMaker extends DocMaker {
  
    AtomicInteger flip = new AtomicInteger(0);
    
    @Override
    public Document makeDocument() throws Exception {
      boolean isCategory = (flip.incrementAndGet() % 2 == 0); 
      Document doc = new Document();
      doc.add(new StringField(BODY_FIELD, "body text", Field.Store.NO));
      doc.add(new StringField(TITLE_FIELD, isCategory ? "Category:title text" : "title text", Field.Store.NO));
      doc.add(new StringField(DATE_FIELD, "date text", Field.Store.NO));
      return doc;
    }
    
  }
  
  private PerfRunData createPerfRunData(File file, String docMakerName) throws Exception {
    Properties props = new Properties();
    props.setProperty("doc.maker", docMakerName);
    props.setProperty("line.file.out", file.getAbsolutePath());
    props.setProperty("directory", "RAMDirectory"); // no accidental FS dir.
    Config config = new Config(props);
    return new PerfRunData(config);
  }
  
  private void doReadTest(File file, String expTitle,
                          String expDate, String expBody) throws Exception {
    doReadTest(2, file, expTitle, expDate, expBody);
    File categoriesFile = WriteEnwikiLineDocTask.categoriesLineFile(file);
    doReadTest(2, categoriesFile, "Category:"+expTitle, expDate, expBody);
  }
  
  private void doReadTest(int n, File file, String expTitle, String expDate, String expBody) throws Exception {
    InputStream in = new FileInputStream(file);
    BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
    try {
      String line = br.readLine();
      WriteLineDocTaskTest.assertHeaderLine(line);
      for (int i=0; i<n; i++) {
        line = br.readLine();
        assertNotNull(line);
        String[] parts = line.split(Character.toString(WriteLineDocTask.SEP));
        int numExpParts = expBody == null ? 2 : 3;
        assertEquals(numExpParts, parts.length);
        assertEquals(expTitle, parts[0]);
        assertEquals(expDate, parts[1]);
        if (expBody != null) {
          assertEquals(expBody, parts[2]);
        }
      }
      assertNull(br.readLine());
    } finally {
      br.close();
    }
  }


  public void testCategoryLines() throws Exception {
    // WriteLineDocTask replaced only \t characters w/ a space, since that's its
    // separator char. However, it didn't replace newline characters, which
    // resulted in errors in LineDocSource.
    File file = new File(getWorkDir(), "two-lines-each.txt");
    PerfRunData runData = createPerfRunData(file, WriteLineCategoryDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteEnwikiLineDocTask(runData);
    for (int i=0; i<4; i++) { // four times so that each file should have 2 lines. 
      wldt.doLogic();
    }
    wldt.close();
    
    doReadTest(file, "title text", "date text", "body text");
  }
  
}
