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
package org.apache.lucene.benchmark.byTask.tasks;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.StreamUtils.Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

/** Tests the functionality of {@link WriteLineDocTask}. */
public class WriteLineDocTaskTest extends BenchmarkTestCase {

  // class has to be public so that Class.forName.newInstance() will work
  public static final class WriteLineDocMaker extends DocMaker {
  
    @Override
    public Document makeDocument() throws Exception {
      Document doc = new Document();
      doc.add(new StringField(BODY_FIELD, "body", Field.Store.NO));
      doc.add(new StringField(TITLE_FIELD, "title", Field.Store.NO));
      doc.add(new StringField(DATE_FIELD, "date", Field.Store.NO));
      return doc;
    }
    
  }
  
  // class has to be public so that Class.forName.newInstance() will work
  public static final class NewLinesDocMaker extends DocMaker {
  
    @Override
    public Document makeDocument() throws Exception {
      Document doc = new Document();
      doc.add(new StringField(BODY_FIELD, "body\r\ntext\ttwo", Field.Store.NO));
      doc.add(new StringField(TITLE_FIELD, "title\r\ntext", Field.Store.NO));
      doc.add(new StringField(DATE_FIELD, "date\r\ntext", Field.Store.NO));
      return doc;
    }
    
  }
  
  // class has to be public so that Class.forName.newInstance() will work
  public static final class NoBodyDocMaker extends DocMaker {
    @Override
    public Document makeDocument() throws Exception {
      Document doc = new Document();
      doc.add(new StringField(TITLE_FIELD, "title", Field.Store.NO));
      doc.add(new StringField(DATE_FIELD, "date", Field.Store.NO));
      return doc;
    }
  }
  
  // class has to be public so that Class.forName.newInstance() will work
  public static final class NoTitleDocMaker extends DocMaker {
    @Override
    public Document makeDocument() throws Exception {
      Document doc = new Document();
      doc.add(new StringField(BODY_FIELD, "body", Field.Store.NO));
      doc.add(new StringField(DATE_FIELD, "date", Field.Store.NO));
      return doc;
    }
  }
  
  // class has to be public so that Class.forName.newInstance() will work
  public static final class JustDateDocMaker extends DocMaker {
    @Override
    public Document makeDocument() throws Exception {
      Document doc = new Document();
      doc.add(new StringField(DATE_FIELD, "date", Field.Store.NO));
      return doc;
    }
  }

  // class has to be public so that Class.forName.newInstance() will work
  // same as JustDate just that this one is treated as legal
  public static final class LegalJustDateDocMaker extends DocMaker {
    @Override
    public Document makeDocument() throws Exception {
      Document doc = new Document();
      doc.add(new StringField(DATE_FIELD, "date", Field.Store.NO));
      return doc;
    }
  }

  // class has to be public so that Class.forName.newInstance() will work
  public static final class EmptyDocMaker extends DocMaker {
    @Override
    public Document makeDocument() throws Exception {
      return new Document();
    }
  }
  
  // class has to be public so that Class.forName.newInstance() will work
  public static final class ThreadingDocMaker extends DocMaker {
  
    @Override
    public Document makeDocument() throws Exception {
      Document doc = new Document();
      String name = Thread.currentThread().getName();
      doc.add(new StringField(BODY_FIELD, "body_" + name, Field.Store.NO));
      doc.add(new StringField(TITLE_FIELD, "title_" + name, Field.Store.NO));
      doc.add(new StringField(DATE_FIELD, "date_" + name, Field.Store.NO));
      return doc;
    }
    
  }

  private static final CompressorStreamFactory csFactory = new CompressorStreamFactory();

  private PerfRunData createPerfRunData(Path file, 
                                        boolean allowEmptyDocs,
                                        String docMakerName) throws Exception {
    Properties props = new Properties();
    props.setProperty("doc.maker", docMakerName);
    props.setProperty("line.file.out", file.toAbsolutePath().toString());
    props.setProperty("directory", "RAMDirectory"); // no accidental FS dir.
    if (allowEmptyDocs) {
      props.setProperty("sufficient.fields", ",");
    }
    if (docMakerName.equals(LegalJustDateDocMaker.class.getName())) {
      props.setProperty("line.fields", DocMaker.DATE_FIELD);
      props.setProperty("sufficient.fields", DocMaker.DATE_FIELD);
    }
    Config config = new Config(props);
    return new PerfRunData(config);
  }
  
  private void doReadTest(Path file, Type fileType, String expTitle,
                          String expDate, String expBody) throws Exception {
    InputStream in = Files.newInputStream(file);
    switch(fileType) {
      case BZIP2:
        in = csFactory.createCompressorInputStream(CompressorStreamFactory.BZIP2, in);
        break;
      case GZIP:
        in = csFactory.createCompressorInputStream(CompressorStreamFactory.GZIP, in);
        break;
      case PLAIN:
        break; // nothing to do
      default:
        assertFalse("Unknown file type!",true); //fail, should not happen
    }
    try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line = br.readLine();
      assertHeaderLine(line);
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
      assertNull(br.readLine());
    }
  }

  static void assertHeaderLine(String line) {
    assertTrue("First line should be a header line",line.startsWith(WriteLineDocTask.FIELDS_HEADER_INDICATOR));
  }
  
  /* Tests WriteLineDocTask with a bzip2 format. */
  public void testBZip2() throws Exception {
    
    // Create a document in bz2 format.
    Path file = getWorkDir().resolve("one-line.bz2");
    PerfRunData runData = createPerfRunData(file, false, WriteLineDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, Type.BZIP2, "title", "date", "body");
  }
  
  /* Tests WriteLineDocTask with a gzip format. */
  public void testGZip() throws Exception {
    
    // Create a document in gz format.
    Path file = getWorkDir().resolve("one-line.gz");
    PerfRunData runData = createPerfRunData(file, false, WriteLineDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, Type.GZIP, "title", "date", "body");
  }
  
  public void testRegularFile() throws Exception {
    
    // Create a document in regular format.
    Path file = getWorkDir().resolve("one-line");
    PerfRunData runData = createPerfRunData(file, false, WriteLineDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, Type.PLAIN, "title", "date", "body");
  }

  public void testCharsReplace() throws Exception {
    // WriteLineDocTask replaced only \t characters w/ a space, since that's its
    // separator char. However, it didn't replace newline characters, which
    // resulted in errors in LineDocSource.
    Path file = getWorkDir().resolve("one-line");
    PerfRunData runData = createPerfRunData(file, false, NewLinesDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, Type.PLAIN, "title text", "date text", "body text two");
  }
  
  public void testEmptyBody() throws Exception {
    // WriteLineDocTask threw away documents w/ no BODY element, even if they
    // had a TITLE element (LUCENE-1755). It should throw away documents if they
    // don't have BODY nor TITLE
    Path file = getWorkDir().resolve("one-line");
    PerfRunData runData = createPerfRunData(file, false, NoBodyDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, Type.PLAIN, "title", "date", null);
  }
  
  public void testEmptyTitle() throws Exception {
    Path file = getWorkDir().resolve("one-line");
    PerfRunData runData = createPerfRunData(file, false, NoTitleDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, Type.PLAIN, "", "date", "body");
  }
  
  /** Fail by default when there's only date */
  public void testJustDate() throws Exception {
    Path file = getWorkDir().resolve("one-line");
    PerfRunData runData = createPerfRunData(file, false, JustDateDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line = br.readLine();
      assertHeaderLine(line);
      line = br.readLine();
      assertNull(line);
    }
  }

  public void testLegalJustDate() throws Exception {
    Path file = getWorkDir().resolve("one-line");
    PerfRunData runData = createPerfRunData(file, false, LegalJustDateDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line = br.readLine();
      assertHeaderLine(line);
      line = br.readLine();
      assertNotNull(line);
    }
  }

  public void testEmptyDoc() throws Exception {
    Path file = getWorkDir().resolve("one-line");
    PerfRunData runData = createPerfRunData(file, true, EmptyDocMaker.class.getName());
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line = br.readLine();
      assertHeaderLine(line);
      line = br.readLine();
      assertNotNull(line);
    }
  }

  public void testMultiThreaded() throws Exception {
    Path file = getWorkDir().resolve("one-line");
    PerfRunData runData = createPerfRunData(file, false, ThreadingDocMaker.class.getName());
    final WriteLineDocTask wldt = new WriteLineDocTask(runData);
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread("t" + i) {
        @Override
        public void run() {
          try {
            wldt.doLogic();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
    
    for (Thread t : threads) t.start();
    for (Thread t : threads) t.join();
    
    wldt.close();
    
    Set<String> ids = new HashSet<>();
    try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line = br.readLine();
      assertHeaderLine(line); // header line is written once, no matter how many threads there are
      for (int i = 0; i < threads.length; i++) {
        line = br.readLine();
        String[] parts = line.split(Character.toString(WriteLineDocTask.SEP));
        assertEquals(3, parts.length);
        // check that all thread names written are the same in the same line
        String tname = parts[0].substring(parts[0].indexOf('_'));
        ids.add(tname);
        assertEquals(tname, parts[1].substring(parts[1].indexOf('_')));
        assertEquals(tname, parts[2].substring(parts[2].indexOf('_')));
      }
      // only threads.length lines should exist
      assertNull(br.readLine());
      assertEquals(threads.length, ids.size());
    }
  }
}
