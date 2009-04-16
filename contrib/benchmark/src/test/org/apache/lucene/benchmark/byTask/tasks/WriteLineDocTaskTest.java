package org.apache.lucene.benchmark.byTask.tasks;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.lucene.benchmark.BenchmarkTestCase;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.BasicDocMaker;
import org.apache.lucene.benchmark.byTask.feeds.DocData;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

/** Tests the functionality of {@link WriteLineDocTask}. */
public class WriteLineDocTaskTest extends BenchmarkTestCase {

  // class has to be public so that Class.forName.newInstance() will work
  public static final class WriteLineDocMaker extends BasicDocMaker {

    protected DocData getNextDocData() throws NoMoreDataException, Exception {
      throw new UnsupportedOperationException("not implemented");
    }

    public Document makeDocument() throws Exception {
      Document doc = new Document();
      doc.add(new Field(BODY_FIELD, "body", Store.NO, Index.NOT_ANALYZED_NO_NORMS));
      doc.add(new Field(TITLE_FIELD, "title", Store.NO, Index.NOT_ANALYZED_NO_NORMS));
      doc.add(new Field(DATE_FIELD, "date", Store.NO, Index.NOT_ANALYZED_NO_NORMS));
      return doc;
    }
    
    public int numUniqueTexts() {
      return 0;
    }
    
  }
  
  private static final CompressorStreamFactory csFactory = new CompressorStreamFactory();

  private PerfRunData createPerfRunData(File file, boolean setBZCompress, String bz2CompressVal) throws Exception {
    Properties props = new Properties();
    props.setProperty("doc.maker", WriteLineDocMaker.class.getName());
    props.setProperty("line.file.out", file.getAbsolutePath());
    if (setBZCompress) {
      props.setProperty("bzip.compression", bz2CompressVal);
    }
    props.setProperty("directory", "RAMDirectory"); // no accidental FS dir.
    Config config = new Config(props);
    return new PerfRunData(config);
  }
  
  private void doReadTest(File file, boolean bz2File) throws Exception {
    InputStream in = new FileInputStream(file);
    if (bz2File) {
      in = csFactory.createCompressorInputStream("bzip2", in);
    }
    BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
    try {
      String line = br.readLine();
      assertNotNull(line);
      String[] parts = line.split(Character.toString(WriteLineDocTask.SEP));
      assertEquals(3, parts.length);
      assertEquals("title", parts[0]);
      assertEquals("date", parts[1]);
      assertEquals("body", parts[2]);
      assertNull(br.readLine());
    } finally {
      br.close();
    }
  }
  
  /* Tests WriteLineDocTask with a bzip2 format. */
  public void testBZip2() throws Exception {
    
    // Create a document in bz2 format.
    File file = new File(getWorkDir(), "one-line.bz2");
    PerfRunData runData = createPerfRunData(file, true, "true");
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, true);
  }
  
  public void testBZip2AutoDetect() throws Exception {
    
    // Create a document in bz2 format.
    File file = new File(getWorkDir(), "one-line.bz2");
    PerfRunData runData = createPerfRunData(file, false, null);
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, true);
  }
  
  public void testRegularFile() throws Exception {
    
    // Create a document in regular format.
    File file = new File(getWorkDir(), "one-line");
    PerfRunData runData = createPerfRunData(file, true, "false");
    WriteLineDocTask wldt = new WriteLineDocTask(runData);
    wldt.doLogic();
    wldt.close();
    
    doReadTest(file, false);
  }
  
}
