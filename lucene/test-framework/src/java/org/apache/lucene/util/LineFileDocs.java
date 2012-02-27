package org.apache.lucene.util;

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
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import org.apache.lucene.document.DocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocValues;

/** Minimal port of contrib/benchmark's LneDocSource +
 * DocMaker, so tests can enum docs from a line file created
 * by contrib/benchmark's WriteLineDoc task */
public class LineFileDocs implements Closeable {

  private BufferedReader reader;
  private final static int BUFFER_SIZE = 1 << 16;     // 64K
  private final AtomicInteger id = new AtomicInteger();
  private final String path;
  private final boolean useDocValues;

  /** If forever is true, we rewind the file at EOF (repeat
   * the docs over and over) */
  public LineFileDocs(Random random, String path, boolean useDocValues) throws IOException {
    this.path = path;
    this.useDocValues = useDocValues;
    open(random);
  }

  public LineFileDocs(Random random) throws IOException {
    this(random, LuceneTestCase.TEST_LINE_DOCS_FILE, true);
  }

  public LineFileDocs(Random random, boolean useDocValues) throws IOException {
    this(random, LuceneTestCase.TEST_LINE_DOCS_FILE, useDocValues);
  }

  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  private synchronized void open(Random random) throws IOException {
    InputStream is = getClass().getResourceAsStream(path);
    if (is == null) {
      // if its not in classpath, we load it as absolute filesystem path (e.g. Hudson's home dir)
      is = new FileInputStream(path);
    }
    File file = new File(path);
    long size;
    if (file.exists()) {
      size = file.length();
    } else {
      size = is.available();
    }
    if (path.endsWith(".gz")) {
      is = new GZIPInputStream(is);
      // guestimate:
      size *= 2.8;
    }

    reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), BUFFER_SIZE);

    // Override sizes for currently "known" line files:
    if (path.equals("europarl.lines.txt.gz")) {
      size = 15129506L;
    } else if (path.equals("/home/hudson/lucene-data/enwiki.random.lines.txt.gz")) {
      size = 3038178822L;
    }

    // Randomly seek to starting point:
    if (random != null && size > 3) {
      final long seekTo = (random.nextLong()&Long.MAX_VALUE) % (size/3);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("TEST: LineFileDocs: seek to fp=" + seekTo + " on open");
      }
      reader.skip(seekTo);
      reader.readLine();
    }
  }

  public synchronized void reset(Random random) throws IOException {
    close();
    open(random);
    id.set(0);
  }

  private final static char SEP = '\t';

  private static final class DocState {
    final Document doc;
    final Field titleTokenized;
    final Field title;
    final Field titleDV;
    final Field body;
    final Field id;
    final Field date;

    public DocState(boolean useDocValues) {
      doc = new Document();
      
      title = new StringField("title", "");
      doc.add(title);

      FieldType ft = new FieldType(TextField.TYPE_STORED);
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorOffsets(true);
      ft.setStoreTermVectorPositions(true);
      
      titleTokenized = new Field("titleTokenized", "", ft);
      doc.add(titleTokenized);

      body = new Field("body", "", ft);
      doc.add(body);

      id = new Field("docid", "", StringField.TYPE_STORED);
      doc.add(id);

      date = new Field("date", "", StringField.TYPE_STORED);
      doc.add(date);

      if (useDocValues) {
        titleDV = new DocValuesField("titleDV", new BytesRef(), DocValues.Type.BYTES_VAR_SORTED);
        doc.add(titleDV);
      } else {
        titleDV = null;
      }
    }
  }

  private final ThreadLocal<DocState> threadDocs = new ThreadLocal<DocState>();

  /** Note: Document instance is re-used per-thread */
  public Document nextDoc() throws IOException {
    String line;
    synchronized(this) {
      line = reader.readLine();
      if (line == null) {
        // Always rewind at end:
        if (LuceneTestCase.VERBOSE) {
          System.out.println("TEST: LineFileDocs: now rewind file...");
        }
        close();
        open(null);
        line = reader.readLine();
      }
    }

    DocState docState = threadDocs.get();
    if (docState == null) {
      docState = new DocState(useDocValues);
      threadDocs.set(docState);
    }

    int spot = line.indexOf(SEP);
    if (spot == -1) {
      throw new RuntimeException("line: [" + line + "] is in an invalid format !");
    }
    int spot2 = line.indexOf(SEP, 1 + spot);
    if (spot2 == -1) {
      throw new RuntimeException("line: [" + line + "] is in an invalid format !");
    }

    docState.body.setStringValue(line.substring(1+spot2, line.length()));
    final String title = line.substring(0, spot);
    docState.title.setStringValue(title);
    if (docState.titleDV != null) {
      docState.titleDV.setBytesValue(new BytesRef(title));
    }
    docState.titleTokenized.setStringValue(title);
    docState.date.setStringValue(line.substring(1+spot, spot2));
    docState.id.setStringValue(Integer.toString(id.getAndIncrement()));
    return docState.doc;
  }
}
