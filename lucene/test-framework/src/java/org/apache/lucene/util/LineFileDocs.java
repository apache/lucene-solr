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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.Random;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/** Minimal port of contrib/benchmark's LneDocSource +
 * DocMaker, so tests can enum docs from a line file created
 * by contrib/benchmark's WriteLineDoc task */
public class LineFileDocs implements Closeable {

  private BufferedReader reader;
  private final static int BUFFER_SIZE = 1 << 16;     // 64K
  private final AtomicInteger id = new AtomicInteger();
  private final String path;

  /** If forever is true, we rewind the file at EOF (repeat
   * the docs over and over) */
  public LineFileDocs(Random random, String path) throws IOException {
    this.path = path;
    open(random);
  }

  public LineFileDocs(Random random) throws IOException {
    this(random, LuceneTestCase.TEST_LINE_DOCS_FILE);
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
    final Field body;
    final Field id;
    final Field date;

    public DocState() {
      doc = new Document();
      
      title = new Field("title", "", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
      doc.add(title);

      titleTokenized = new Field("titleTokenized", "", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
      doc.add(titleTokenized);

      body = new Field("body", "", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
      doc.add(body);

      id = new Field("docid", "", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
      doc.add(id);

      date = new Field("date", "", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
      doc.add(date);
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
      docState = new DocState();
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

    docState.body.setValue(line.substring(1+spot2, line.length()));
    final String title = line.substring(0, spot);
    docState.title.setValue(title);
    docState.titleTokenized.setValue(title);
    docState.date.setValue(line.substring(1+spot, spot2));
    docState.id.setValue(Integer.toString(id.getAndIncrement()));
    return docState.doc;
  }
}
