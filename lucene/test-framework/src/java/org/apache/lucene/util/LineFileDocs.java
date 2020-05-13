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
package org.apache.lucene.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;

/** Minimal port of benchmark's LneDocSource +
 * DocMaker, so tests can enum docs from a line file created
 * by benchmark's WriteLineDoc task */
public class LineFileDocs implements Closeable {

  private BufferedReader reader;
  private final static int BUFFER_SIZE = 1 << 16;     // 64K
  private final AtomicInteger id = new AtomicInteger();
  private final String path;
  private final Random random;

  /** If forever is true, we rewind the file at EOF (repeat
   * the docs over and over) */
  public LineFileDocs(Random random, String path) throws IOException {
    this.path = path;
    this.random = new Random(random.nextLong());
    open();
  }

  public LineFileDocs(Random random) throws IOException {
    this(random, LuceneTestCase.TEST_LINE_DOCS_FILE);
  }

  @Override
  public synchronized void close() throws IOException {
    IOUtils.close(reader, threadDocs);
    reader = null;
  }
  
  private long randomSeekPos(Random random, long size) {
    if (random == null || size <= 3L) {
      return 0L;
    } else {
      return (random.nextLong()&Long.MAX_VALUE) % (size/3);
    }
  }

  private synchronized void open() throws IOException {
    InputStream is = getClass().getResourceAsStream(path);

    // true if the InputStream is not already randomly seek'd after the if/else block below:
    boolean needSkip;
    
    long size = 0L, seekTo = 0L;
    if (is == null) {
      // if it's not in classpath, we load it as absolute filesystem path (e.g. Jenkins' home dir)
      Path file = Paths.get(path);
      size = Files.size(file);
      if (path.endsWith(".gz")) {
        // if it is a gzip file, we need to use InputStream and seek to one of the pre-computed skip points:
        is = Files.newInputStream(file);
        needSkip = true;
      } else {
        // file is not compressed: optimized seek using SeekableByteChannel
        seekTo = randomSeekPos(random, size);
        final SeekableByteChannel channel = Files.newByteChannel(file);
        if (LuceneTestCase.VERBOSE) {
          System.out.println("TEST: LineFileDocs: file seek to fp=" + seekTo + " on open");
        }
        channel.position(seekTo);
        is = Channels.newInputStream(channel);

        // read until newline char, otherwise we may hit "java.nio.charset.MalformedInputException: Input length = 1"
        // exception in readline() below, because we seeked part way through a multi-byte (in UTF-8) encoded
        // unicode character:
        if (seekTo > 0L) {
          int b;
          do {
            b = is.read();
          } while (b >= 0 && b != 13 && b != 10);
        }

        needSkip = false;
      }
    } else {
      // if the file comes from Classpath:
      size = is.available();
      needSkip = true;
    }

    if (needSkip) {

      // LUCENE-9191: use the optimized (pre-computed, using dev-tools/scripts/create_line_file_docs.py)
      // seek file, so we can seek in a gzip'd file

      int index = path.lastIndexOf('.');
      if (index == -1) {
        throw new IllegalArgumentException("could not determine extension for path \"" + path + "\"");
      }

      // e.g. foo.txt --> foo.seek, foo.txt.gz --> foo.txt.seek
      String seekFilePath = path.substring(0, index) + ".seek";
      InputStream seekIS = getClass().getResourceAsStream(seekFilePath);
      if (seekIS == null) {
        seekIS = Files.newInputStream(Paths.get(seekFilePath));
      }

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(seekIS,
                                                                    StandardCharsets.UTF_8))) {
        List<Long> skipPoints = new ArrayList<>();

        // explicitly insert implicit 0 as the first skip point:
        skipPoints.add(0L);
        
        while (true) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          skipPoints.add(Long.parseLong(line.trim()));
        }

        seekTo = skipPoints.get(random.nextInt(skipPoints.size()));

        // dev-tools/scripts/create_line_file_docs.py ensures this is a "safe" skip point, and we
        // can begin gunziping from here:
        is.skip(seekTo);
        is = new GZIPInputStream(is);

        if (LuceneTestCase.VERBOSE) {
          System.out.println("TEST: LineFileDocs: stream skip to fp=" + seekTo + " on open");
        }
      }
    }
    
    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
    reader = new BufferedReader(new InputStreamReader(is, decoder), BUFFER_SIZE);
  }

  public synchronized void reset() throws IOException {
    reader.close();
    reader = null;
    open();
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
    final Field idNum;
    final Field idNumDV;
    final Field date;

    public DocState() {
      doc = new Document();
      
      title = new StringField("title", "", Field.Store.NO);
      doc.add(title);

      FieldType ft = new FieldType(TextField.TYPE_STORED);
      ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
      ft.setStoreTermVectors(true);
      ft.setStoreTermVectorOffsets(true);
      ft.setStoreTermVectorPositions(true);
      
      titleTokenized = new Field("titleTokenized", "", ft);
      doc.add(titleTokenized);

      body = new Field("body", "", ft);
      doc.add(body);

      id = new StringField("docid", "", Field.Store.YES);
      doc.add(id);

      idNum = new IntPoint("docid_int", 0);
      doc.add(idNum);

      date = new StringField("date", "", Field.Store.YES);
      doc.add(date);

      titleDV = new SortedDocValuesField("titleDV", new BytesRef());
      idNumDV = new NumericDocValuesField("docid_intDV", 0);
      doc.add(titleDV);
      doc.add(idNumDV);
    }
  }

  private final CloseableThreadLocal<DocState> threadDocs = new CloseableThreadLocal<>();

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
        reader.close();
        reader = null;
        open();
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

    docState.body.setStringValue(line.substring(1+spot2, line.length()));
    final String title = line.substring(0, spot);
    docState.title.setStringValue(title);
    if (docState.titleDV != null) {
      docState.titleDV.setBytesValue(new BytesRef(title));
    }
    docState.titleTokenized.setStringValue(title);
    docState.date.setStringValue(line.substring(1+spot, spot2));
    final int i = id.getAndIncrement();
    docState.id.setStringValue(Integer.toString(i));
    docState.idNum.setIntValue(i);
    if (docState.idNumDV != null) {
      docState.idNumDV.setLongValue(i);
    }

    if (random.nextInt(5) == 4) {
      // Make some sparse fields
      Document doc = new Document();
      for(IndexableField field : docState.doc) {
        doc.add(field);
      }

      if (random.nextInt(3) == 1) {
        int x = random.nextInt(4);
        doc.add(new IntPoint("docLength" + x, line.length()));
      }

      if (random.nextInt(3) == 1) {
        int x = random.nextInt(4);
        doc.add(new IntPoint("docTitleLength" + x, title.length()));
      }

      if (random.nextInt(3) == 1) {
        int x = random.nextInt(4);
        doc.add(new NumericDocValuesField("docLength" + x, line.length()));
      }

      // TODO: more random sparse fields here too
    }

    return docState.doc;
  }
}
