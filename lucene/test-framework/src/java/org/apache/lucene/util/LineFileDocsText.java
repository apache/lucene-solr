package org.apache.lucene.util;

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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

/** Just provides the parsed body, title, date, id from the line file docs. */
public class LineFileDocsText implements Closeable {

  private BufferedReader reader;
  private final static int BUFFER_SIZE = 1 << 16;     // 64K
  private final AtomicInteger id = new AtomicInteger();
  private final String path;
  
  public static class DocText {
    public final String body;
    public final String title;
    public final String date;
    public final String docid;

    DocText(String body, String title, String date, String docid) {
      this.body = body;
      this.title = title;
      this.date = date;
      this.docid = docid;
    }
  }

  /** If forever is true, we rewind the file at EOF (repeat
   * the docs over and over) */
  public LineFileDocsText(Random random, String path) throws IOException {
    this.path = path;
    open(random);
  }

  public LineFileDocsText(Random random) throws IOException {
    this(random, LuceneTestCase.TEST_LINE_DOCS_FILE);
  }

  @Override
  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }
  
  private long randomSeekPos(Random random, long size) {
    if (random == null || size <= 3L)
      return 0L;
    return (random.nextLong()&Long.MAX_VALUE) % (size/3);
  }

  private synchronized void open(Random random) throws IOException {
    InputStream is = getClass().getResourceAsStream(path);
    boolean needSkip = true;
    long size = 0L, seekTo = 0L;
    if (is == null) {
      // if its not in classpath, we load it as absolute filesystem path (e.g. Hudson's home dir)
      Path file = Paths.get(path);
      size = Files.size(file);
      if (path.endsWith(".gz")) {
        // if it is a gzip file, we need to use InputStream and slowly skipTo:
        is = Files.newInputStream(file);
      } else {
        // optimized seek using SeekableByteChannel
        seekTo = randomSeekPos(random, size);
        final SeekableByteChannel channel = Files.newByteChannel(file);
        if (LuceneTestCase.VERBOSE) {
          System.out.println("TEST: LineFileDocs: file seek to fp=" + seekTo + " on open");
        }
        channel.position(seekTo);
        is = Channels.newInputStream(channel);
        needSkip = false;
      }
    } else {
      // if the file comes from Classpath:
      size = is.available();
    }
    
    if (path.endsWith(".gz")) {
      is = new GZIPInputStream(is);
      // guestimate:
      size *= 2.8;
    }
    
    // If we only have an InputStream, we need to seek now,
    // but this seek is a scan, so very inefficient!!!
    if (needSkip) {
      seekTo = randomSeekPos(random, size);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("TEST: LineFileDocs: stream skip to fp=" + seekTo + " on open");
      }
      is.skip(seekTo);
    }
    
    // if we seeked somewhere, read until newline char
    if (seekTo > 0L) {
      int b;
      do {
        b = is.read();
      } while (b >= 0 && b != 13 && b != 10);
    }
    
    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
    reader = new BufferedReader(new InputStreamReader(is, decoder), BUFFER_SIZE);
    
    if (seekTo > 0L) {
      // read one more line, to make sure we are not inside a Windows linebreak (\r\n):
      reader.readLine();
    }
  }

  public synchronized void reset(Random random) throws IOException {
    close();
    open(random);
    id.set(0);
  }

  private final static char SEP = '\t';

  /** Note: Document instance is re-used per-thread */
  public DocText nextDoc() throws IOException {
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

    int spot = line.indexOf(SEP);
    if (spot == -1) {
      throw new RuntimeException("line: [" + line + "] is in an invalid format !");
    }
    int spot2 = line.indexOf(SEP, 1 + spot);
    if (spot2 == -1) {
      throw new RuntimeException("line: [" + line + "] is in an invalid format !");
    }

    return new DocText(line.substring(1+spot2, line.length()),
                       line.substring(0, spot),
                       line.substring(1+spot, spot2),
                       Integer.toString(id.getAndIncrement()));
  }
}
