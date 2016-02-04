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
package org.apache.lucene.benchmark.byTask.feeds;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import org.apache.lucene.benchmark.byTask.feeds.TrecDocParser.ParsePathType;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.StreamUtils;

/**
 * Implements a {@link ContentSource} over the TREC collection.
 * <p>
 * Supports the following configuration parameters (on top of
 * {@link ContentSource}):
 * <ul>
 * <li><b>work.dir</b> - specifies the working directory. Required if "docs.dir"
 * denotes a relative path (<b>default=work</b>).
 * <li><b>docs.dir</b> - specifies the directory where the TREC files reside.
 * Can be set to a relative path if "work.dir" is also specified
 * (<b>default=trec</b>).
 * <li><b>trec.doc.parser</b> - specifies the {@link TrecDocParser} class to use for
 * parsing the TREC documents content (<b>default=TrecGov2Parser</b>).
 * <li><b>html.parser</b> - specifies the {@link HTMLParser} class to use for
 * parsing the HTML parts of the TREC documents content (<b>default=DemoHTMLParser</b>).
 * <li><b>content.source.encoding</b> - if not specified, ISO-8859-1 is used.
 * <li><b>content.source.excludeIteration</b> - if true, do not append iteration number to docname
 * </ul>
 */
public class TrecContentSource extends ContentSource {

  static final class DateFormatInfo {
    DateFormat[] dfs;
    ParsePosition pos;
  }

  public static final String DOCNO = "<DOCNO>";
  public static final String TERMINATING_DOCNO = "</DOCNO>";
  public static final String DOC = "<DOC>";
  public static final String TERMINATING_DOC = "</DOC>";

  /** separator between lines in the byffer */ 
  public static final String NEW_LINE = System.getProperty("line.separator");

  private static final String DATE_FORMATS [] = {
       "EEE, dd MMM yyyy kk:mm:ss z",   // Tue, 09 Dec 2003 22:39:08 GMT
       "EEE MMM dd kk:mm:ss yyyy z",    // Tue Dec 09 16:45:08 2003 EST
       "EEE, dd-MMM-':'y kk:mm:ss z",   // Tue, 09 Dec 2003 22:39:08 GMT
       "EEE, dd-MMM-yyy kk:mm:ss z",    // Tue, 09 Dec 2003 22:39:08 GMT
       "EEE MMM dd kk:mm:ss yyyy",      // Tue Dec 09 16:45:08 2003
       "dd MMM yyyy",                   // 1 March 1994
       "MMM dd, yyyy",                  // February 3, 1994
       "yyMMdd",                        // 910513
       "hhmm z.z.z. MMM dd, yyyy",       // 0901 u.t.c. April 28, 1994
  };

  private ThreadLocal<DateFormatInfo> dateFormats = new ThreadLocal<>();
  private ThreadLocal<StringBuilder> trecDocBuffer = new ThreadLocal<>();
  private Path dataDir = null;
  private ArrayList<Path> inputFiles = new ArrayList<>();
  private int nextFile = 0;
  // Use to synchronize threads on reading from the TREC documents.
  private Object lock = new Object();

  // Required for test
  BufferedReader reader;
  int iteration = 0;
  HTMLParser htmlParser;
  
  private boolean excludeDocnameIteration;
  private TrecDocParser trecDocParser = new TrecGov2Parser(); // default
  ParsePathType currPathType; // not private for tests
  
  private DateFormatInfo getDateFormatInfo() {
    DateFormatInfo dfi = dateFormats.get();
    if (dfi == null) {
      dfi = new DateFormatInfo();
      dfi.dfs = new SimpleDateFormat[DATE_FORMATS.length];
      for (int i = 0; i < dfi.dfs.length; i++) {
        dfi.dfs[i] = new SimpleDateFormat(DATE_FORMATS[i], Locale.ENGLISH);
        dfi.dfs[i].setLenient(true);
      }
      dfi.pos = new ParsePosition(0);
      dateFormats.set(dfi);
    }
    return dfi;
  }

  private StringBuilder getDocBuffer() {
    StringBuilder sb = trecDocBuffer.get();
    if (sb == null) {
      sb = new StringBuilder();
      trecDocBuffer.set(sb);
    }
    return sb;
  }
  
  HTMLParser getHtmlParser() {
    return htmlParser;
  }
  
  /**
   * Read until a line starting with the specified <code>lineStart</code>.
   * @param buf buffer for collecting the data if so specified/ 
   * @param lineStart line start to look for, must not be null.
   * @param collectMatchLine whether to collect the matching line into <code>buffer</code>.
   * @param collectAll whether to collect all lines into <code>buffer</code>.
   * @throws IOException If there is a low-level I/O error.
   * @throws NoMoreDataException If the source is exhausted.
   */
   private void read(StringBuilder buf, String lineStart, 
       boolean collectMatchLine, boolean collectAll) throws IOException, NoMoreDataException {
    String sep = "";
    while (true) {
      String line = reader.readLine();

      if (line == null) {
        openNextFile();
        continue;
      }

      if (lineStart!=null && line.startsWith(lineStart)) {
        if (collectMatchLine) {
          buf.append(sep).append(line);
          sep = NEW_LINE;
        }
        return;
      }

      if (collectAll) {
        buf.append(sep).append(line);
        sep = NEW_LINE;
      }
    }
  }
  
  void openNextFile() throws NoMoreDataException, IOException {
    close();
    currPathType = null;
    while (true) {
      if (nextFile >= inputFiles.size()) { 
        // exhausted files, start a new round, unless forever set to false.
        if (!forever) {
          throw new NoMoreDataException();
        }
        nextFile = 0;
        iteration++;
      }
      Path f = inputFiles.get(nextFile++);
      if (verbose) {
        System.out.println("opening: " + f + " length: " + Files.size(f));
      }
      try {
        InputStream inputStream = StreamUtils.inputStream(f); // support either gzip, bzip2, or regular text file, by extension  
        reader = new BufferedReader(new InputStreamReader(inputStream, encoding), StreamUtils.BUFFER_SIZE);
        currPathType = TrecDocParser.pathType(f);
        return;
      } catch (Exception e) {
        if (verbose) {
          System.out.println("Skipping 'bad' file " + f.toAbsolutePath()+" due to "+e.getMessage());
          continue;
        }
        throw new NoMoreDataException();
      }
    }
  }

  public Date parseDate(String dateStr) {
    dateStr = dateStr.trim();
    DateFormatInfo dfi = getDateFormatInfo();
    for (int i = 0; i < dfi.dfs.length; i++) {
      DateFormat df = dfi.dfs[i];
      dfi.pos.setIndex(0);
      dfi.pos.setErrorIndex(-1);
      Date d = df.parse(dateStr, dfi.pos);
      if (d != null) {
        // Parse succeeded.
        return d;
      }
    }
    // do not fail test just because a date could not be parsed
    if (verbose) {
      System.out.println("failed to parse date (assigning 'now') for: " + dateStr);
    }
    return null; 
  }
  
  @Override
  public void close() throws IOException {
    if (reader == null) {
      return;
    }

    try {
      reader.close();
    } catch (IOException e) {
      if (verbose) {
        System.out.println("failed to close reader !");
        e.printStackTrace(System.out);
      }
    }
    reader = null;
  }

  @Override
  public DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
    String name = null;
    StringBuilder docBuf = getDocBuffer();
    ParsePathType parsedPathType;
    
    // protect reading from the TREC files by multiple threads. The rest of the
    // method, i.e., parsing the content and returning the DocData can run unprotected.
    synchronized (lock) {
      if (reader == null) {
        openNextFile();
      }
      
      // 1. skip until doc start - required for all TREC formats
      docBuf.setLength(0);
      read(docBuf, DOC, false, false);
      
      // save parsedFile for passing trecDataParser after the sync block, in 
      // case another thread will open another file in between.
      parsedPathType = currPathType;
      
      // 2. name - required for all TREC formats
      docBuf.setLength(0);
      read(docBuf, DOCNO, true, false);
      name = docBuf.substring(DOCNO.length(), docBuf.indexOf(TERMINATING_DOCNO,
          DOCNO.length())).trim();
      
      if (!excludeDocnameIteration) {
        name = name + "_" + iteration;
      }

      // 3. read all until end of doc
      docBuf.setLength(0);
      read(docBuf, TERMINATING_DOC, false, true);
    }
      
    // count char length of text to be parsed (may be larger than the resulted plain doc body text).
    addBytes(docBuf.length()); 

    // This code segment relies on HtmlParser being thread safe. When we get 
    // here, everything else is already private to that thread, so we're safe.
    docData = trecDocParser.parse(docData, name, this, docBuf, parsedPathType);
    addItem();

    return docData;
  }

  @Override
  public void resetInputs() throws IOException {
    synchronized (lock) {
      super.resetInputs();
      close();
      nextFile = 0;
      iteration = 0;
    }
  }

  @Override
  public void setConfig(Config config) {
    super.setConfig(config);
    // dirs
    Path workDir = Paths.get(config.get("work.dir", "work"));
    String d = config.get("docs.dir", "trec");
    dataDir = Paths.get(d);
    if (!dataDir.isAbsolute()) {
      dataDir = workDir.resolve(d);
    }
    // files
    try {
      collectFiles(dataDir, inputFiles);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (inputFiles.size() == 0) {
      throw new IllegalArgumentException("No files in dataDir: " + dataDir);
    }
    // trec doc parser
    try {
      String trecDocParserClassName = config.get("trec.doc.parser", "org.apache.lucene.benchmark.byTask.feeds.TrecGov2Parser");
      trecDocParser = Class.forName(trecDocParserClassName).asSubclass(TrecDocParser.class).newInstance();
    } catch (Exception e) {
      // Should not get here. Throw runtime exception.
      throw new RuntimeException(e);
    }
    // html parser
    try {
      String htmlParserClassName = config.get("html.parser",
          "org.apache.lucene.benchmark.byTask.feeds.DemoHTMLParser");
      htmlParser = Class.forName(htmlParserClassName).asSubclass(HTMLParser.class).newInstance();
    } catch (Exception e) {
      // Should not get here. Throw runtime exception.
      throw new RuntimeException(e);
    }
    // encoding
    if (encoding == null) {
      encoding = StandardCharsets.ISO_8859_1.name();
    }
    // iteration exclusion in doc name 
    excludeDocnameIteration = config.get("content.source.excludeIteration", false);
  }

}
