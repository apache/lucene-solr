package org.apache.lucene.benchmark.byTask.feeds;

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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.StringBuilderReader;
import org.apache.lucene.util.ThreadInterruptedException;

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
 * <li><b>html.parser</b> - specifies the {@link HTMLParser} class to use for
 * parsing the TREC documents content (<b>default=DemoHTMLParser</b>).
 * <li><b>content.source.encoding</b> - if not specified, ISO-8859-1 is used.
 * <li><b>content.source.excludeIteration</b> - if true, do not append iteration number to docname
 * </ul>
 */
public class TrecContentSource extends ContentSource {

  private static final class DateFormatInfo {
    DateFormat[] dfs;
    ParsePosition pos;
  }

  private static final String DATE = "Date: ";
  private static final String DOCHDR = "<DOCHDR>";
  private static final String TERMINATING_DOCHDR = "</DOCHDR>";
  private static final String DOCNO = "<DOCNO>";
  private static final String TERMINATING_DOCNO = "</DOCNO>";
  private static final String DOC = "<DOC>";
  private static final String TERMINATING_DOC = "</DOC>";

  private static final String NEW_LINE = System.getProperty("line.separator");

  private static final String DATE_FORMATS [] = {
       "EEE, dd MMM yyyy kk:mm:ss z",	  // Tue, 09 Dec 2003 22:39:08 GMT
       "EEE MMM dd kk:mm:ss yyyy z",  	// Tue Dec 09 16:45:08 2003 EST
       "EEE, dd-MMM-':'y kk:mm:ss z", 	// Tue, 09 Dec 2003 22:39:08 GMT
       "EEE, dd-MMM-yyy kk:mm:ss z", 	  // Tue, 09 Dec 2003 22:39:08 GMT
       "EEE MMM dd kk:mm:ss yyyy",  	  // Tue Dec 09 16:45:08 2003
  };

  private ThreadLocal<DateFormatInfo> dateFormats = new ThreadLocal<DateFormatInfo>();
  private ThreadLocal<StringBuilderReader> trecDocReader = new ThreadLocal<StringBuilderReader>();
  private ThreadLocal<StringBuilder> trecDocBuffer = new ThreadLocal<StringBuilder>();
  private File dataDir = null;
  private ArrayList<File> inputFiles = new ArrayList<File>();
  private int nextFile = 0;
  private int rawDocSize;

  // Use to synchronize threads on reading from the TREC documents.
  private Object lock = new Object();

  // Required for test
  BufferedReader reader;
  int iteration = 0;
  HTMLParser htmlParser;
  private boolean excludeDocnameIteration;
  
  private DateFormatInfo getDateFormatInfo() {
    DateFormatInfo dfi = dateFormats.get();
    if (dfi == null) {
      dfi = new DateFormatInfo();
      dfi.dfs = new SimpleDateFormat[DATE_FORMATS.length];
      for (int i = 0; i < dfi.dfs.length; i++) {
        dfi.dfs[i] = new SimpleDateFormat(DATE_FORMATS[i], Locale.US);
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
  
  private Reader getTrecDocReader(StringBuilder docBuffer) {
    StringBuilderReader r = trecDocReader.get();
    if (r == null) {
      r = new StringBuilderReader(docBuffer);
      trecDocReader.set(r);
    } else {
      r.set(docBuffer);
    }
    return r;
  }

  // read until finding a line that starts with the specified prefix, or a terminating tag has been found.
  private void read(StringBuilder buf, String prefix, boolean collectMatchLine,
                    boolean collectAll, String terminatingTag)
      throws IOException, NoMoreDataException {
    String sep = "";
    while (true) {
      String line = reader.readLine();

      if (line == null) {
        openNextFile();
        continue;
      }

      rawDocSize += line.length();

      if (line.startsWith(prefix)) {
        if (collectMatchLine) {
          buf.append(sep).append(line);
          sep = NEW_LINE;
        }
        break;
      }

      if (terminatingTag != null && line.startsWith(terminatingTag)) {
        // didn't find the prefix that was asked, but the terminating
        // tag was found. set the length to 0 to signal no match was
        // found.
        buf.setLength(0);
        break;
      }

      if (collectAll) {
        buf.append(sep).append(line);
        sep = NEW_LINE;
      }
    }
  }
  
  void openNextFile() throws NoMoreDataException, IOException {
    close();
    int retries = 0;
    while (true) {
      if (nextFile >= inputFiles.size()) { 
        // exhausted files, start a new round, unless forever set to false.
        if (!forever) {
          throw new NoMoreDataException();
        }
        nextFile = 0;
        iteration++;
      }
      File f = inputFiles.get(nextFile++);
      if (verbose) {
        System.out.println("opening: " + f + " length: " + f.length());
      }
      try {
        GZIPInputStream zis = new GZIPInputStream(new FileInputStream(f), BUFFER_SIZE);
        reader = new BufferedReader(new InputStreamReader(zis, encoding), BUFFER_SIZE);
        return;
      } catch (Exception e) {
        retries++;
        if (retries < 20 && verbose) {
          System.out.println("Skipping 'bad' file " + f.getAbsolutePath() + "  #retries=" + retries);
          continue;
        }
        throw new NoMoreDataException();
      }
    }
  }

  Date parseDate(String dateStr) {
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
    String dateStr = null, name = null;
    Reader r = null;
    // protect reading from the TREC files by multiple threads. The rest of the
    // method, i.e., parsing the content and returning the DocData can run
    // unprotected.
    synchronized (lock) {
      if (reader == null) {
        openNextFile();
      }

      StringBuilder docBuf = getDocBuffer();
      
      // 1. skip until doc start
      docBuf.setLength(0);
      read(docBuf, DOC, false, false, null);

      // 2. name
      docBuf.setLength(0);
      read(docBuf, DOCNO, true, false, null);
      name = docBuf.substring(DOCNO.length(), docBuf.indexOf(TERMINATING_DOCNO,
          DOCNO.length()));
      if (!excludeDocnameIteration)
        name = name + "_" + iteration;

      // 3. skip until doc header
      docBuf.setLength(0);
      read(docBuf, DOCHDR, false, false, null);

      boolean findTerminatingDocHdr = false;

      // 4. date - look for the date only until /DOCHDR
      docBuf.setLength(0);
      read(docBuf, DATE, true, false, TERMINATING_DOCHDR);
      if (docBuf.length() != 0) {
        // Date found.
        dateStr = docBuf.substring(DATE.length());
        findTerminatingDocHdr = true;
      }

      // 5. skip until end of doc header
      if (findTerminatingDocHdr) {
        docBuf.setLength(0);
        read(docBuf, TERMINATING_DOCHDR, false, false, null);
      }

      // 6. collect until end of doc
      docBuf.setLength(0);
      read(docBuf, TERMINATING_DOC, false, true, null);
      
      // 7. Set up a Reader over the read content
      r = getTrecDocReader(docBuf);
      // Resetting the thread's reader means it will reuse the instance
      // allocated as well as re-read from docBuf.
      r.reset();
      
      // count char length of parsed html text (larger than the plain doc body text).
      addBytes(docBuf.length()); 
    }

    // This code segment relies on HtmlParser being thread safe. When we get 
    // here, everything else is already private to that thread, so we're safe.
    Date date = dateStr != null ? parseDate(dateStr) : null;
    try {
      docData = htmlParser.parse(docData, name, date, r, null);
      addDoc();
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }

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
    File workDir = new File(config.get("work.dir", "work"));
    String d = config.get("docs.dir", "trec");
    dataDir = new File(d);
    if (!dataDir.isAbsolute()) {
      dataDir = new File(workDir, d);
    }
    collectFiles(dataDir, inputFiles);
    if (inputFiles.size() == 0) {
      throw new IllegalArgumentException("No files in dataDir: " + dataDir);
    }
    try {
      String parserClassName = config.get("html.parser",
          "org.apache.lucene.benchmark.byTask.feeds.DemoHTMLParser");
      htmlParser = Class.forName(parserClassName).asSubclass(HTMLParser.class).newInstance();
    } catch (Exception e) {
      // Should not get here. Throw runtime exception.
      throw new RuntimeException(e);
    }
    if (encoding == null) {
      encoding = "ISO-8859-1";
    }
    excludeDocnameIteration = config.get("content.source.excludeIteration", false);
  }

}
