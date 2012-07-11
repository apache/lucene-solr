package org.apache.lucene.benchmark.byTask.feeds;

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
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.util.IOUtils;

/**
 * A {@link ContentSource} reading from the Reuters collection.
 * <p>
 * Config properties:
 * <ul>
 * <li><b>work.dir</b> - path to the root of docs and indexes dirs (default
 * <b>work</b>).
 * <li><b>docs.dir</b> - path to the docs dir (default <b>reuters-out</b>).
 * </ul>
 */
public class ReutersContentSource extends ContentSource {

  private static final class DateFormatInfo {
    DateFormat df;
    ParsePosition pos;
  }

  private ThreadLocal<DateFormatInfo> dateFormat = new ThreadLocal<DateFormatInfo>();
  private File dataDir = null;
  private ArrayList<File> inputFiles = new ArrayList<File>();
  private int nextFile = 0;
  private int iteration = 0;
  
  @Override
  public void setConfig(Config config) {
    super.setConfig(config);
    File workDir = new File(config.get("work.dir", "work"));
    String d = config.get("docs.dir", "reuters-out");
    dataDir = new File(d);
    if (!dataDir.isAbsolute()) {
      dataDir = new File(workDir, d);
    }
    inputFiles.clear();
    collectFiles(dataDir, inputFiles);
    if (inputFiles.size() == 0) {
      throw new RuntimeException("No txt files in dataDir: "+dataDir.getAbsolutePath());
    }
  }

  private synchronized DateFormatInfo getDateFormatInfo() {
    DateFormatInfo dfi = dateFormat.get();
    if (dfi == null) {
      dfi = new DateFormatInfo();
      // date format: 30-MAR-1987 14:22:36.87
      dfi.df = new SimpleDateFormat("dd-MMM-yyyy kk:mm:ss.SSS",Locale.ROOT);
      dfi.df.setLenient(true);
      dfi.pos = new ParsePosition(0);
      dateFormat.set(dfi);
    }
    return dfi;
  }
  
  private Date parseDate(String dateStr) {
    DateFormatInfo dfi = getDateFormatInfo();
    dfi.pos.setIndex(0);
    dfi.pos.setErrorIndex(-1);
    return dfi.df.parse(dateStr.trim(), dfi.pos);
  }


  @Override
  public void close() throws IOException {
    // TODO implement?
  }
  
  @Override
  public DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
    File f = null;
    String name = null;
    synchronized (this) {
      if (nextFile >= inputFiles.size()) {
        // exhausted files, start a new round, unless forever set to false.
        if (!forever) {
          throw new NoMoreDataException();
        }
        nextFile = 0;
        iteration++;
      }
      f = inputFiles.get(nextFile++);
      name = f.getCanonicalPath() + "_" + iteration;
    }

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), IOUtils.CHARSET_UTF_8));
    try {
      // First line is the date, 3rd is the title, rest is body
      String dateStr = reader.readLine();
      reader.readLine();// skip an empty line
      String title = reader.readLine();
      reader.readLine();// skip an empty line
      StringBuilder bodyBuf = new StringBuilder(1024);
      String line = null;
      while ((line = reader.readLine()) != null) {
        bodyBuf.append(line).append(' ');
      }
      reader.close();
      
      addBytes(f.length());
      
      Date date = parseDate(dateStr.trim());
      
      docData.clear();
      docData.setName(name);
      docData.setBody(bodyBuf.toString());
      docData.setTitle(title);
      docData.setDate(date);
      return docData;
    } finally {
      reader.close();
    }
  }

  @Override
  public synchronized void resetInputs() throws IOException {
    super.resetInputs();
    nextFile = 0;
    iteration = 0;
  }

}
