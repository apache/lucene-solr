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

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.util.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Stack;

/**
 * A {@link ContentSource} using the Dir collection for its input. Supports
 * the following configuration parameters (on top of {@link ContentSource}):
 * <ul>
 * <li><b>work.dir</b> - specifies the working directory. Required if "docs.dir"
 * denotes a relative path (<b>default=work</b>).
 * <li><b>docs.dir</b> - specifies the directory the Dir collection. Can be set
 * to a relative path if "work.dir" is also specified (<b>default=dir-out</b>).
 * </ul>
 */
public class DirContentSource extends ContentSource {

  private static final class DateFormatInfo {
    DateFormat df;
    ParsePosition pos;
  }
  
  /**
   * Iterator over the files in the directory
   */
  public static class Iterator implements java.util.Iterator<File> {

    static class Comparator implements java.util.Comparator<File> {
      @Override
      public int compare(File _a, File _b) {
        String a = _a.toString();
        String b = _b.toString();
        int diff = a.length() - b.length();

        if (diff > 0) {
          while (diff-- > 0) {
            b = "0" + b;
          }
        } else if (diff < 0) {
          diff = -diff;
          while (diff-- > 0) {
            a = "0" + a;
          }
        }

        /* note it's reversed because we're going to push,
           which reverses again */
        return b.compareTo(a);
      }
    }

    int count = 0;

    Stack<File> stack = new Stack<File>();

    /* this seems silly ... there must be a better way ...
       not that this is good, but can it matter? */

    Comparator c = new Comparator();

    public Iterator(File f) {
      push(f);
    }

    void find() {
      if (stack.empty()) {
        return;
      }
      if (!(stack.peek()).isDirectory()) {
        return;
      }
      File f = stack.pop();
      push(f);
    }

    void push(File f) {
      push(f.listFiles(new FileFilter() {

        @Override
        public boolean accept(File file) {
          return file.isDirectory();
        }
      }));
      push(f.listFiles(new FileFilter() {

        @Override
        public boolean accept(File file) {
          return file.getName().endsWith(".txt");
        }
      }));
      find();
    }

    void push(File[] files) {
      Arrays.sort(files, c);
      for(int i = 0; i < files.length; i++) {
        // System.err.println("push " + files[i]);
        stack.push(files[i]);
      }
    }

    public int getCount(){
      return count;
    }

    @Override
    public boolean hasNext() {
      return stack.size() > 0;
    }
    
    @Override
    public File next() {
      assert hasNext();
      count++;
      File object = stack.pop();
      // System.err.println("pop " + object);
      find();
      return object;
    }

    @Override
    public void remove() {
      throw new RuntimeException("cannot");
    }

  }
  
  private ThreadLocal<DateFormatInfo> dateFormat = new ThreadLocal<DateFormatInfo>();
  private File dataDir = null;
  private int iteration = 0;
  private Iterator inputFiles = null;

  // get/initiate a thread-local simple date format (must do so 
  // because SimpleDateFormat is not thread-safe).
  private DateFormatInfo getDateFormatInfo() {
    DateFormatInfo dfi = dateFormat.get();
    if (dfi == null) {
      dfi = new DateFormatInfo();
      dfi.pos = new ParsePosition(0);
      // date format: 30-MAR-1987 14:22:36.87
      dfi.df = new SimpleDateFormat("dd-MMM-yyyy kk:mm:ss.SSS", Locale.ROOT);
      dfi.df.setLenient(true);
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
    inputFiles = null;
  }
  
  @Override
  public DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
    File f = null;
    String name = null;
    synchronized (this) {
      if (!inputFiles.hasNext()) { 
        // exhausted files, start a new round, unless forever set to false.
        if (!forever) {
          throw new NoMoreDataException();
        }
        inputFiles = new Iterator(dataDir);
        iteration++;
      }
      f = inputFiles.next();
      // System.err.println(f);
      name = f.getCanonicalPath()+"_"+iteration;
    }
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), IOUtils.CHARSET_UTF_8));
    String line = null;
    //First line is the date, 3rd is the title, rest is body
    String dateStr = reader.readLine();
    reader.readLine();//skip an empty line
    String title = reader.readLine();
    reader.readLine();//skip an empty line
    StringBuilder bodyBuf = new StringBuilder(1024);
    while ((line = reader.readLine()) != null) {
      bodyBuf.append(line).append(' ');
    }
    reader.close();
    addBytes(f.length());
    
    Date date = parseDate(dateStr);
    
    docData.clear();
    docData.setName(name);
    docData.setBody(bodyBuf.toString());
    docData.setTitle(title);
    docData.setDate(date);
    return docData;
  }
  
  @Override
  public synchronized void resetInputs() throws IOException {
    super.resetInputs();
    inputFiles = new Iterator(dataDir);
    iteration = 0;
  }

  @Override
  public void setConfig(Config config) {
    super.setConfig(config);
    
    File workDir = new File(config.get("work.dir", "work"));
    String d = config.get("docs.dir", "dir-out");
    dataDir = new File(d);
    if (!dataDir.isAbsolute()) {
      dataDir = new File(workDir, d);
    }

    inputFiles = new Iterator(dataDir);

    if (inputFiles == null) {
      throw new RuntimeException("No txt files in dataDir: " + dataDir.getAbsolutePath());
    }
  }

}
