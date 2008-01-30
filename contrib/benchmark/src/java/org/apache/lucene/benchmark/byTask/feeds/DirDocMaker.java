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

import org.apache.lucene.benchmark.byTask.utils.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Stack;

/**
 * A DocMaker using the Dir collection for its input.
 *
 * Config properties:
 * docs.dir=&lt;path to the docs dir| Default: dir-out&gt;

 *
 */
public class DirDocMaker extends BasicDocMaker {

  protected ThreadLocal dateFormat = new ThreadLocal();
  protected File dataDir = null;
  protected int iteration=0;
  
  static public class Iterator implements java.util.Iterator {

    int count = 0;

    public int getCount(){
      return count;
    }

    Stack stack = new Stack();

    /* this seems silly ... there must be a better way ...
       not that this is good, but can it matter? */

    static class Comparator implements java.util.Comparator {
      public int compare(Object _a, Object _b) {
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

    Comparator c = new Comparator();

    void push(File[] files) {
      Arrays.sort(files, c);
      for(int i = 0; i < files.length; i++) {
        // System.err.println("push " + files[i]);
        stack.push(files[i]);
      }
    }

    void push(File f) {
      push(f.listFiles(new FileFilter() {
          public boolean accept(File f) { return f.isDirectory(); } }));
      push(f.listFiles(new FileFilter() {
          public boolean accept(File f) { return f.getName().endsWith(".txt"); } }));
      find();
    }

    void find() {
      if (stack.empty()) {
        return;
      }
      if (!((File)stack.peek()).isDirectory()) {
        return;
      }
      File f = (File)stack.pop();
      push(f);
    }

    public Iterator(File f) {
      push(f);
    }

    public void remove() {
      throw new RuntimeException("cannot");
    }
    
    public boolean hasNext() {
      return stack.size() > 0;
    }

    public Object next() {
      assert hasNext();
      count++;
      Object object = stack.pop();
      // System.err.println("pop " + object);
      find();
      return object;
    }

  }

  protected Iterator inputFiles = null;

  /* (non-Javadoc)
   * @see SimpleDocMaker#setConfig(java.util.Properties)
   */
  public void setConfig(Config config) {
    super.setConfig(config);
    String d = config.get("docs.dir", "dir-out");
    dataDir = new File(d);
    if (!dataDir.isAbsolute()) {
      dataDir = new File(new File("work"), d);
    }

    inputFiles = new Iterator(dataDir);

    if (inputFiles==null) {
      throw new RuntimeException("No txt files in dataDir: "+dataDir.getAbsolutePath());
    }
  }

  // get/initiate a thread-local simple date format (must do so 
  // because SimpleDateFormat is not thread-safe).
  protected DateFormat getDateFormat () {
    DateFormat df = (DateFormat) dateFormat.get();
    if (df == null) {
      // date format: 30-MAR-1987 14:22:36.87
      df = new SimpleDateFormat("dd-MMM-yyyy kk:mm:ss.SSS",Locale.US);
      df.setLenient(true);
      dateFormat.set(df);
    }
    return df;
  }
  
  protected DocData getNextDocData() throws Exception {
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
      f = (File) inputFiles.next();
      // System.err.println(f);
      name = f.getCanonicalPath()+"_"+iteration;
    }
    
    BufferedReader reader = new BufferedReader(new FileReader(f));
    String line = null;
    //First line is the date, 3rd is the title, rest is body
    String dateStr = reader.readLine();
    reader.readLine();//skip an empty line
    String title = reader.readLine();
    reader.readLine();//skip an empty line
    StringBuffer bodyBuf = new StringBuffer(1024);
    while ((line = reader.readLine()) != null) {
      bodyBuf.append(line).append(' ');
    }
    reader.close();
    addBytes(f.length());
    
    Date date = getDateFormat().parse(dateStr.trim()); 
    return new DocData(name, bodyBuf.toString(), title, null, date);
  }


  /*
   *  (non-Javadoc)
   * @see DocMaker#resetIinputs()
   */
  public synchronized void resetInputs() {
    super.resetInputs();
    inputFiles = new Iterator(dataDir);
    iteration = 0;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#numUniqueTexts()
   */
  public int numUniqueTexts() {
    return inputFiles.getCount();
  }

}
