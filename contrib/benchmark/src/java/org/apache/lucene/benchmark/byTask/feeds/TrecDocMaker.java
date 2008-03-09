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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

import org.apache.lucene.benchmark.byTask.utils.Config;


/**
 * A DocMaker using the (compressed) Trec collection for its input.
 * <p>
 * Config properties:<ul>
 * <li>work.dir=&lt;path to the root of docs and indexes dirs| Default: work&gt;</li>
 * <li>docs.dir=&lt;path to the docs dir| Default: trec&gt;</li>
 * </ul>
 */
public class TrecDocMaker extends BasicDocMaker {

  private static final String newline = System.getProperty("line.separator");
  
  protected ThreadLocal dateFormat = new ThreadLocal();
  protected File dataDir = null;
  protected ArrayList inputFiles = new ArrayList();
  protected int nextFile = 0;
  protected int iteration=0;
  protected BufferedReader reader;
  private GZIPInputStream zis;
  
  private static final String DATE_FORMATS [] = {
    "EEE, dd MMM yyyy kk:mm:ss z", //Tue, 09 Dec 2003 22:39:08 GMT
    "EEE MMM dd kk:mm:ss yyyy z",  //Tue Dec 09 16:45:08 2003 EST
    "EEE, dd-MMM-':'y kk:mm:ss z", //Tue, 09 Dec 2003 22:39:08 GMT
    "EEE, dd-MMM-yyy kk:mm:ss z", //Tue, 09 Dec 2003 22:39:08 GMT
  };
  
  /* (non-Javadoc)
   * @see SimpleDocMaker#setConfig(java.util.Properties)
   */
  public void setConfig(Config config) {
    super.setConfig(config);
    File workDir = new File(config.get("work.dir","work"));
    String d = config.get("docs.dir","trec");
    dataDir = new File(d);
    if (!dataDir.isAbsolute()) {
      dataDir = new File(workDir, d);
    }
    resetUniqueBytes();
    inputFiles.clear();
    collectFiles(dataDir,inputFiles);
    if (inputFiles.size()==0) {
      throw new RuntimeException("No txt files in dataDir: "+dataDir.getAbsolutePath());
    }
 }

  protected void openNextFile() throws NoMoreDataException, Exception {
    closeInputs();
    int retries = 0;
    while (true) {
      File f = null;
      synchronized (this) {
        if (nextFile >= inputFiles.size()) { 
          // exhausted files, start a new round, unless forever set to false.
          if (!forever) {
            throw new NoMoreDataException();
          }
          nextFile = 0;
          iteration++;
        }
        f = (File) inputFiles.get(nextFile++);
      }
      System.out.println("opening: "+f+" length: "+f.length());
      try {
        zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(f)));
        reader = new BufferedReader(new InputStreamReader(zis));
        return;
      } catch (Exception e) {
        retries++;
        if (retries<20) {
          System.out.println("Skipping 'bad' file "+f.getAbsolutePath()+"  #retries="+retries);
          continue;
        } else {
          throw new NoMoreDataException();
        }
      }
    }
  }

  protected void closeInputs() {
    if (zis!=null) {
      try {
        zis.close();
      } catch (IOException e) {
        System.out.println("closeInputs(): Ingnoring error: "+e);
        e.printStackTrace();
      }
      zis = null;
    }
    if (reader!=null) { 
      try {
        reader.close();
      } catch (IOException e) {
        System.out.println("closeInputs(): Ingnoring error: "+e);
        e.printStackTrace();
      }
      reader = null;
    }
  }
  
  // read until finding a line that starts with the specified prefix
  protected StringBuffer read (String prefix, StringBuffer sb, boolean collectMatchLine, boolean collectAll) throws Exception {
    sb = (sb==null ? new StringBuffer() : sb);
    String sep = "";
    while (true) {
      String line = reader.readLine();
      if (line==null) {
        openNextFile();
        continue;
      }
      if (line.startsWith(prefix)) {
        if (collectMatchLine) {
          sb.append(sep+line);
          sep = newline;
        }
        break;
      }
      if (collectAll) {
        sb.append(sep+line);
        sep = newline;
      }
    }
    //System.out.println("read: "+sb);
    return sb;
  }
  
  protected synchronized DocData getNextDocData() throws NoMoreDataException, Exception {
    if (reader==null) {
      openNextFile();
    }
    // 1. skip until doc start
    read("<DOC>",null,false,false); 
    // 2. name
    StringBuffer sb = read("<DOCNO>",null,true,false);
    String name = sb.substring("<DOCNO>".length());
    name = name.substring(0,name.indexOf("</DOCNO>"))+"_"+iteration;
    // 3. skip until doc header
    read("<DOCHDR>",null,false,false); 
    // 4. date
    sb = read("Date: ",null,true,false);
    String dateStr = sb.substring("Date: ".length());
    // 5. skip until end of doc header
    read("</DOCHDR>",null,false,false); 
    // 6. collect until end of doc
    sb = read("</DOC>",null,false,true);
    // this is the next document, so parse it 
    Date date = parseDate(dateStr);
    HTMLParser p = getHtmlParser();
    DocData docData = p.parse(name, date, sb, getDateFormat(0));
    addBytes(sb.length()); // count char length of parsed html text (larger than the plain doc body text). 
    
    return docData;
  }

  protected DateFormat getDateFormat(int n) {
    DateFormat df[] = (DateFormat[]) dateFormat.get();
    if (df == null) {
      df = new SimpleDateFormat[DATE_FORMATS.length];
      for (int i = 0; i < df.length; i++) {
        df[i] = new SimpleDateFormat(DATE_FORMATS[i],Locale.US);
        df[i].setLenient(true);
      }
      dateFormat.set(df);
    }
    return df[n];
  }

  protected Date parseDate(String dateStr) {
    Date date = null;
    for (int i=0; i<DATE_FORMATS.length; i++) {
      try {
        date = getDateFormat(i).parse(dateStr.trim());
        return date;
      } catch (ParseException e) {
      }
    }
    // do not fail test just because a date could not be parsed
    System.out.println("ignoring date parse exception (assigning 'now') for: "+dateStr);
    date = new Date(); // now 
    return date;
  }


  /*
   *  (non-Javadoc)
   * @see DocMaker#resetIinputs()
   */
  public synchronized void resetInputs() {
    super.resetInputs();
    closeInputs();
    nextFile = 0;
    iteration = 0;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#numUniqueTexts()
   */
  public int numUniqueTexts() {
    return inputFiles.size();
  }

}
