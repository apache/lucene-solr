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
import java.io.Reader;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.demo.html.HTMLParser;


/**
 * A DocMaker using the (compressed) Trec collection for its input.
 */
public class TrecDocMaker extends BasicDocMaker {

  private static final String newline = System.getProperty("line.separator");
  
  private DateFormat dateFormat;
  private File dataDir = null;
  private ArrayList inputFiles = new ArrayList();
  private int nextFile = 0;
  private int iteration=0;
  private BufferedReader reader;
  private GZIPInputStream zis;
  
  /* (non-Javadoc)
   * @see SimpleDocMaker#setConfig(java.util.Properties)
   */
  public void setConfig(Config config) {
    super.setConfig(config);
    String d = config.get("docs.dir","trec");
    dataDir = new File(new File("work"),d);
    collectFiles(dataDir,inputFiles);
    if (inputFiles.size()==0) {
      throw new RuntimeException("No txt files in dataDir: "+dataDir.getAbsolutePath());
    }
    // date format: 30-MAR-1987 14:22:36.87
    dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy kk:mm:ss ",Locale.US);  //Tue, 09 Dec 2003 22:39:08 GMT
    dateFormat.setLenient(true);
  }

  private void openNextFile() throws Exception {
    closeInputs();
    int retries = 0;
    while (retries<20) {
      File f = null;
      synchronized (this) {
        f = (File) inputFiles.get(nextFile++);
        if (nextFile >= inputFiles.size()) { 
          // exhausted files, start a new round
          nextFile = 0;
          iteration++;
        }
      }
      System.out.println("opening: "+f+" length: "+f.length());
      try {
        zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(f)));
        break;
      } catch (Exception e) {
        retries++;
        System.out.println("Skipping 'bad' file "+f.getAbsolutePath()+"  #retries="+retries);
        continue;
      }
    }
    reader = new BufferedReader(new InputStreamReader(zis));
  }

  private void closeInputs() {
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
  private StringBuffer read (String prefix, StringBuffer sb, boolean collectMatchLine, boolean collectAll) throws Exception {
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
  
  protected DocData getNextDocData() throws Exception {
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
    HTMLParser p = new HTMLParser(new StringReader(sb.toString()));
    // title
    String title = p.getTitle();
    // properties 
    Properties props = p.getMetaTags(); 
    // body
    Reader r = p.getReader();
    char c[] = new char[1024];
    StringBuffer bodyBuf = new StringBuffer();
    int n;
    while ((n = r.read(c)) >= 0) {
      if (n>0) {
        bodyBuf.append(c,0,n);
      }
    }
    addBytes(bodyBuf.length());
    
    DocData dd = new DocData();
    
    dd.date = dateFormat.parse(dateStr.trim());
    dd.name = name;
    dd.title = title;
    dd.body = bodyBuf.toString();
    dd.props = props;
    return dd;
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
