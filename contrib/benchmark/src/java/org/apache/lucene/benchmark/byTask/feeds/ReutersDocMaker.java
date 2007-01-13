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
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.benchmark.byTask.utils.Config;


/**
 * A DocMaker using the Reuters collection for its input.
 */
public class ReutersDocMaker extends SimpleDocMaker {

  private DateFormat dateFormat;
  private File dataDir = null;
  private ArrayList txtFiles = new ArrayList();
  private int nextFile = 0;
  private int round=0;
  private int count = 0;
  
  /* (non-Javadoc)
   * @see SimpleDocMaker#setConfig(java.util.Properties)
   */
  public void setConfig(Config config) {
    super.setConfig(config);
    String d = config.get("docs.dir","reuters-out");
    dataDir = new File(new File("work"),d);
    addFiles(dataDir);
    if (txtFiles.size()==0) {
      throw new RuntimeException("No txt files in dataDir: "+dataDir.getAbsolutePath());
    }
    // date format: 30-MAR-1987 14:22:36.87
    dateFormat = new SimpleDateFormat("dd-MMM-yyyy kk:mm:ss.SSS");
    dateFormat.setLenient(true);
  }

  private void addFiles(File f) {
    if (!f.canRead()) {
      return;
    }
    if (f.isDirectory()) {
      File files[] = f.listFiles();
      for (int i = 0; i < files.length; i++) {
        addFiles(files[i]);
      }
      return;
    }
    txtFiles.add(f);
    addUniqueBytes(f.length());
  }

  /* (non-Javadoc)
   * @see SimpleDocMaker#makeDocument()
   */
  public Document makeDocument() throws Exception {
    File f = null;
    String name = null;
    synchronized (this) {
      f = (File) txtFiles.get(nextFile++);
      name = f.getCanonicalPath()+"_"+round;
      if (nextFile >= txtFiles.size()) { 
        // exhausted files, start a new round
        nextFile = 0;
        round++;
      }
    }
    
    Document doc = new Document();
    doc.add(new Field("name",name,storeVal,indexVal,termVecVal));
    BufferedReader reader = new BufferedReader(new FileReader(f));
    String line = null;
    //First line is the date, 3rd is the title, rest is body
    String dateStr = reader.readLine();
    reader.readLine();//skip an empty line
    String title = reader.readLine();
    reader.readLine();//skip an empty line
    StringBuffer body = new StringBuffer(1024);
    while ((line = reader.readLine()) != null) {
      body.append(line).append(' ');
    }
    Date date = dateFormat.parse(dateStr.trim());
    doc.add(new Field("date", DateTools.dateToString(date, DateTools.Resolution.SECOND), 
        Field.Store.YES, Field.Index.UN_TOKENIZED));

    if (title != null) {
      doc.add(new Field("title", title, storeVal,indexVal,termVecVal));
    }
    if (body.length() > 0) {
        doc.add(new Field("body", body.toString(), storeVal,indexVal,termVecVal));
    }

    count++;
    addBytes(f.length());

    return doc;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#resetIinputs()
   */
  public synchronized void resetInputs() {
    super.resetInputs();
    nextFile = 0;
    round = 0;
    count = 0;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#numUniqueTexts()
   */
  public int numUniqueTexts() {
    return txtFiles.size();
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#getCount()
   */
  public int getCount() {
    return count;
  }

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.DocMaker#makeDocument(int)
   */
  public Document makeDocument(int size) throws Exception {
    throw new Exception(this+".makeDocument (int size) is not supported!");
  }
}
