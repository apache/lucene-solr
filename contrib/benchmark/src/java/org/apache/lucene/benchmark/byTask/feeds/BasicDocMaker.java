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
import org.apache.lucene.benchmark.byTask.utils.Format;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;


/**
 * Create documents for the test.
 * Maintains counters of chars etc. so that sub-classes just need to 
 * provide textual content, and the create-by-size is handled here.
 *
 * <p/>
 * Config Params (default is in caps):
 * doc.stored=true|FALSE<br/>
 * doc.tokenized=TRUE|false<br/>
 * doc.term.vector=true|FALSE<br/>
 * doc.store.bytes=true|FALSE //Store the body contents raw UTF-8 bytes as a field<br/>
 */
public abstract class BasicDocMaker implements DocMaker {
  
  private int numDocsCreated = 0;
  private boolean storeBytes = false;

  static class DocData {
    String name;
    Date date;
    String title;
    String body;
    Properties props;
  }
  
  private static class LeftOver {
    private DocData docdata;
    private int cnt;
  }

  // leftovers are thread local, because it is unsafe to share residues between threads
  private ThreadLocal leftovr = new ThreadLocal();

  static final String BODY_FIELD = "body";
  private long numBytes = 0;
  private long numUniqueBytes = 0;

  protected Config config;

  protected Field.Store storeVal = Field.Store.NO;
  protected Field.Index indexVal = Field.Index.TOKENIZED;
  protected Field.TermVector termVecVal = Field.TermVector.NO;
  
  private synchronized int incrNumDocsCreated() {
    return numDocsCreated++;
  }

  /**
   * Return the data of the next document.
   * @return data of the next document.
   * @exception if cannot create the next doc data
   */
  protected abstract DocData getNextDocData() throws Exception;

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.DocMaker#makeDocument()
   */
  public Document makeDocument () throws Exception {
    resetLeftovers();
    DocData docData = getNextDocData();
    Document doc = createDocument(docData,0,-1);
    return doc;
  }

  // create a doc
  // use only part of the body, modify it to keep the rest (or use all if size==0).
  // reset the docdata properties so they are not added more than once.
  private Document createDocument(DocData docData, int size, int cnt) throws UnsupportedEncodingException {
    int docid = incrNumDocsCreated();
    Document doc = new Document();
    doc.add(new Field("docid", "doc"+docid, storeVal, indexVal, termVecVal));
    if (docData.name!=null) {
      String name = (cnt<0 ? docData.name : docData.name+"_"+cnt);
      doc.add(new Field("docname", name, storeVal, indexVal, termVecVal));
    }
    if (docData.date!=null) {
      String dateStr = DateTools.dateToString(docData.date, DateTools.Resolution.SECOND);
      doc.add(new Field("docdate", dateStr, storeVal, indexVal, termVecVal));
    }
    if (docData.title!=null) {
      doc.add(new Field("doctitle", docData.title, storeVal, indexVal, termVecVal));
    }
    if (docData.body!=null && docData.body.length()>0) {
      String bdy;
      if (size<=0 || size>=docData.body.length()) {
        bdy = docData.body; // use all
        docData.body = "";  // nothing left
      } else {
        // attempt not to break words - if whitespace found within next 20 chars...
        for (int n=size-1; n<size+20 && n<docData.body.length(); n++) {
          if (Character.isWhitespace(docData.body.charAt(n))) {
            size = n;
            break;
          }
        }
        bdy = docData.body.substring(0,size); // use part
        docData.body = docData.body.substring(size); // some left
      }
      doc.add(new Field(BODY_FIELD, bdy, storeVal, indexVal, termVecVal));
      if (storeBytes == true) {
        doc.add(new Field("bytes", bdy.getBytes("UTF-8"), Field.Store.YES));
      }
    }

    if (docData.props!=null) {
      for (Iterator it = docData.props.keySet().iterator(); it.hasNext(); ) {
        String key = (String) it.next();
        String val = (String) docData.props.get(key);
        doc.add(new Field(key, val, storeVal, indexVal, termVecVal));
      }
      docData.props = null;
    }
    //System.out.println("============== Created doc "+numDocsCreated+" :\n"+doc+"\n==========");
    return doc;
  }

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.DocMaker#makeDocument(int)
   */
  public Document makeDocument(int size) throws Exception {
    LeftOver lvr = (LeftOver) leftovr.get();
    if (lvr==null || lvr.docdata==null || lvr.docdata.body==null || lvr.docdata.body.length()==0) {
      resetLeftovers();
    }
    DocData dd = (lvr==null ? getNextDocData() : lvr.docdata);
    int cnt = (lvr==null ? 0 : lvr.cnt);
    while (dd.body==null || dd.body.length()<size) {
      DocData dd2 = dd;
      dd = getNextDocData();
      cnt = 0;
      dd.body = dd2.body + dd.body;
    }
    Document doc = createDocument(dd,size,cnt);
    if (dd.body==null || dd.body.length()==0) {
      resetLeftovers();
    } else {
      if (lvr == null) {
        lvr = new LeftOver();
        leftovr.set(lvr);
      }
      lvr.docdata = dd;
      lvr.cnt = ++cnt;
    }
    return doc;
  }

  private void resetLeftovers() {
    leftovr.set(null);
  }

  /* (non-Javadoc)
   * @see DocMaker#setConfig(java.util.Properties)
   */
  public void setConfig(Config config) {
    this.config = config;
    boolean stored = config.get("doc.stored",false); 
    boolean tokenized = config.get("doc.tokenized",true);
    boolean termVec = config.get("doc.term.vector",false);
    storeVal = (stored ? Field.Store.YES : Field.Store.NO);
    indexVal = (tokenized ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED);
    termVecVal = (termVec ? Field.TermVector.YES : Field.TermVector.NO);
    storeBytes = config.get("doc.store.body.bytes", false);
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#resetIinputs()
   */
  public synchronized void resetInputs() {
    printDocStatistics();
    numBytes = 0;
    numDocsCreated = 0;
    resetLeftovers();
  }

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.DocMaker#numUniqueBytes()
   */
  public long numUniqueBytes() {
    return numUniqueBytes;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#getCount()
   */
  public synchronized int getCount() {
    return numDocsCreated;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#getByteCount()
   */
  public synchronized long getByteCount() {
    return numBytes;
  }

  protected void addUniqueBytes (long n) {
    numUniqueBytes += n;
  }
  
  protected synchronized void addBytes (long n) {
    numBytes += n;
  }

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.DocMaker#printDocStatistics()
   */
  private int lastPrintedNumUniqueTexts = 0;
  private long lastPrintedNumUniqueBytes = 0;
  private int printNum = 0;
  public void printDocStatistics() {
    boolean print = false;
    String col = "                  ";
    StringBuffer sb = new StringBuffer();
    String newline = System.getProperty("line.separator");
    sb.append("------------> ").append(Format.simpleName(getClass())).append(" statistics (").append(printNum).append("): ").append(newline);
    int nut = numUniqueTexts();
    if (nut > lastPrintedNumUniqueTexts) {
      print = true;
      sb.append("total count of unique texts: ").append(Format.format(0,nut,col)).append(newline);
      lastPrintedNumUniqueTexts = nut;
    }
    long nub = numUniqueBytes();
    if (nub > lastPrintedNumUniqueBytes) {
      print = true;
      sb.append("total bytes of unique texts: ").append(Format.format(0,nub,col)).append(newline);
      lastPrintedNumUniqueBytes = nub;
    }
    if (getCount()>0) {
      print = true;
      sb.append("num docs added since last inputs reset:   ").append(Format.format(0,getCount(),col)).append(newline);
      sb.append("total bytes added since last inputs reset: ").append(Format.format(0,getByteCount(),col)).append(newline);
    }
    if (print) {
      System.out.println(sb.append(newline).toString());
      printNum++;
    }
  }

  protected void collectFiles(File f, ArrayList inputFiles) {
    if (!f.canRead()) {
      return;
    }
    if (f.isDirectory()) {
      File files[] = f.listFiles();
      for (int i = 0; i < files.length; i++) {
        collectFiles(files[i],inputFiles);
      }
      return;
    }
    inputFiles.add(f);
    addUniqueBytes(f.length());
  }


}
