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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.Format;


/**
 * Create documents for the test
 */
public class SimpleDocMaker implements DocMaker {
  
  static final String BODY_FIELD = "body";
  private int docID = 0;
  private long numBytes = 0;
  private long numUniqueBytes = 0;

  protected Config config;
  private int nextDocTextPosition = 0; // for creating docs of fixed size.

  protected Field.Store storeVal = Field.Store.NO;
  protected Field.Index indexVal = Field.Index.TOKENIZED;
  protected Field.TermVector termVecVal = Field.TermVector.NO;
  
  static final String DOC_TEXT = // from a public first aid info at http://firstaid.ie.eu.org 
    "Well it may be a little dramatic but sometimes it true. " +
    "If you call the emergency medical services to an incident, " +
    "your actions have started the chain of survival. " +
    "You have acted to help someone you may not even know. " +
    "First aid is helping, first aid is making that call, " +
    "putting a Band-Aid on a small wound, controlling bleeding in large " +
    "wounds or providing CPR for a collapsed person whose not breathing " +
    "and heart has stopped beating. You can help yourself, your loved " +
    "ones and the stranger whose life may depend on you being in the " +
    "right place at the right time with the right knowledge.";
  
  private static int DOC_TEXT_LENGTH = DOC_TEXT.length(); 

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.DocMaker#makeDocument()
   */
  public Document makeDocument () throws Exception {
    return makeDocument(0);
  }

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.DocMaker#makeDocument(int)
   */
  public Document makeDocument(int size) throws Exception {
    int docid = newdocid();
    Document doc = new Document();
    doc.add(new Field("docid", "doc"+docid, storeVal, indexVal, termVecVal));
    String docText = createDocText(size);
    doc.add(new Field(BODY_FIELD, "synthetic body text"+docid+" "+docText, storeVal, indexVal, termVecVal));
    addBytes(docText.length()); // should multiply by 2 here?
    return doc;
  }

  private synchronized int[] nextDocText(int fixedDocSize) {
    int from = nextDocTextPosition;
    int to = nextDocTextPosition;
    int wraps = 0;
    int size = 0;
    
    while (size<fixedDocSize) {
      int added = DOC_TEXT_LENGTH - to;
      if (size+added <= fixedDocSize) {
        to = 0;
        size += added;
        wraps ++;
      } else {
        added = fixedDocSize - size;
        size += added;
        to += added;
      }
    }
    
    nextDocTextPosition = to;
    
    return new int[]{from,to,wraps};
  }
  
  private String createDocText(int fixedDocSize) {
    if (fixedDocSize<=0) { 
      //no fixed doc size requirement
      return DOC_TEXT;
    } 
      
    // create a document wit fixed doc size
    int fromToWraps[] = nextDocText(fixedDocSize);
    int from = fromToWraps[0];
    int to = fromToWraps[1];
    int wraps = fromToWraps[2];
    StringBuffer sb = new StringBuffer();
    while (wraps-- > 0) {
      sb.append(DOC_TEXT.substring(from));
      from = 0;
    }
    sb.append(DOC_TEXT.substring(from,to));
    return sb.toString();
  }

  // return a new docid
  private synchronized int newdocid() {
    return docID++;
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
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#resetIinputs()
   */
  public synchronized void resetInputs() {
    printDocStatistics();
    docID = 0;
    numBytes = 0;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#numUniqueTexts()
   */
  public int numUniqueTexts() {
    return 0; // not applicable
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
  public int getCount() {
    return docID;
  }

  /*
   *  (non-Javadoc)
   * @see DocMaker#getByteCount()
   */
  public long getByteCount() {
    return numBytes;
  }

  protected void addUniqueBytes (long n) {
    numUniqueBytes += n;
  }
  
  protected void addBytes (long n) {
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
      sb.append("total bytes of unique texts: ").append(Format.format(0,nut,col)).append(newline);
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
      sb.append("num files added since last inputs reset:   ").append(Format.format(0,getCount(),col)).append(newline);
      sb.append("total bytes added since last inputs reset: ").append(Format.format(0,getByteCount(),col)).append(newline);
    }
    if (print) {
      System.out.println(sb.append(newline).toString());
      printNum++;
    }
  }

}
