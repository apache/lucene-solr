package org.apache.lucene.search.function;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import junit.framework.TestCase;

/**
 * Setup for function tests
 */
public abstract class FunctionTestSetup extends TestCase {

  /**
   * Actual score computation order is slightly different than assumptios
   * this allows for a small amount of variation
   */
  public static float TEST_SCORE_TOLERANCE_DELTA = 0.00005f;
  
  protected static final boolean DBG = false; // change to true for logging to print

  protected static final int N_DOCS = 17; // select a primary number > 2

  protected static final String ID_FIELD = "id";
  protected static final String TEXT_FIELD = "text";
  protected static final String INT_FIELD = "iii";
  protected static final String FLOAT_FIELD = "fff";
  
  private static final String DOC_TEXT_LINES[] = {
    // from a public first aid info at http://firstaid.ie.eu.org 
    "Well it may be a little dramatic but sometimes it true. ",
    "If you call the emergency medical services to an incident, ",
    "your actions have started the chain of survival. ",
    "You have acted to help someone you may not even know. ",
    "First aid is helping, first aid is making that call, ",
    "putting a Band-Aid on a small wound, controlling bleeding in large ",
    "wounds or providing CPR for a collapsed person whose not breathing ",
    "and heart has stopped beating. You can help yourself, your loved ",
    "ones and the stranger whose life may depend on you being in the ",
    "right place at the right time with the right knowledge.",
  };
  
  protected Directory dir;
  protected Analyzer anlzr;
  
  /* @override constructor */
  public FunctionTestSetup(String name) {
    super(name);
  }

  /* @override */
  protected void tearDown() throws Exception {
    super.tearDown();
    dir = null;
    anlzr = null;
  }

  /* @override */
  protected void setUp() throws Exception {
    // prepare a small index with just a few documents.  
    super.setUp();
    dir = new RAMDirectory();
    anlzr = new StandardAnalyzer();
    IndexWriter iw = new IndexWriter(dir,anlzr);
    // add docs not exactly in natural ID order, to verify we do check the order of docs by scores
    int remaining = N_DOCS;
    boolean done[] = new boolean[N_DOCS];
    int i = 0;
    while (remaining>0) {
      if (done[i]) {
        throw new Exception("to set this test correctly N_DOCS="+N_DOCS+" must be primary and greater than 2!");
      }
      addDoc(iw,i);
      done[i] = true;
      i = (i+4)%N_DOCS;
      remaining --;
    }
    iw.close();
  }

  private void addDoc(IndexWriter iw, int i) throws Exception {
    Document d = new Document();
    Fieldable f;
    int scoreAndID = i+1;
    
    f = new Field(ID_FIELD,id2String(scoreAndID),Field.Store.YES,Field.Index.UN_TOKENIZED); // for debug purposes
    f.setOmitNorms(true);
    d.add(f);
    
    f = new Field(TEXT_FIELD,"text of doc"+scoreAndID+textLine(i),Field.Store.NO,Field.Index.TOKENIZED); // for regular search
    f.setOmitNorms(true);
    d.add(f);
    
    f = new Field(INT_FIELD,""+scoreAndID,Field.Store.NO,Field.Index.UN_TOKENIZED); // for function scoring
    f.setOmitNorms(true);
    d.add(f);
    
    f = new Field(FLOAT_FIELD,scoreAndID+".000",Field.Store.NO,Field.Index.UN_TOKENIZED); // for function scoring
    f.setOmitNorms(true);
    d.add(f);

    iw.addDocument(d);
    log("added: "+d);
  }

  // 17 --> ID00017
  protected String id2String(int scoreAndID) {
    String s = "000000000"+scoreAndID;
    int n = (""+N_DOCS).length() + 3;
    int k = s.length() - n; 
    return "ID"+s.substring(k);
  }
  
  // some text line for regular search
  private String textLine(int docNum) {
    return DOC_TEXT_LINES[docNum % DOC_TEXT_LINES.length];
  }

  // extract expected doc score from its ID Field: "ID7" --> 7.0
  protected float expectedFieldScore(String docIDFieldVal) {
    return Float.parseFloat(docIDFieldVal.substring(2)); 
  }
  
  // debug messages (change DBG to true for anything to print) 
  protected void log (Object o) {
    if (DBG) {
      System.out.println(o.toString());
    }
  }
}
