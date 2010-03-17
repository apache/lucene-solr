package org.apache.lucene.index;

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

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Test that norms info is preserved during index life - including
 * separate norms, addDocument, addIndexesNoOptimize, optimize.
 */
public class TestNorms extends LuceneTestCase {

  private class SimilarityOne extends DefaultSimilarity {
    @Override
    public float lengthNorm(String fieldName, int numTerms) {
      return 1;
    }
  }

  private static final int NUM_FIELDS = 10;
  
  private Similarity similarityOne;
  private Analyzer anlzr;
  private int numDocNorms;
  private ArrayList norms; 
  private ArrayList modifiedNorms; 
  private float lastNorm = 0;
  private float normDelta = (float) 0.001;

  public TestNorms(String s) {
    super(s);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    similarityOne = new SimilarityOne();
    anlzr = new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_CURRENT);
  }

  /**
   * Test that norms values are preserved as the index is maintained.
   * Including separate norms.
   * Including merging indexes with seprate norms. 
   * Including optimize. 
   */
  public void testNorms() throws IOException {
    Directory dir1 = new RAMDirectory();

    norms = new ArrayList();
    modifiedNorms = new ArrayList();

    createIndex(dir1);
    doTestNorms(dir1);

    // test with a single index: index2
    ArrayList norms1 = norms;
    ArrayList modifiedNorms1 = modifiedNorms;
    int numDocNorms1 = numDocNorms;

    norms = new ArrayList();
    modifiedNorms = new ArrayList();
    numDocNorms = 0;
    
    Directory dir2 = new RAMDirectory();

    createIndex(dir2);
    doTestNorms(dir2);

    // add index1 and index2 to a third index: index3
    Directory dir3 = new RAMDirectory();

    createIndex(dir3);
    IndexWriter iw = new IndexWriter(dir3,anlzr,false, IndexWriter.MaxFieldLength.LIMITED);
    iw.setMaxBufferedDocs(5);
    iw.setMergeFactor(3);
    iw.addIndexesNoOptimize(new Directory[]{dir1,dir2});
    iw.optimize();
    iw.close();
    
    norms1.addAll(norms);
    norms = norms1;
    modifiedNorms1.addAll(modifiedNorms);
    modifiedNorms = modifiedNorms1;
    numDocNorms += numDocNorms1;

    // test with index3
    verifyIndex(dir3);
    doTestNorms(dir3);
    
    // now with optimize
    iw = new IndexWriter(dir3,anlzr,false, IndexWriter.MaxFieldLength.LIMITED);
    iw.setMaxBufferedDocs(5);
    iw.setMergeFactor(3);
    iw.optimize();
    iw.close();
    verifyIndex(dir3);
    
    dir1.close();
    dir2.close();
    dir3.close();
  }

  private void doTestNorms(Directory dir) throws IOException {
    for (int i=0; i<5; i++) {
      addDocs(dir,12,true);
      verifyIndex(dir);
      modifyNormsForF1(dir);
      verifyIndex(dir);
      addDocs(dir,12,false);
      verifyIndex(dir);
      modifyNormsForF1(dir);
      verifyIndex(dir);
    }
  }

  private void createIndex(Directory dir) throws IOException {
    IndexWriter iw = new IndexWriter(dir,anlzr,true, IndexWriter.MaxFieldLength.LIMITED);
    iw.setMaxBufferedDocs(5);
    iw.setMergeFactor(3);
    iw.setSimilarity(similarityOne);
    iw.setUseCompoundFile(true);
    iw.close();
  }

  private void modifyNormsForF1(Directory dir) throws IOException {
    IndexReader ir = IndexReader.open(dir, false);
    int n = ir.maxDoc();
    for (int i = 0; i < n; i+=3) { // modify for every third doc
      int k = (i*3) % modifiedNorms.size();
      float origNorm = ((Float)modifiedNorms.get(i)).floatValue();
      float newNorm = ((Float)modifiedNorms.get(k)).floatValue();
      //System.out.println("Modifying: for "+i+" from "+origNorm+" to "+newNorm);
      //System.out.println("      and: for "+k+" from "+newNorm+" to "+origNorm);
      modifiedNorms.set(i, Float.valueOf(newNorm));
      modifiedNorms.set(k, Float.valueOf(origNorm));
      ir.setNorm(i, "f"+1, newNorm); 
      ir.setNorm(k, "f"+1, origNorm); 
    }
    ir.close();
  }


  private void verifyIndex(Directory dir) throws IOException {
    IndexReader ir = IndexReader.open(dir, false);
    for (int i = 0; i < NUM_FIELDS; i++) {
      String field = "f"+i;
      byte b[] = ir.norms(field);
      assertEquals("number of norms mismatches",numDocNorms,b.length);
      ArrayList storedNorms = (i==1 ? modifiedNorms : norms);
      for (int j = 0; j < b.length; j++) {
        float norm = Similarity.decodeNorm(b[j]);
        float norm1 = ((Float)storedNorms.get(j)).floatValue();
        assertEquals("stored norm value of "+field+" for doc "+j+" is "+norm+" - a mismatch!", norm, norm1, 0.000001);
      }
    }
  }

  private void addDocs(Directory dir, int ndocs, boolean compound) throws IOException {
    IndexWriter iw = new IndexWriter(dir,anlzr,false, IndexWriter.MaxFieldLength.LIMITED);
    iw.setMaxBufferedDocs(5);
    iw.setMergeFactor(3);
    iw.setSimilarity(similarityOne);
    iw.setUseCompoundFile(compound);
    for (int i = 0; i < ndocs; i++) {
      iw.addDocument(newDoc());
    }
    iw.close();
  }

  // create the next document
  private Document newDoc() {
    Document d = new Document();
    float boost = nextNorm();
    for (int i = 0; i < 10; i++) {
      Field f = new Field("f"+i,"v"+i,Store.NO,Index.NOT_ANALYZED);
      f.setBoost(boost);
      d.add(f);
    }
    return d;
  }

  // return unique norm values that are unchanged by encoding/decoding
  private float nextNorm() {
    float norm = lastNorm + normDelta;
    do {
      float norm1 = Similarity.decodeNorm(Similarity.encodeNorm(norm));
      if (norm1 > lastNorm) {
        //System.out.println(norm1+" > "+lastNorm);
        norm = norm1;
        break;
      }
      norm += normDelta;
    } while (true);
    norms.add(numDocNorms, Float.valueOf(norm));
    modifiedNorms.add(numDocNorms, Float.valueOf(norm));
    //System.out.println("creating norm("+numDocNorms+"): "+norm);
    numDocNorms ++;
    lastNorm = (norm>10 ? 0 : norm); //there's a limit to how many distinct values can be stored in a ingle byte
    return norm;
  }
  
}
