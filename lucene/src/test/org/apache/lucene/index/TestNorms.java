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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test that norms info is preserved during index life - including
 * separate norms, addDocument, addIndexes, forceMerge.
 */
public class TestNorms extends LuceneTestCase {

  private class SimilarityOne extends DefaultSimilarity {
    @Override
    public float computeNorm(String fieldName, FieldInvertState state) {
      // Disable length norm
      return state.getBoost();
    }
  }

  private static final int NUM_FIELDS = 10;
  
  private Similarity similarityOne;
  private Analyzer anlzr;
  private int numDocNorms;
  private ArrayList<Float> norms; 
  private ArrayList<Float> modifiedNorms; 
  private float lastNorm = 0;
  private float normDelta = (float) 0.001;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    similarityOne = new SimilarityOne();
    anlzr = new MockAnalyzer(random);
  }

  /**
   * Test that norms values are preserved as the index is maintained.
   * Including separate norms.
   * Including merging indexes with seprate norms. 
   * Including forceMerge. 
   */
  public void testNorms() throws IOException {
    Directory dir1 = newDirectory();

    norms = new ArrayList<Float>();
    modifiedNorms = new ArrayList<Float>();

    createIndex(random, dir1);
    doTestNorms(random, dir1);

    // test with a single index: index2
    ArrayList<Float> norms1 = norms;
    ArrayList<Float> modifiedNorms1 = modifiedNorms;
    int numDocNorms1 = numDocNorms;

    norms = new ArrayList<Float>();
    modifiedNorms = new ArrayList<Float>();
    numDocNorms = 0;
    
    Directory dir2 = newDirectory();

    createIndex(random, dir2);
    doTestNorms(random, dir2);

    // add index1 and index2 to a third index: index3
    Directory dir3 = newDirectory();

    createIndex(random, dir3);
    IndexWriter iw = new IndexWriter(dir3, newIndexWriterConfig(
        TEST_VERSION_CURRENT, anlzr).setOpenMode(OpenMode.APPEND)
                                     .setMaxBufferedDocs(5).setMergePolicy(newLogMergePolicy(3)));
    iw.addIndexes(new Directory[]{dir1,dir2});
    iw.forceMerge(1);
    iw.close();
    
    norms1.addAll(norms);
    norms = norms1;
    modifiedNorms1.addAll(modifiedNorms);
    modifiedNorms = modifiedNorms1;
    numDocNorms += numDocNorms1;

    // test with index3
    verifyIndex(dir3);
    doTestNorms(random, dir3);
    
    // now with single segment
    iw = new IndexWriter(dir3, newIndexWriterConfig( TEST_VERSION_CURRENT,
        anlzr).setOpenMode(OpenMode.APPEND).setMaxBufferedDocs(5).setMergePolicy(newLogMergePolicy(3)));
    iw.forceMerge(1);
    iw.close();
    verifyIndex(dir3);
    
    dir1.close();
    dir2.close();
    dir3.close();
  }

  private void doTestNorms(Random random, Directory dir) throws IOException {
    int num = atLeast(1);
    for (int i=0; i<num; i++) {
      addDocs(random, dir,12,true);
      verifyIndex(dir);
      modifyNormsForF1(dir);
      verifyIndex(dir);
      addDocs(random, dir,12,false);
      verifyIndex(dir);
      modifyNormsForF1(dir);
      verifyIndex(dir);
    }
  }

  private void createIndex(Random random, Directory dir) throws IOException {
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, anlzr).setOpenMode(OpenMode.CREATE)
        .setMaxBufferedDocs(5).setSimilarity(similarityOne).setMergePolicy(newLogMergePolicy()));
    LogMergePolicy lmp = (LogMergePolicy) iw.getConfig().getMergePolicy();
    lmp.setMergeFactor(3);
    lmp.setUseCompoundFile(true);
    iw.close();
  }

  private void modifyNormsForF1(Directory dir) throws IOException {
    IndexReader ir = IndexReader.open(dir, false);
    int n = ir.maxDoc();
    for (int i = 0; i < n; i+=3) { // modify for every third doc
      int k = (i*3) % modifiedNorms.size();
      float origNorm = modifiedNorms.get(i).floatValue();
      float newNorm = modifiedNorms.get(k).floatValue();
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
      ArrayList<Float> storedNorms = (i==1 ? modifiedNorms : norms);
      for (int j = 0; j < b.length; j++) {
        float norm = similarityOne.decodeNormValue(b[j]);
        float norm1 = storedNorms.get(j).floatValue();
        assertEquals("stored norm value of "+field+" for doc "+j+" is "+norm+" - a mismatch!", norm, norm1, 0.000001);
      }
    }
    ir.close();
  }

  private void addDocs(Random random, Directory dir, int ndocs, boolean compound) throws IOException {
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, anlzr).setOpenMode(OpenMode.APPEND)
        .setMaxBufferedDocs(5).setSimilarity(similarityOne).setMergePolicy(newLogMergePolicy()));
    LogMergePolicy lmp = (LogMergePolicy) iw.getConfig().getMergePolicy();
    lmp.setMergeFactor(3);
    lmp.setUseCompoundFile(compound);
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
      Field f = newField("f"+i,"v"+i,Store.NO,Index.NOT_ANALYZED);
      f.setBoost(boost);
      d.add(f);
    }
    return d;
  }

  // return unique norm values that are unchanged by encoding/decoding
  private float nextNorm() {
    float norm = lastNorm + normDelta;
    do {
      float norm1 = similarityOne.decodeNormValue(similarityOne.encodeNormValue(norm));
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
  
  class CustomNormEncodingSimilarity extends DefaultSimilarity {
    @Override
    public byte encodeNormValue(float f) {
      return (byte) f;
    }
    
    @Override
    public float decodeNormValue(byte b) {
      return (float) b;
    }

    @Override
    public float computeNorm(String field, FieldInvertState state) {
      return (float) state.getLength();
    }
  }
  
  // LUCENE-1260
  public void testCustomEncoder() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    config.setSimilarity(new CustomNormEncodingSimilarity());
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, config);
    Document doc = new Document();
    Field foo = newField("foo", "", Field.Store.NO, Field.Index.ANALYZED);
    Field bar = newField("bar", "", Field.Store.NO, Field.Index.ANALYZED);
    doc.add(foo);
    doc.add(bar);
    
    for (int i = 0; i < 100; i++) {
      bar.setValue("singleton");
      writer.addDocument(doc);
    }
    
    IndexReader reader = writer.getReader();
    writer.close();
    
    byte fooNorms[] = reader.norms("foo");
    for (int i = 0; i < reader.maxDoc(); i++)
      assertEquals(0, fooNorms[i]);
    
    byte barNorms[] = reader.norms("bar");
    for (int i = 0; i < reader.maxDoc(); i++)
      assertEquals(1, barNorms[i]);
    
    reader.close();
    dir.close();
  }
}
