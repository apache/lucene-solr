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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SegmentReader.Norm;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.DefaultSimilarityProvider;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests cloning IndexReader norms
 */
public class TestIndexReaderCloneNorms extends LuceneTestCase {

  private class SimilarityProviderOne extends DefaultSimilarityProvider {
    @Override
    public Similarity get(String field) {
      return new DefaultSimilarity() {
        @Override
        public float computeNorm(FieldInvertState state) {
          // diable length norm
          return state.getBoost();
        }
      };
    } 
  }

  private static final int NUM_FIELDS = 10;

  private SimilarityProvider similarityProviderOne;

  private Analyzer anlzr;

  private int numDocNorms;

  private ArrayList<Float> norms;

  private ArrayList<Float> modifiedNorms;

  private float lastNorm = 0;

  private float normDelta = (float) 0.001;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    similarityProviderOne = new SimilarityProviderOne();
    anlzr = new MockAnalyzer(random);
  }
  
  /**
   * Test that norms values are preserved as the index is maintained. Including
   * separate norms. Including merging indexes with seprate norms. Including
   * optimize.
   */
  public void testNorms() throws IOException {
    // test with a single index: index1
    Directory dir1 = newDirectory();
    IndexWriter.unlock(dir1);

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
    if (VERBOSE) {
      System.out.println("TEST: now addIndexes/optimize");
    }
    IndexWriter iw = new IndexWriter(
        dir3,
        newIndexWriterConfig(TEST_VERSION_CURRENT, anlzr).
            setOpenMode(OpenMode.APPEND).
            setMaxBufferedDocs(5).
        setMergePolicy(newLogMergePolicy(3))
    );
    iw.setInfoStream(VERBOSE ? System.out : null);
    iw.addIndexes(dir1, dir2);
    iw.optimize();
    iw.close();

    norms1.addAll(norms);
    norms = norms1;
    modifiedNorms1.addAll(modifiedNorms);
    modifiedNorms = modifiedNorms1;
    numDocNorms += numDocNorms1;

    // test with index3
    verifyIndex(dir3);
    doTestNorms(random, dir3);

    // now with optimize
    iw = new IndexWriter(
        dir3,
        newIndexWriterConfig(TEST_VERSION_CURRENT, anlzr).
            setOpenMode(OpenMode.APPEND).
            setMaxBufferedDocs(5).
            setMergePolicy(newLogMergePolicy(3))
    );
    iw.optimize();
    iw.close();
    verifyIndex(dir3);

    dir1.close();
    dir2.close();
    dir3.close();
  }

  // try cloning and reopening the norms
  private void doTestNorms(Random random, Directory dir) throws IOException {
    if (VERBOSE) {
      System.out.println("TEST: now doTestNorms");
    }
    addDocs(random, dir, 12, true);
    IndexReader ir = IndexReader.open(dir, false);
    verifyIndex(ir);
    modifyNormsForF1(ir);
    IndexReader irc = (IndexReader) ir.clone();// IndexReader.open(dir, false);//ir.clone();
    verifyIndex(irc);

    modifyNormsForF1(irc);

    IndexReader irc3 = (IndexReader) irc.clone();
    verifyIndex(irc3);
    modifyNormsForF1(irc3);
    verifyIndex(irc3);
    irc3.flush();
    
    ir.close();
    irc.close();
    irc3.close();
  }
  
  public void testNormsClose() throws IOException { 
    Directory dir1 = newDirectory(); 
    TestIndexReaderReopen.createIndex(random, dir1, false);
    SegmentReader reader1 = getOnlySegmentReader(IndexReader.open(dir1, false));
    reader1.norms("field1");
    Norm r1norm = reader1.norms.get("field1");
    AtomicInteger r1BytesRef = r1norm.bytesRef();
    SegmentReader reader2 = (SegmentReader)reader1.clone();
    assertEquals(2, r1norm.bytesRef().get());
    reader1.close();
    assertEquals(1, r1BytesRef.get());
    reader2.norms("field1");
    reader2.close();
    dir1.close();
  }
  
  public void testNormsRefCounting() throws IOException { 
    Directory dir1 = newDirectory(); 
    TestIndexReaderReopen.createIndex(random, dir1, false);
    IndexReader reader1 = IndexReader.open(dir1, false);
        
    IndexReader reader2C = (IndexReader) reader1.clone();
    SegmentReader segmentReader2C = getOnlySegmentReader(reader2C);
    segmentReader2C.norms("field1"); // load the norms for the field
    Norm reader2CNorm = segmentReader2C.norms.get("field1");
    assertTrue("reader2CNorm.bytesRef()=" + reader2CNorm.bytesRef(), reader2CNorm.bytesRef().get() == 2);
    
    
    
    IndexReader reader3C = (IndexReader) reader2C.clone();
    SegmentReader segmentReader3C = getOnlySegmentReader(reader3C);
    Norm reader3CCNorm = segmentReader3C.norms.get("field1");
    assertEquals(3, reader3CCNorm.bytesRef().get());
    
    // edit a norm and the refcount should be 1
    IndexReader reader4C = (IndexReader) reader3C.clone();
    SegmentReader segmentReader4C = getOnlySegmentReader(reader4C);
    assertEquals(4, reader3CCNorm.bytesRef().get());
    Similarity sim = new DefaultSimilarity();
    reader4C.setNorm(5, "field1", sim.encodeNormValue(0.33f));
    
    // generate a cannot update exception in reader1
    try {
      reader3C.setNorm(1, "field1", sim.encodeNormValue(0.99f));
      fail("did not hit expected exception");
    } catch (Exception ex) {
      // expected
    }
    
    // norm values should be different 
    assertTrue(sim.decodeNormValue(segmentReader3C.norms("field1")[5]) 
    		!= sim.decodeNormValue(segmentReader4C.norms("field1")[5]));
    Norm reader4CCNorm = segmentReader4C.norms.get("field1");
    assertEquals(3, reader3CCNorm.bytesRef().get());
    assertEquals(1, reader4CCNorm.bytesRef().get());
        
    IndexReader reader5C = (IndexReader) reader4C.clone();
    SegmentReader segmentReader5C = getOnlySegmentReader(reader5C);
    Norm reader5CCNorm = segmentReader5C.norms.get("field1");
    reader5C.setNorm(5, "field1", sim.encodeNormValue(0.7f));
    assertEquals(1, reader5CCNorm.bytesRef().get());

    reader5C.close();
    reader4C.close();
    reader3C.close();
    reader2C.close();
    reader1.close();
    dir1.close();
  }
  
  private void createIndex(Random random, Directory dir) throws IOException {
    if (VERBOSE) {
      System.out.println("TEST: createIndex");
    }
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, anlzr).setOpenMode(OpenMode.CREATE)
                                     .setMaxBufferedDocs(5).setSimilarityProvider(similarityProviderOne).setMergePolicy(newLogMergePolicy()));

    LogMergePolicy lmp = (LogMergePolicy) iw.getConfig().getMergePolicy();
    lmp.setMergeFactor(3);
    lmp.setUseCompoundFile(true);
    iw.close();
    if (VERBOSE) {
      System.out.println("TEST: done createIndex");
    }
  }

  private void modifyNormsForF1(IndexReader ir) throws IOException {
    int n = ir.maxDoc();
    // System.out.println("modifyNormsForF1 maxDoc: "+n);
    for (int i = 0; i < n; i += 3) { // modify for every third doc
      int k = (i * 3) % modifiedNorms.size();
      float origNorm =  modifiedNorms.get(i).floatValue();
      float newNorm =  modifiedNorms.get(k).floatValue();
      // System.out.println("Modifying: for "+i+" from "+origNorm+" to
      // "+newNorm);
      // System.out.println(" and: for "+k+" from "+newNorm+" to "+origNorm);
      modifiedNorms.set(i, Float.valueOf(newNorm));
      modifiedNorms.set(k, Float.valueOf(origNorm));
      Similarity sim = new DefaultSimilarity();
      ir.setNorm(i, "f" + 1, sim.encodeNormValue(newNorm));
      ir.setNorm(k, "f" + 1, sim.encodeNormValue(origNorm));
      // System.out.println("setNorm i: "+i);
      // break;
    }
    // ir.close();
  }

  private void verifyIndex(Directory dir) throws IOException {
    IndexReader ir = IndexReader.open(dir, false);
    verifyIndex(ir);
    ir.close();
  }

  private void verifyIndex(IndexReader ir) throws IOException {
    for (int i = 0; i < NUM_FIELDS; i++) {
      String field = "f" + i;
      byte b[] = MultiNorms.norms(ir, field);
      assertEquals("number of norms mismatches", numDocNorms, b.length);
      ArrayList<Float> storedNorms = (i == 1 ? modifiedNorms : norms);
      for (int j = 0; j < b.length; j++) {
        Similarity sim = new DefaultSimilarity();
        float norm = sim.decodeNormValue(b[j]);
        float norm1 =  storedNorms.get(j).floatValue();
        assertEquals("stored norm value of " + field + " for doc " + j + " is "
            + norm + " - a mismatch!", norm, norm1, 0.000001);
      }
    }
  }

  private void addDocs(Random random, Directory dir, int ndocs, boolean compound)
      throws IOException {
    IndexWriterConfig conf = newIndexWriterConfig(
            TEST_VERSION_CURRENT, anlzr).setOpenMode(OpenMode.APPEND)
            .setMaxBufferedDocs(5).setSimilarityProvider(similarityProviderOne).setMergePolicy(newLogMergePolicy());
    LogMergePolicy lmp = (LogMergePolicy) conf.getMergePolicy();
    lmp.setMergeFactor(3);
    lmp.setUseCompoundFile(compound);
    IndexWriter iw = new IndexWriter(dir, conf);
    iw.setInfoStream(VERBOSE ? System.out : null);
    for (int i = 0; i < ndocs; i++) {
      iw.addDocument(newDoc());
    }
    iw.close();
  }

  // create the next document
  private Document newDoc() {
    Document d = new Document();
    float boost = nextNorm("anyfield"); // in this test the same similarity is used for all fields so it does not matter what field is passed
    for (int i = 0; i < 10; i++) {
      Field f = newField("f" + i, "v" + i, Store.NO, Index.NOT_ANALYZED);
      f.setBoost(boost);
      d.add(f);
    }
    return d;
  }

  // return unique norm values that are unchanged by encoding/decoding
  private float nextNorm(String fname) {
    float norm = lastNorm + normDelta;
    Similarity sim = new DefaultSimilarity();
    do {
      float norm1 = sim.decodeNormValue(
    		  sim.encodeNormValue(norm));
      if (norm1 > lastNorm) {
        // System.out.println(norm1+" > "+lastNorm);
        norm = norm1;
        break;
      }
      norm += normDelta;
    } while (true);
    norms.add(numDocNorms, Float.valueOf(norm));
    modifiedNorms.add(numDocNorms, Float.valueOf(norm));
    // System.out.println("creating norm("+numDocNorms+"): "+norm);
    numDocNorms++;
    lastNorm = (norm > 10 ? 0 : norm); // there's a limit to how many distinct
                                        // values can be stored in a ingle byte
    return norm;
  }
}
