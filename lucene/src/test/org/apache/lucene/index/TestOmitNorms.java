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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;

public class TestOmitNorms extends LuceneTestCase {
  // Tests whether the DocumentWriter correctly enable the
  // omitNorms bit in the FieldInfo
  public void testOmitNorms() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random);
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    Document d = new Document();
        
    // this field will have norms
    Field f1 = newField("f1", "This field has norms", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f1);
       
    // this field will NOT have norms
    Field f2 = newField("f2", "This field has NO norms in all docs", Field.Store.NO, Field.Index.ANALYZED);
    f2.setOmitNorms(true);
    d.add(f2);
        
    writer.addDocument(d);
    writer.optimize();
    // now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = new Document();
        
    // Reverse
    f1.setOmitNorms(true);
    d.add(f1);
        
    f2.setOmitNorms(false);        
    d.add(f2);
        
    writer.addDocument(d);

    // force merge
    writer.optimize();
    // flush
    writer.close();
    _TestUtil.checkIndex(ram);

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram, false));
    FieldInfos fi = reader.fieldInfos();
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f1").omitNorms);
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f2").omitNorms);
        
    reader.close();
    ram.close();
  }
 
  // Tests whether merging of docs that have different
  // omitNorms for the same field works
  public void testMixedMerge() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random);
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer).
            setMaxBufferedDocs(3).
            setMergePolicy(newLogMergePolicy(2))
    );
    Document d = new Document();
        
    // this field will have norms
    Field f1 = newField("f1", "This field has norms", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f1);
       
    // this field will NOT have norms
    Field f2 = newField("f2", "This field has NO norms in all docs", Field.Store.NO, Field.Index.ANALYZED);
    f2.setOmitNorms(true);
    d.add(f2);

    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }
        
    // now we add another document which has norms for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = new Document();
        
    // Reverese
    f1.setOmitNorms(true);
    d.add(f1);
        
    f2.setOmitNorms(false);        
    d.add(f2);
        
    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }
        
    // force merge
    writer.optimize();
    // flush
    writer.close();

    _TestUtil.checkIndex(ram);

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram, false));
    FieldInfos fi = reader.fieldInfos();
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f1").omitNorms);
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f2").omitNorms);
        
    reader.close();
    ram.close();
  }

  // Make sure first adding docs that do not omitNorms for
  // field X, then adding docs that do omitNorms for that same
  // field, 
  public void testMixedRAM() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random);
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer).
            setMaxBufferedDocs(10).
            setMergePolicy(newLogMergePolicy(2))
    );
    Document d = new Document();
        
    // this field will have norms
    Field f1 = newField("f1", "This field has norms", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f1);
       
    // this field will NOT have norms
    Field f2 = newField("f2", "This field has NO norms in all docs", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f2);

    for (int i = 0; i < 5; i++) {
      writer.addDocument(d);
    }

    f2.setOmitNorms(true);
        
    for (int i = 0; i < 20; i++) {
      writer.addDocument(d);
    }

    // force merge
    writer.optimize();

    // flush
    writer.close();

    _TestUtil.checkIndex(ram);

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram, false));
    FieldInfos fi = reader.fieldInfos();
    assertTrue("OmitNorms field bit should not be set.", !fi.fieldInfo("f1").omitNorms);
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f2").omitNorms);
        
    reader.close();
    ram.close();
  }

  private void assertNoNrm(Directory dir) throws Throwable {
    final String[] files = dir.listAll();
    for (int i = 0; i < files.length; i++) {
      assertFalse(files[i].endsWith(".nrm"));
    }
  }

  // Verifies no *.nrm exists when all fields omit norms:
  public void testNoNrmFile() throws Throwable {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random);
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(
            TEST_VERSION_CURRENT, analyzer).setMaxBufferedDocs(3).setMergePolicy(newLogMergePolicy()));
    writer.setInfoStream(VERBOSE ? System.out : null);
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setMergeFactor(2);
    lmp.setUseCompoundFile(false);
    Document d = new Document();
        
    Field f1 = newField("f1", "This field has no norms", Field.Store.NO, Field.Index.ANALYZED);
    f1.setOmitNorms(true);
    d.add(f1);

    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }

    writer.commit();

    assertNoNrm(ram);
        
    // force merge
    writer.optimize();
    // flush
    writer.close();

    assertNoNrm(ram);
    _TestUtil.checkIndex(ram);
    ram.close();
  }
  
  /**
   * Tests various combinations of omitNorms=true/false, the field not existing at all,
   * ensuring that only omitNorms is 'viral'.
   * Internally checks that MultiNorms.norms() is consistent (returns the same bytes)
   * as the optimized equivalent.
   */
  public void testOmitNormsCombos() throws IOException {
    // indexed with norms
    Field norms = new Field("foo", "a", Field.Store.YES, Field.Index.ANALYZED);
    // indexed without norms
    Field noNorms = new Field("foo", "a", Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
    // not indexed, but stored
    Field noIndex = new Field("foo", "a", Field.Store.YES, Field.Index.NO);
    // not indexed but stored, omitNorms is set
    Field noNormsNoIndex = new Field("foo", "a", Field.Store.YES, Field.Index.NO);
    noNormsNoIndex.setOmitNorms(true);
    // not indexed nor stored (doesnt exist at all, we index a different field instead)
    Field emptyNorms = new Field("bar", "a", Field.Store.YES, Field.Index.ANALYZED);
    
    assertNotNull(getNorms("foo", norms, norms));
    assertNull(getNorms("foo", norms, noNorms));
    assertNotNull(getNorms("foo", norms, noIndex));
    assertNotNull(getNorms("foo", norms, noNormsNoIndex));
    assertNotNull(getNorms("foo", norms, emptyNorms));
    assertNull(getNorms("foo", noNorms, noNorms));
    assertNull(getNorms("foo", noNorms, noIndex));
    assertNull(getNorms("foo", noNorms, noNormsNoIndex));
    assertNull(getNorms("foo", noNorms, emptyNorms));
    assertNull(getNorms("foo", noIndex, noIndex));
    assertNull(getNorms("foo", noIndex, noNormsNoIndex));
    assertNull(getNorms("foo", noIndex, emptyNorms));
    assertNull(getNorms("foo", noNormsNoIndex, noNormsNoIndex));
    assertNull(getNorms("foo", noNormsNoIndex, emptyNorms));
    assertNull(getNorms("foo", emptyNorms, emptyNorms));
  }

  /**
   * Indexes at least 1 document with f1, and at least 1 document with f2.
   * returns the norms for "field".
   */
  static byte[] getNorms(String field, Field f1, Field f2) throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy());
    RandomIndexWriter riw = new RandomIndexWriter(random, dir, iwc);
    
    // add f1
    Document d = new Document();
    d.add(f1);
    riw.addDocument(d);
    
    // add f2
    d = new Document();
    d.add(f2);
    riw.addDocument(d);
    
    // add a mix of f1's and f2's
    int numExtraDocs = _TestUtil.nextInt(random, 1, 1000);
    for (int i = 0; i < numExtraDocs; i++) {
      d = new Document();
      d.add(random.nextBoolean() ? f1 : f2);
      riw.addDocument(d);
    }

    IndexReader ir1 = riw.getReader();
    byte[] norms1 = MultiNorms.norms(ir1, field);
    
    // optimize and validate MultiNorms against single segment.
    riw.optimize();
    IndexReader ir2 = riw.getReader();
    byte[] norms2 = ir2.getSequentialSubReaders()[0].norms(field);
    
    assertArrayEquals(norms1, norms2);
    ir1.close();
    ir2.close();
    riw.close();
    dir.close();
    return norms1;
  }
}
