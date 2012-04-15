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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;

public class TestOmitNorms extends LuceneTestCase {
  // Tests whether the DocumentWriter correctly enable the
  // omitNorms bit in the FieldInfo
  public void testOmitNorms() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    Document d = new Document();
        
    // this field will have norms
    Field f1 = newField("f1", "This field has norms", TextField.TYPE_UNSTORED);
    d.add(f1);
       
    // this field will NOT have norms
    FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
    customType.setOmitNorms(true);
    Field f2 = newField("f2", "This field has NO norms in all docs", customType);
    d.add(f2);
        
    writer.addDocument(d);
    writer.forceMerge(1);
    // now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = new Document();
        
    // Reverse
    d.add(newField("f1", "This field has norms", customType));
        
    d.add(newField("f2", "This field has NO norms in all docs", TextField.TYPE_UNSTORED));
        
    writer.addDocument(d);

    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f1").omitNorms);
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f2").omitNorms);
        
    reader.close();
    ram.close();
  }
 
  // Tests whether merging of docs that have different
  // omitNorms for the same field works
  public void testMixedMerge() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer).
            setMaxBufferedDocs(3).
            setMergePolicy(newLogMergePolicy(2))
    );
    Document d = new Document();
        
    // this field will have norms
    Field f1 = newField("f1", "This field has norms", TextField.TYPE_UNSTORED);
    d.add(f1);
       
    // this field will NOT have norms
    FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
    customType.setOmitNorms(true);
    Field f2 = newField("f2", "This field has NO norms in all docs", customType);
    d.add(f2);

    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }
        
    // now we add another document which has norms for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = new Document();
        
    // Reverese
    d.add(newField("f1", "This field has norms", customType));
        
    d.add(newField("f2", "This field has NO norms in all docs", TextField.TYPE_UNSTORED));
        
    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }
        
    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
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
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer).
            setMaxBufferedDocs(10).
            setMergePolicy(newLogMergePolicy(2))
    );
    Document d = new Document();
        
    // this field will have norms
    Field f1 = newField("f1", "This field has norms", TextField.TYPE_UNSTORED);
    d.add(f1);
       
    // this field will NOT have norms

    FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
    customType.setOmitNorms(true);
    Field f2 = newField("f2", "This field has NO norms in all docs", customType);
    d.add(f2);

    for (int i = 0; i < 5; i++) {
      writer.addDocument(d);
    }
        
    for (int i = 0; i < 20; i++) {
      writer.addDocument(d);
    }

    // force merge
    writer.forceMerge(1);

    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertTrue("OmitNorms field bit should not be set.", !fi.fieldInfo("f1").omitNorms);
    assertTrue("OmitNorms field bit should be set.", fi.fieldInfo("f2").omitNorms);
        
    reader.close();
    ram.close();
  }

  private void assertNoNrm(Directory dir) throws Throwable {
    final String[] files = dir.listAll();
    for (int i = 0; i < files.length; i++) {
      // TODO: this relies upon filenames
      assertFalse(files[i].endsWith(".nrm") || files[i].endsWith(".len"));
    }
  }

  // Verifies no *.nrm exists when all fields omit norms:
  public void testNoNrmFile() throws Throwable {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(
            TEST_VERSION_CURRENT, analyzer).setMaxBufferedDocs(3).setMergePolicy(newLogMergePolicy()));
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setMergeFactor(2);
    lmp.setUseCompoundFile(false);
    Document d = new Document();

    FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
    customType.setOmitNorms(true);
    Field f1 = newField("f1", "This field has no norms", customType);
    d.add(f1);

    for (int i = 0; i < 30; i++) {
      writer.addDocument(d);
    }

    writer.commit();

    assertNoNrm(ram);
        
    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    assertNoNrm(ram);
    ram.close();
  }
  
  /**
   * Tests various combinations of omitNorms=true/false, the field not existing at all,
   * ensuring that only omitNorms is 'viral'.
   * Internally checks that MultiNorms.norms() is consistent (returns the same bytes)
   * as the fully merged equivalent.
   */
  public void testOmitNormsCombos() throws IOException {
    // indexed with norms
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    Field norms = new Field("foo", "a", customType);
    // indexed without norms
    FieldType customType1 = new FieldType(TextField.TYPE_STORED);
    customType1.setOmitNorms(true);
    Field noNorms = new Field("foo", "a", customType1);
    // not indexed, but stored
    FieldType customType2 = new FieldType();
    customType2.setStored(true);
    Field noIndex = new Field("foo", "a", customType2);
    // not indexed but stored, omitNorms is set
    FieldType customType3 = new FieldType();
    customType3.setStored(true);
    customType3.setOmitNorms(true);
    Field noNormsNoIndex = new Field("foo", "a", customType3);
    // not indexed nor stored (doesnt exist at all, we index a different field instead)
    Field emptyNorms = new Field("bar", "a", customType);
    
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
  byte[] getNorms(String field, Field f1, Field f2) throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy());
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, iwc);
    
    // add f1
    Document d = new Document();
    d.add(f1);
    riw.addDocument(d);
    
    // add f2
    d = new Document();
    d.add(f2);
    riw.addDocument(d);
    
    // add a mix of f1's and f2's
    int numExtraDocs = _TestUtil.nextInt(random(), 1, 1000);
    for (int i = 0; i < numExtraDocs; i++) {
      d = new Document();
      d.add(random().nextBoolean() ? f1 : f2);
      riw.addDocument(d);
    }

    IndexReader ir1 = riw.getReader();
    // todo: generalize
    DocValues dv1 = MultiDocValues.getNormDocValues(ir1, field);
    byte[] norms1 = dv1 == null ? null : (byte[]) dv1.getSource().getArray();
    
    // fully merge and validate MultiNorms against single segment.
    riw.forceMerge(1);
    DirectoryReader ir2 = riw.getReader();
    DocValues dv2 = getOnlySegmentReader(ir2).normValues(field);
    byte[] norms2 = dv2 == null ? null : (byte[]) dv2.getSource().getArray();
    
    assertArrayEquals(norms1, norms2);
    ir1.close();
    ir2.close();
    riw.close();
    dir.close();
    return norms1;
  }
}
