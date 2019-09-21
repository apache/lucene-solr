/*
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
package org.apache.lucene.index;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * 
 * @lucene.experimental
 */
public class TestOmitPositions extends LuceneTestCase {

  public void testBasic() throws Exception {   
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Field f = newField("foo", "this is a test test", ft);
    doc.add(f);
    for (int i = 0; i < 100; i++) {
      w.addDocument(doc);
    }
    
    IndexReader reader = w.getReader();
    w.close();
    
    assertNotNull(MultiTerms.getTermPostingsEnum(reader, "foo", new BytesRef("test")));
    
    PostingsEnum de = TestUtil.docs(random(), reader, "foo", new BytesRef("test"), null, PostingsEnum.FREQS);
    while (de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(2, de.freq());
    }
    
    reader.close();
    dir.close();
  }
  
  // Tests whether the DocumentWriter correctly enable the
  // omitTermFreqAndPositions bit in the FieldInfo
  public void testPositions() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(analyzer));
    Document d = new Document();
        
    // f1: docs only
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS);
    
    Field f1 = newField("f1", "This field has docs only", ft);
    d.add(f1);

    FieldType ft2 = new FieldType(TextField.TYPE_NOT_STORED);
    ft2.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    
    // f2: docs and freqs
    Field f2 = newField("f2", "This field has docs and freqs", ft2);
    d.add(f2);
    
    FieldType ft3 = new FieldType(TextField.TYPE_NOT_STORED);
    ft3.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    
    // f3: docs/freqs/positions
    Field f3 = newField("f3", "This field has docs and freqs and positions", ft3);
    d.add(f3);
        
    writer.addDocument(d);
    writer.forceMerge(1);
    // flush
    writer.close();

    LeafReader reader = getOnlyLeafReader(DirectoryReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    // docs + docs = docs
    assertEquals(IndexOptions.DOCS, fi.fieldInfo("f1").getIndexOptions());
    // docs/freqs + docs/freqs = docs/freqs
    assertEquals(IndexOptions.DOCS_AND_FREQS, fi.fieldInfo("f2").getIndexOptions());
    // docs/freqs/pos + docs/freqs/pos = docs/freqs/pos
    assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fi.fieldInfo("f3").getIndexOptions());
    
    reader.close();
    ram.close();
  }
  
  private void assertNoPrx(Directory dir) throws Throwable {
    final String[] files = dir.listAll();
    for(int i=0;i<files.length;i++) {
      assertFalse(files[i].endsWith(".prx"));
      assertFalse(files[i].endsWith(".pos"));
    }
  }

  // Verifies no *.prx exists when all fields omit term positions:
  public void testNoPrxFile() throws Throwable {
    Directory ram = newDirectory();

    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(analyzer)
                                                .setMaxBufferedDocs(3)
                                                .setMergePolicy(newLogMergePolicy()));
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setMergeFactor(2);
    lmp.setNoCFSRatio(0.0);
    Document d = new Document();

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Field f1 = newField("f1", "This field has term freqs", ft);
    d.add(f1);

    for(int i=0;i<30;i++)
      writer.addDocument(d);

    writer.commit();

    assertNoPrx(ram);
    
    writer.close();
    ram.close();
  }

}
