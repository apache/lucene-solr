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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Some tests for {@link ParallelLeafReader}s with empty indexes
 */
public class TestParallelReaderEmptyIndex extends LuceneTestCase {

  /**
   * Creates two empty indexes and wraps a ParallelReader around. Adding this
   * reader to a new index should not throw any exception.
   */
  public void testEmptyIndex() throws IOException {
    Directory rd1 = newDirectory();
    IndexWriter iw = new IndexWriter(rd1, newIndexWriterConfig(new MockAnalyzer(random())));
    iw.close();
    // create a copy:
    Directory rd2 = newDirectory(rd1);

    Directory rdOut = newDirectory();

    IndexWriter iwOut = new IndexWriter(rdOut, newIndexWriterConfig(new MockAnalyzer(random())));
    
    // add a readerless parallel reader
    iwOut.addIndexes(SlowCodecReaderWrapper.wrap(new ParallelLeafReader()));
    iwOut.forceMerge(1);

    ParallelCompositeReader cpr = new ParallelCompositeReader(
        DirectoryReader.open(rd1),
        DirectoryReader.open(rd2));
    
    // When unpatched, Lucene crashes here with a NoSuchElementException (caused by ParallelTermEnum)
    List<CodecReader> leaves = new ArrayList<>();
    for (LeafReaderContext leaf : cpr.leaves()) {
      leaves.add(SlowCodecReaderWrapper.wrap(leaf.reader()));
    }
    iwOut.addIndexes(leaves.toArray(new CodecReader[0]));
    iwOut.forceMerge(1);
    
    iwOut.close();
    rdOut.close();
    rd1.close();
    rd2.close();
  }

  /**
   * This method creates an empty index (numFields=0, numDocs=0) but is marked
   * to have TermVectors. Adding this index to another index should not throw
   * any exception.
   */
  public void testEmptyIndexWithVectors() throws IOException {
    Directory rd1 = newDirectory();
    {
      if (VERBOSE) {
        System.out.println("\nTEST: make 1st writer");
      }
      IndexWriter iw = new IndexWriter(rd1, newIndexWriterConfig(new MockAnalyzer(random())));
      Document doc = new Document();
      Field idField = newTextField("id", "", Field.Store.NO);
      doc.add(idField);
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      doc.add(newField("test", "", customType));
      idField.setStringValue("1");
      iw.addDocument(doc);
      doc.add(newField("test", "", customType));
      idField.setStringValue("2");
      iw.addDocument(doc);
      iw.close();

      IndexWriterConfig dontMergeConfig = new IndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(NoMergePolicy.INSTANCE);
      if (VERBOSE) {
        System.out.println("\nTEST: make 2nd writer");
      }
      IndexWriter writer = new IndexWriter(rd1, dontMergeConfig);
      
      writer.deleteDocuments(new Term("id", "1"));
      writer.close();
      IndexReader ir = DirectoryReader.open(rd1);
      assertEquals(2, ir.maxDoc());
      assertEquals(1, ir.numDocs());
      ir.close();

      iw = new IndexWriter(rd1, newIndexWriterConfig(new MockAnalyzer(random()))
                                  .setOpenMode(OpenMode.APPEND));
      iw.forceMerge(1);
      iw.close();
    }

    Directory rd2 = newDirectory();
    {
      IndexWriter iw = new IndexWriter(rd2, newIndexWriterConfig(new MockAnalyzer(random())));
      Document doc = new Document();
      iw.addDocument(doc);
      iw.close();
    }

    Directory rdOut = newDirectory();

    IndexWriter iwOut = new IndexWriter(rdOut, newIndexWriterConfig(new MockAnalyzer(random())));
    DirectoryReader reader1 = DirectoryReader.open(rd1);
    DirectoryReader reader2 = DirectoryReader.open(rd2);
    ParallelLeafReader pr = new ParallelLeafReader(false,
                                                   getOnlyLeafReader(reader1),
                                                   getOnlyLeafReader(reader2));

    // When unpatched, Lucene crashes here with an ArrayIndexOutOfBoundsException (caused by TermVectorsWriter)
    iwOut.addIndexes(SlowCodecReaderWrapper.wrap(pr));

    pr.close();
    reader1.close();
    reader2.close();
    
    // assert subreaders were closed
    assertEquals(0, reader1.getRefCount());
    assertEquals(0, reader2.getRefCount());

    rd1.close();
    rd2.close();

    iwOut.forceMerge(1);
    iwOut.close();
    
    rdOut.close();
  }
}
