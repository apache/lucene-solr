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

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

/**
 * Some tests for {@link ParallelReader}s with empty indexes
 * 
 * @author Christian Kohlschuetter
 */
public class TestParallelReaderEmptyIndex extends LuceneTestCase {

  /**
   * Creates two empty indexes and wraps a ParallelReader around. Adding this
   * reader to a new index should not throw any exception.
   * 
   * @throws IOException
   */
  public void testEmptyIndex() throws IOException {
    Directory rd1 = newDirectory();
    IndexWriter iw = new IndexWriter(rd1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    iw.close();

    Directory rd2 = newDirectory(rd1);

    Directory rdOut = newDirectory();

    IndexWriter iwOut = new IndexWriter(rdOut, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(rd1,true));
    pr.add(IndexReader.open(rd2,true));
		
    // When unpatched, Lucene crashes here with a NoSuchElementException (caused by ParallelTermEnum)
    iwOut.addIndexes(pr);
		
    iwOut.optimize();
    iwOut.close();
    _TestUtil.checkIndex(rdOut);
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
      IndexWriter iw = new IndexWriter(rd1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      Document doc = new Document();
      doc.add(newField("test", "", Store.NO, Index.ANALYZED,
                        TermVector.YES));
      iw.addDocument(doc);
      doc.add(newField("test", "", Store.NO, Index.ANALYZED,
                        TermVector.NO));
      iw.addDocument(doc);
      iw.close();

      IndexReader ir = IndexReader.open(rd1,false);
      ir.deleteDocument(0);
      ir.close();

      iw = new IndexWriter(rd1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
      iw.optimize();
      iw.close();
    }

    Directory rd2 = newDirectory();
    {
      IndexWriter iw = new IndexWriter(rd2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      Document doc = new Document();
      iw.addDocument(doc);
      iw.close();
    }

    Directory rdOut = newDirectory();

    IndexWriter iwOut = new IndexWriter(rdOut, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(rd1,true));
    pr.add(IndexReader.open(rd2,true));

    // When unpatched, Lucene crashes here with an ArrayIndexOutOfBoundsException (caused by TermVectorsWriter)
    iwOut.addIndexes(pr);

    // ParallelReader closes any IndexReader you added to it:
    pr.close();

    rd1.close();
    rd2.close();
		
    iwOut.optimize();
    iwOut.close();
    
    _TestUtil.checkIndex(rdOut);
    rdOut.close();
  }
}
