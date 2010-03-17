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

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.RAMDirectory;

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
    RAMDirectory rd1 = new MockRAMDirectory();
    IndexWriter iw = new IndexWriter(rd1, new SimpleAnalyzer(), true,
                                     MaxFieldLength.UNLIMITED);
    iw.close();

    RAMDirectory rd2 = new MockRAMDirectory(rd1);

    RAMDirectory rdOut = new MockRAMDirectory();

    IndexWriter iwOut = new IndexWriter(rdOut, new SimpleAnalyzer(), true,
                                        MaxFieldLength.UNLIMITED);
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(rd1,true));
    pr.add(IndexReader.open(rd2,true));
		
    // When unpatched, Lucene crashes here with a NoSuchElementException (caused by ParallelTermEnum)
    iwOut.addIndexes(new IndexReader[] { pr });
		
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
    RAMDirectory rd1 = new MockRAMDirectory();
    {
      IndexWriter iw = new IndexWriter(rd1, new SimpleAnalyzer(), true,
                                       MaxFieldLength.UNLIMITED);
      Document doc = new Document();
      doc.add(new Field("test", "", Store.NO, Index.ANALYZED,
                        TermVector.YES));
      iw.addDocument(doc);
      doc.add(new Field("test", "", Store.NO, Index.ANALYZED,
                        TermVector.NO));
      iw.addDocument(doc);
      iw.close();

      IndexReader ir = IndexReader.open(rd1,false);
      ir.deleteDocument(0);
      ir.close();

      iw = new IndexWriter(rd1, new SimpleAnalyzer(), false,
                           MaxFieldLength.UNLIMITED);
      iw.optimize();
      iw.close();
    }

    RAMDirectory rd2 = new MockRAMDirectory();
    {
      IndexWriter iw = new IndexWriter(rd2, new SimpleAnalyzer(), true,
                                       MaxFieldLength.UNLIMITED);
      Document doc = new Document();
      iw.addDocument(doc);
      iw.close();
    }

    RAMDirectory rdOut = new MockRAMDirectory();

    IndexWriter iwOut = new IndexWriter(rdOut, new SimpleAnalyzer(), true,
                                        MaxFieldLength.UNLIMITED);
    ParallelReader pr = new ParallelReader();
    pr.add(IndexReader.open(rd1,true));
    pr.add(IndexReader.open(rd2,true));

    // When unpatched, Lucene crashes here with an ArrayIndexOutOfBoundsException (caused by TermVectorsWriter)
    iwOut.addIndexes(new IndexReader[] { pr });

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
