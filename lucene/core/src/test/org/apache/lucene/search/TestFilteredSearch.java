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

package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;



/**
 *
 */
public class TestFilteredSearch extends LuceneTestCase {

  private static final String FIELD = "category";
  
  public void testFilteredSearch() throws CorruptIndexException, LockObtainFailedException, IOException {
    boolean enforceSingleSegment = true;
    Directory directory = newDirectory();
    int[] filterBits = {1, 36};
    SimpleDocIdSetFilter filter = new SimpleDocIdSetFilter(filterBits);
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    searchFiltered(writer, directory, filter, enforceSingleSegment);
    // run the test on more than one segment
    enforceSingleSegment = false;
    // reset - it is stateful
    filter.reset();
    writer.close();
    writer = new IndexWriter(directory, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(10).setMergePolicy(newLogMergePolicy()));
    // we index 60 docs - this will create 6 segments
    searchFiltered(writer, directory, filter, enforceSingleSegment);
    writer.close();
    directory.close();
  }

  public void searchFiltered(IndexWriter writer, Directory directory, Filter filter, boolean fullMerge) {
    try {
      for (int i = 0; i < 60; i++) {//Simple docs
        Document doc = new Document();
        doc.add(newField(FIELD, Integer.toString(i), StringField.TYPE_STORED));
        writer.addDocument(doc);
      }
      if (fullMerge) {
        writer.forceMerge(1);
      }
      writer.close();

      BooleanQuery booleanQuery = new BooleanQuery();
      booleanQuery.add(new TermQuery(new Term(FIELD, "36")), BooleanClause.Occur.SHOULD);
     
     
      IndexReader reader = IndexReader.open(directory);
      IndexSearcher indexSearcher = new IndexSearcher(reader);
      ScoreDoc[] hits = indexSearcher.search(booleanQuery, filter, 1000).scoreDocs;
      assertEquals("Number of matched documents", 1, hits.length);
      reader.close();
    }
    catch (IOException e) {
      fail(e.getMessage());
    }
    
  }
 
  public static final class SimpleDocIdSetFilter extends Filter {
    private final int[] docs;
    private int index;
    
    public SimpleDocIdSetFilter(int[] docs) {
      this.docs = docs;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
      assertNull("acceptDocs should be null, as we have an index without deletions", acceptDocs);
      final FixedBitSet set = new FixedBitSet(context.reader().maxDoc());
      int docBase = context.docBase;
      final int limit = docBase+context.reader().maxDoc();
      for (;index < docs.length; index++) {
        final int docId = docs[index];
        if(docId > limit)
          break;
        if (docId >= docBase) {
          set.set(docId-docBase);
        }
      }
      return set.cardinality() == 0 ? null:set;
    }
    
    public void reset(){
      index = 0;
    }
  }

}
