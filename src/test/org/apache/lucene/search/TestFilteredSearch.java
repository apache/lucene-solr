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

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.OpenBitSet;


/**
 *
 */
public class TestFilteredSearch extends TestCase
{

  public TestFilteredSearch(String name) {
    super(name);
  }

  private static final String FIELD = "category";
  
  public void testFilteredSearch() {
    RAMDirectory directory = new RAMDirectory();
    int[] filterBits = {1, 36};
    Filter filter = new SimpleDocIdSetFilter(filterBits);
    

    try {
      IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      for (int i = 0; i < 60; i++) {//Simple docs
        Document doc = new Document();
        doc.add(new Field(FIELD, Integer.toString(i), Field.Store.YES, Field.Index.NOT_ANALYZED));
        writer.addDocument(doc);
      }
      writer.close();

      BooleanQuery booleanQuery = new BooleanQuery();
      booleanQuery.add(new TermQuery(new Term(FIELD, "36")), BooleanClause.Occur.SHOULD);
     
     
      IndexSearcher indexSearcher = new IndexSearcher(directory);
      ScoreDoc[] hits = indexSearcher.search(booleanQuery, filter, 1000).scoreDocs;
      assertEquals("Number of matched documents", 1, hits.length);

    }
    catch (IOException e) {
      fail(e.getMessage());
    }

  }
  

  public static final class SimpleDocIdSetFilter extends Filter {
    private OpenBitSet bits;

    public SimpleDocIdSetFilter(int[] docs) {
      bits = new OpenBitSet();
      for(int i = 0; i < docs.length; i++){
    	  bits.set(docs[i]);
      }
      
    }

    public DocIdSet getDocIdSet(IndexReader reader) {
      return bits;
    }
  }

}
