package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * This class tests PhrasePrefixQuery class.
 */
public class TestPhrasePrefixQuery extends LuceneTestCase {
  
  /**
     *
     */
  public void testPhrasePrefix() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    Document doc1 = new Document();
    Document doc2 = new Document();
    Document doc3 = new Document();
    Document doc4 = new Document();
    Document doc5 = new Document();
    doc1.add(newTextField("body", "blueberry pie", Field.Store.YES));
    doc2.add(newTextField("body", "blueberry strudel", Field.Store.YES));
    doc3.add(newTextField("body", "blueberry pizza", Field.Store.YES));
    doc4.add(newTextField("body", "blueberry chewing gum", Field.Store.YES));
    doc5.add(newTextField("body", "piccadilly circus", Field.Store.YES));
    writer.addDocument(doc1);
    writer.addDocument(doc2);
    writer.addDocument(doc3);
    writer.addDocument(doc4);
    writer.addDocument(doc5);
    IndexReader reader = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(reader);
    
    // PhrasePrefixQuery query1 = new PhrasePrefixQuery();
    MultiPhraseQuery query1 = new MultiPhraseQuery();
    // PhrasePrefixQuery query2 = new PhrasePrefixQuery();
    MultiPhraseQuery query2 = new MultiPhraseQuery();
    query1.add(new Term("body", "blueberry"));
    query2.add(new Term("body", "strawberry"));
    
    LinkedList<Term> termsWithPrefix = new LinkedList<>();
    
    // this TermEnum gives "piccadilly", "pie" and "pizza".
    String prefix = "pi";
    TermsEnum te = MultiFields.getFields(reader).terms("body").iterator();
    te.seekCeil(new BytesRef(prefix));
    do {
      String s = te.term().utf8ToString();
      if (s.startsWith(prefix)) {
        termsWithPrefix.add(new Term("body", s));
      } else {
        break;
      }
    } while (te.next() != null);
    
    query1.add(termsWithPrefix.toArray(new Term[0]));
    query2.add(termsWithPrefix.toArray(new Term[0]));
    
    ScoreDoc[] result;
    result = searcher.search(query1, 1000).scoreDocs;
    assertEquals(2, result.length);
    
    result = searcher.search(query2, 1000).scoreDocs;
    assertEquals(0, result.length);
    reader.close();
    indexStore.close();
  }
}
