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
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;


public class FuzzyTermOnShortTermsTest extends LuceneTestCase {
   private final static String FIELD = "field";
   
   @Test
   public void test() throws Exception {
      // proves rule that edit distance between the two terms
      // must be > smaller term for there to be a match
      Analyzer a = getAnalyzer();
      //these work
      countHits(a, new String[]{"abc"}, new FuzzyQuery(new Term(FIELD, "ab"), 1), 1);
      countHits(a, new String[]{"ab"}, new FuzzyQuery(new Term(FIELD, "abc"), 1), 1);

      countHits(a, new String[]{"abcde"}, new FuzzyQuery(new Term(FIELD, "abc"), 2), 1);
      countHits(a, new String[]{"abc"}, new FuzzyQuery(new Term(FIELD, "abcde"), 2), 1);

      // LUCENE-7439: these now work as well:
      
      countHits(a, new String[]{"ab"}, new FuzzyQuery(new Term(FIELD, "a"), 1), 1);
      countHits(a, new String[]{"a"}, new FuzzyQuery(new Term(FIELD, "ab"), 1), 1);
      
      countHits(a, new String[]{"abc"}, new FuzzyQuery(new Term(FIELD, "a"), 2), 1);
      countHits(a, new String[]{"a"}, new FuzzyQuery(new Term(FIELD, "abc"), 2), 1);

      countHits(a, new String[]{"abcd"}, new FuzzyQuery(new Term(FIELD, "ab"), 2), 1);
      countHits(a, new String[]{"ab"}, new FuzzyQuery(new Term(FIELD, "abcd"), 2), 1);
   }
   
   private void countHits(Analyzer analyzer, String[] docs, Query q, int expected) throws Exception {
      Directory d = getDirectory(analyzer, docs);
      IndexReader r = DirectoryReader.open(d);
      IndexSearcher s = new IndexSearcher(r);
      TotalHitCountCollector c = new TotalHitCountCollector();
      s.search(q,  c);
      assertEquals(q.toString(), expected, c.getTotalHits());
      r.close();
      d.close();
   }
   
   public static Analyzer getAnalyzer(){
      return new Analyzer() {
         @Override
         public TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
            return new TokenStreamComponents(tokenizer, tokenizer);
         }
      };
   }
   public static Directory getDirectory(Analyzer analyzer, String[] vals) throws IOException{
      Directory directory = newDirectory();
      RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
          newIndexWriterConfig(analyzer)
          .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000)).setMergePolicy(newLogMergePolicy()));

      for (String s : vals){
         Document d = new Document();
         d.add(newTextField(FIELD, s, Field.Store.YES));
         writer.addDocument(d);
            
      }
      writer.close();
      return directory;
   }
}
