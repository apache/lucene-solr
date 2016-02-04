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


import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestNGramPhraseQuery extends LuceneTestCase {

  private static IndexReader reader;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    writer.close();
    reader = DirectoryReader.open(directory);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }
  
  public void testRewrite() throws Exception {
    // bi-gram test ABC => AB/BC => AB/BC
    NGramPhraseQuery pq1 = new NGramPhraseQuery(2, new PhraseQuery("f", "AB", "BC"));
    
    Query q = pq1.rewrite(reader);
    assertSame(q.rewrite(reader), q);
    PhraseQuery rewritten1 = (PhraseQuery) q;
    assertArrayEquals(new Term[]{new Term("f", "AB"), new Term("f", "BC")}, rewritten1.getTerms());
    assertArrayEquals(new int[]{0, 1}, rewritten1.getPositions());

    // bi-gram test ABCD => AB/BC/CD => AB//CD
    NGramPhraseQuery pq2 = new NGramPhraseQuery(2, new PhraseQuery("f", "AB", "BC", "CD"));
    
    q = pq2.rewrite(reader);
    assertTrue(q instanceof PhraseQuery);
    assertNotSame(pq2, q);
    PhraseQuery rewritten2 = (PhraseQuery) q;
    assertArrayEquals(new Term[]{new Term("f", "AB"), new Term("f", "CD")}, rewritten2.getTerms());
    assertArrayEquals(new int[]{0, 2}, rewritten2.getPositions());

    // tri-gram test ABCDEFGH => ABC/BCD/CDE/DEF/EFG/FGH => ABC///DEF//FGH
    NGramPhraseQuery pq3 = new NGramPhraseQuery(3, new PhraseQuery("f", "ABC", "BCD", "CDE", "DEF", "EFG", "FGH"));
    
    q = pq3.rewrite(reader);
    assertTrue(q instanceof PhraseQuery);
    assertNotSame(pq3, q);
    PhraseQuery rewritten3 = (PhraseQuery) q;
    assertArrayEquals(new Term[]{new Term("f", "ABC"), new Term("f", "DEF"), new Term("f", "FGH")}, rewritten3.getTerms());
    assertArrayEquals(new int[]{0, 3, 5}, rewritten3.getPositions());
  }

}
