package org.apache.lucene.search;

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
    reader = IndexReader.open(directory);
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
    PhraseQuery pq1 = new NGramPhraseQuery(2);
    pq1.add(new Term("f", "AB"));
    pq1.add(new Term("f", "BC"));
    
    Query q = pq1.rewrite(reader);
    assertTrue(q instanceof NGramPhraseQuery);
    assertSame(pq1, q);
    pq1 = (NGramPhraseQuery)q;
    assertArrayEquals(new Term[]{new Term("f", "AB"), new Term("f", "BC")}, pq1.getTerms());
    assertArrayEquals(new int[]{0, 1}, pq1.getPositions());

    // bi-gram test ABCD => AB/BC/CD => AB//CD
    PhraseQuery pq2 = new NGramPhraseQuery(2);
    pq2.add(new Term("f", "AB"));
    pq2.add(new Term("f", "BC"));
    pq2.add(new Term("f", "CD"));
    
    q = pq2.rewrite(reader);
    assertTrue(q instanceof PhraseQuery);
    assertNotSame(pq2, q);
    pq2 = (PhraseQuery)q;
    assertArrayEquals(new Term[]{new Term("f", "AB"), new Term("f", "CD")}, pq2.getTerms());
    assertArrayEquals(new int[]{0, 2}, pq2.getPositions());

    // tri-gram test ABCDEFGH => ABC/BCD/CDE/DEF/EFG/FGH => ABC///DEF//FGH
    PhraseQuery pq3 = new NGramPhraseQuery(3);
    pq3.add(new Term("f", "ABC"));
    pq3.add(new Term("f", "BCD"));
    pq3.add(new Term("f", "CDE"));
    pq3.add(new Term("f", "DEF"));
    pq3.add(new Term("f", "EFG"));
    pq3.add(new Term("f", "FGH"));
    
    q = pq3.rewrite(reader);
    assertTrue(q instanceof PhraseQuery);
    assertNotSame(pq3, q);
    pq3 = (PhraseQuery)q;
    assertArrayEquals(new Term[]{new Term("f", "ABC"), new Term("f", "DEF"), new Term("f", "FGH")}, pq3.getTerms());
    assertArrayEquals(new int[]{0, 3, 5}, pq3.getPositions());
  }

}
