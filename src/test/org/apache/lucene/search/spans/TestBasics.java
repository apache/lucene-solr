package org.apache.lucene.search.spans;

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

import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

import org.apache.lucene.util.English;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.search.*;

/**
 * Tests basic search capabilities.
 *
 * <p>Uses a collection of 1000 documents, each the english rendition of their
 * document number.  For example, the document numbered 333 has text "three
 * hundred thirty three".
 *
 * <p>Tests are each a single query, and its hits are checked to ensure that
 * all and only the correct documents are returned, thus providing end-to-end
 * testing of the indexing and search code.
 *
 */
public class TestBasics extends LuceneTestCase {
  private IndexSearcher searcher;

  public void setUp() throws Exception {
    super.setUp();
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true, 
                                         IndexWriter.MaxFieldLength.LIMITED);
    //writer.infoStream = System.out;
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(new Field("field", English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }

    writer.close();

    searcher = new IndexSearcher(directory);
  }
  
  public void testTerm() throws Exception {
    Query query = new TermQuery(new Term("field", "seventy"));
    checkHits(query, new int[]
      {70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 170, 171, 172, 173, 174, 175,
       176, 177, 178, 179, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279,
       370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 470, 471, 472, 473,
       474, 475, 476, 477, 478, 479, 570, 571, 572, 573, 574, 575, 576, 577,
       578, 579, 670, 671, 672, 673, 674, 675, 676, 677, 678, 679, 770, 771,
       772, 773, 774, 775, 776, 777, 778, 779, 870, 871, 872, 873, 874, 875,
       876, 877, 878, 879, 970, 971, 972, 973, 974, 975, 976, 977, 978, 979});
    }

  public void testTerm2() throws Exception {
    Query query = new TermQuery(new Term("field", "seventish"));
    checkHits(query, new int[] {});
  }

  public void testPhrase() throws Exception {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "seventy"));
    query.add(new Term("field", "seven"));
    checkHits(query, new int[]
      {77, 177, 277, 377, 477, 577, 677, 777, 877, 977});
  }

  public void testPhrase2() throws Exception {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "seventish"));
    query.add(new Term("field", "sevenon"));
    checkHits(query, new int[] {});
  }

  public void testBoolean() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("field", "seventy")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("field", "seven")), BooleanClause.Occur.MUST);
    checkHits(query, new int[]
      {77, 777, 177, 277, 377, 477, 577, 677, 770, 771, 772, 773, 774, 775,
       776, 778, 779, 877, 977});
  }

  public void testBoolean2() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("field", "sevento")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("field", "sevenly")), BooleanClause.Occur.MUST);
    checkHits(query, new int[] {});
  }

  public void testSpanNearExact() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "seventy"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "seven"));
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            0, true);
    checkHits(query, new int[]
      {77, 177, 277, 377, 477, 577, 677, 777, 877, 977});

    assertTrue(searcher.explain(query, 77).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 977).getValue() > 0.0f);

    QueryUtils.check(term1);
    QueryUtils.check(term2);
    QueryUtils.checkUnequal(term1,term2);
  }

  public void testSpanNearUnordered() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "nine"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "six"));
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            4, false);

    checkHits(query, new int[]
      {609, 629, 639, 649, 659, 669, 679, 689, 699,
       906, 926, 936, 946, 956, 966, 976, 986, 996});
  }

  public void testSpanNearOrdered() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "nine"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "six"));
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            4, true);
    checkHits(query, new int[]
      {906, 926, 936, 946, 956, 966, 976, 986, 996});
  }

  public void testSpanNot() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "eight"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "one"));
    SpanNearQuery near = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                           4, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));
    SpanNotQuery query = new SpanNotQuery(near, term3);

    checkHits(query, new int[]
      {801, 821, 831, 851, 861, 871, 881, 891});

    assertTrue(searcher.explain(query, 801).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue() > 0.0f);
  }
  
  public void testSpanWithMultipleNotSingle() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "eight"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "one"));
    SpanNearQuery near = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                           4, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));

    SpanOrQuery or = new SpanOrQuery(new SpanQuery[] {term3});

    SpanNotQuery query = new SpanNotQuery(near, or);

    checkHits(query, new int[]
      {801, 821, 831, 851, 861, 871, 881, 891});

    assertTrue(searcher.explain(query, 801).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue() > 0.0f);
  }

  public void testSpanWithMultipleNotMany() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "eight"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "one"));
    SpanNearQuery near = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                           4, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));
    SpanTermQuery term4 = new SpanTermQuery(new Term("field", "sixty"));
    SpanTermQuery term5 = new SpanTermQuery(new Term("field", "eighty"));

    SpanOrQuery or = new SpanOrQuery(new SpanQuery[] {term3, term4, term5});

    SpanNotQuery query = new SpanNotQuery(near, or);

    checkHits(query, new int[]
      {801, 821, 831, 851, 871, 891});

    assertTrue(searcher.explain(query, 801).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue() > 0.0f);
  }
    
  public void testNpeInSpanNearWithSpanNot() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "eight"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "one"));
    SpanNearQuery near = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                           4, true);
    SpanTermQuery hun = new SpanTermQuery(new Term("field", "hundred"));
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));
    SpanNearQuery exclude = new SpanNearQuery(new SpanQuery[] {hun, term3},
                                              1, true);
    
    SpanNotQuery query = new SpanNotQuery(near, exclude);

    checkHits(query, new int[]
      {801, 821, 831, 851, 861, 871, 881, 891});

    assertTrue(searcher.explain(query, 801).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue() > 0.0f);
  }

  
  public void testNpeInSpanNearInSpanFirstInSpanNot() throws Exception {
    int n = 5;
    SpanTermQuery hun = new SpanTermQuery(new Term("field", "hundred"));
    SpanTermQuery term40 = new SpanTermQuery(new Term("field", "forty"));
    SpanTermQuery term40c = (SpanTermQuery)term40.clone();

    SpanFirstQuery include = new SpanFirstQuery(term40, n);
    SpanNearQuery near = new SpanNearQuery(new SpanQuery[]{hun, term40c},
                                           n-1, true);
    SpanFirstQuery exclude = new SpanFirstQuery(near, n-1);
    SpanNotQuery q = new SpanNotQuery(include, exclude);
    
    checkHits(q, new int[]{40,41,42,43,44,45,46,47,48,49});
    
  }
  
  public void testSpanFirst() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "five"));
    SpanFirstQuery query = new SpanFirstQuery(term1, 1);

    checkHits(query, new int[]
      {5, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513,
       514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527,
       528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541,
       542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555,
       556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569,
       570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583,
       584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597,
       598, 599});

    assertTrue(searcher.explain(query, 5).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 599).getValue() > 0.0f);

  }

  public void testSpanOr() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "thirty"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "three"));
    SpanNearQuery near1 = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            0, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));
    SpanTermQuery term4 = new SpanTermQuery(new Term("field", "seven"));
    SpanNearQuery near2 = new SpanNearQuery(new SpanQuery[] {term3, term4},
                                            0, true);

    SpanOrQuery query = new SpanOrQuery(new SpanQuery[] {near1, near2});

    checkHits(query, new int[]
      {33, 47, 133, 147, 233, 247, 333, 347, 433, 447, 533, 547, 633, 647, 733,
       747, 833, 847, 933, 947});

    assertTrue(searcher.explain(query, 33).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 947).getValue() > 0.0f);
  }

  public void testSpanExactNested() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "three"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "hundred"));
    SpanNearQuery near1 = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            0, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "thirty"));
    SpanTermQuery term4 = new SpanTermQuery(new Term("field", "three"));
    SpanNearQuery near2 = new SpanNearQuery(new SpanQuery[] {term3, term4},
                                            0, true);

    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {near1, near2},
                                            0, true);

    checkHits(query, new int[] {333});

    assertTrue(searcher.explain(query, 333).getValue() > 0.0f);
  }

  public void testSpanNearOr() throws Exception {

    SpanTermQuery t1 = new SpanTermQuery(new Term("field","six"));
    SpanTermQuery t3 = new SpanTermQuery(new Term("field","seven"));
    
    SpanTermQuery t5 = new SpanTermQuery(new Term("field","seven"));
    SpanTermQuery t6 = new SpanTermQuery(new Term("field","six"));

    SpanOrQuery to1 = new SpanOrQuery(new SpanQuery[] {t1, t3});
    SpanOrQuery to2 = new SpanOrQuery(new SpanQuery[] {t5, t6});
    
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {to1, to2},
                                            10, true);

    checkHits(query, new int[]
      {606, 607, 626, 627, 636, 637, 646, 647, 
       656, 657, 666, 667, 676, 677, 686, 687, 696, 697,
       706, 707, 726, 727, 736, 737, 746, 747, 
       756, 757, 766, 767, 776, 777, 786, 787, 796, 797});
  }

  public void testSpanComplex1() throws Exception {
      
    SpanTermQuery t1 = new SpanTermQuery(new Term("field","six"));
    SpanTermQuery t2 = new SpanTermQuery(new Term("field","hundred"));
    SpanNearQuery tt1 = new SpanNearQuery(new SpanQuery[] {t1, t2}, 0,true);

    SpanTermQuery t3 = new SpanTermQuery(new Term("field","seven"));
    SpanTermQuery t4 = new SpanTermQuery(new Term("field","hundred"));
    SpanNearQuery tt2 = new SpanNearQuery(new SpanQuery[] {t3, t4}, 0,true);
    
    SpanTermQuery t5 = new SpanTermQuery(new Term("field","seven"));
    SpanTermQuery t6 = new SpanTermQuery(new Term("field","six"));

    SpanOrQuery to1 = new SpanOrQuery(new SpanQuery[] {tt1, tt2});
    SpanOrQuery to2 = new SpanOrQuery(new SpanQuery[] {t5, t6});
    
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {to1, to2},
                                            100, true);
    
    checkHits(query, new int[]
      {606, 607, 626, 627, 636, 637, 646, 647, 
       656, 657, 666, 667, 676, 677, 686, 687, 696, 697,
       706, 707, 726, 727, 736, 737, 746, 747, 
       756, 757, 766, 767, 776, 777, 786, 787, 796, 797});
  }


  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(query, "field", searcher, results);
  }
}
