package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001, 2004 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import junit.framework.TestCase;

import java.io.IOException;

import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.util.English;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.search.spans.*;

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
 * @author Doug Cutting
 */
public class TestBasics extends TestCase {
  private IndexSearcher searcher;

  public void setUp() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer
      = new IndexWriter(directory, new SimpleAnalyzer(), true);
    //writer.infoStream = System.out;
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(Field.Text("field", English.intToEnglish(i)));
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
    query.add(new TermQuery(new Term("field", "seventy")), true, false);
    query.add(new TermQuery(new Term("field", "seven")), true, false);
    checkHits(query, new int[]
      {77, 777, 177, 277, 377, 477, 577, 677, 770, 771, 772, 773, 774, 775,
       776, 778, 779, 877, 977});
  }

  public void testBoolean2() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("field", "sevento")), true, false);
    query.add(new TermQuery(new Term("field", "sevenly")), true, false);
    checkHits(query, new int[] {});
  }

  public void testSpanNearExact() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "seventy"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "seven"));
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            0, true);
    checkHits(query, new int[]
      {77, 177, 277, 377, 477, 577, 677, 777, 877, 977});

    //System.out.println(searcher.explain(query, 77));
    //System.out.println(searcher.explain(query, 977));
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

    //System.out.println(searcher.explain(query, 801));
    //System.out.println(searcher.explain(query, 891));
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

    //System.out.println(searcher.explain(query, 5));
    //System.out.println(searcher.explain(query, 599));

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

    //System.out.println(searcher.explain(query, 33));
    //System.out.println(searcher.explain(query, 947));
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

    //System.out.println(searcher.explain(query, 333));
  }

  private void checkHits(Query query, int[] results) throws IOException {
    Hits hits = searcher.search(query);

    Set correct = new TreeSet();
    for (int i = 0; i < results.length; i++) {
      correct.add(new Integer(results[i]));
    }

    Set actual = new TreeSet();
    for (int i = 0; i < hits.length(); i++) {
      actual.add(new Integer(hits.id(i)));
    }

    assertEquals(query.toString("field"), correct, actual);
  }

  private void printHits(Query query) throws IOException {
    Hits hits = searcher.search(query);
    System.out.print("new int[] {");
    for (int i = 0; i < hits.length(); i++) {
      System.out.print(hits.id(i));
      if (i != hits.length()-1)
        System.out.print(", ");
    }
    System.out.println("}");
  }

}
