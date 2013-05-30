package org.apache.lucene.search.spans;

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
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
  private static IndexSearcher searcher;
  private static IndexReader reader;
  private static Directory directory;

  static final class SimplePayloadFilter extends TokenFilter {
    int pos;
    final PayloadAttribute payloadAttr;
    final CharTermAttribute termAttr;

    public SimplePayloadFilter(TokenStream input) {
      super(input);
      pos = 0;
      payloadAttr = input.addAttribute(PayloadAttribute.class);
      termAttr = input.addAttribute(CharTermAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        payloadAttr.setPayload(new BytesRef(("pos: " + pos).getBytes("UTF-8")));
        pos++;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      pos = 0;
    }
  }
  
  static Analyzer simplePayloadAnalyzer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    simplePayloadAnalyzer = new Analyzer() {
        @Override
        public TokenStreamComponents createComponents(String fieldName, Reader reader) {
          Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
          return new TokenStreamComponents(tokenizer, new SimplePayloadFilter(tokenizer));
        }
    };
  
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, simplePayloadAnalyzer)
                                                     .setMaxBufferedDocs(_TestUtil.nextInt(random(), 100, 1000)).setMergePolicy(newLogMergePolicy()));
    //writer.infoStream = System.out;
    for (int i = 0; i < 2000; i++) {
      Document doc = new Document();
      doc.add(newTextField("field", English.intToEnglish(i), Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    searcher = null;
    reader = null;
    directory = null;
    simplePayloadAnalyzer = null;
  }

  @Test
  public void testTerm() throws Exception {
    Query query = new TermQuery(new Term("field", "seventy"));
    checkHits(query, new int[]
      {70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 170, 171, 172, 173, 174, 175,
              176, 177, 178, 179, 270, 271, 272, 273, 274, 275, 276, 277, 278,
              279, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 470, 471,
              472, 473, 474, 475, 476, 477, 478, 479, 570, 571, 572, 573, 574,
              575, 576, 577, 578, 579, 670, 671, 672, 673, 674, 675, 676, 677,
              678, 679, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 870,
              871, 872, 873, 874, 875, 876, 877, 878, 879, 970, 971, 972, 973,
              974, 975, 976, 977, 978, 979, 1070, 1071, 1072, 1073, 1074, 1075,
              1076, 1077, 1078, 1079, 1170, 1171, 1172, 1173, 1174, 1175, 1176,
              1177, 1178, 1179, 1270, 1271, 1272, 1273, 1274, 1275, 1276, 1277,
              1278, 1279, 1370, 1371, 1372, 1373, 1374, 1375, 1376, 1377, 1378,
              1379, 1470, 1471, 1472, 1473, 1474, 1475, 1476, 1477, 1478, 1479,
              1570, 1571, 1572, 1573, 1574, 1575, 1576, 1577, 1578, 1579, 1670,
              1671, 1672, 1673, 1674, 1675, 1676, 1677, 1678, 1679, 1770, 1771,
              1772, 1773, 1774, 1775, 1776, 1777, 1778, 1779, 1870, 1871, 1872,
              1873, 1874, 1875, 1876, 1877,
              1878, 1879, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978,
              1979});
    }

  @Test
  public void testTerm2() throws Exception {
    Query query = new TermQuery(new Term("field", "seventish"));
    checkHits(query, new int[] {});
  }

  @Test
  public void testPhrase() throws Exception {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "seventy"));
    query.add(new Term("field", "seven"));
    checkHits(query, new int[]
      {77, 177, 277, 377, 477, 577, 677, 777, 877,
              977, 1077, 1177, 1277, 1377, 1477, 1577, 1677, 1777, 1877, 1977});
  }

  @Test
  public void testPhrase2() throws Exception {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "seventish"));
    query.add(new Term("field", "sevenon"));
    checkHits(query, new int[] {});
  }

  @Test
  public void testBoolean() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("field", "seventy")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("field", "seven")), BooleanClause.Occur.MUST);
    checkHits(query, new int[]
      {77, 177, 277, 377, 477, 577, 677, 770, 771, 772, 773, 774, 775, 776, 777,
              778, 779, 877, 977, 1077, 1177, 1277, 1377, 1477, 1577, 1677,
              1770, 1771, 1772, 1773, 1774, 1775, 1776, 1777, 1778, 1779, 1877,
              1977});
  }

  @Test
  public void testBoolean2() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("field", "sevento")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("field", "sevenly")), BooleanClause.Occur.MUST);
    checkHits(query, new int[] {});
  }

  @Test
  public void testSpanNearExact() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "seventy"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "seven"));
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            0, true);
    checkHits(query, new int[]
      {77, 177, 277, 377, 477, 577, 677, 777, 877, 977, 1077, 1177, 1277, 1377, 1477, 1577, 1677, 1777, 1877, 1977});

    assertTrue(searcher.explain(query, 77).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 977).getValue() > 0.0f);

    QueryUtils.check(term1);
    QueryUtils.check(term2);
    QueryUtils.checkUnequal(term1,term2);
  }
  
  public void testSpanTermQuery() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "seventy"));
    checkHits(term1, new int[]
                             { 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 170,
        171, 172, 173, 174, 175, 176, 177, 178, 179, 270, 271, 272, 273, 274,
        275, 276, 277, 278, 279, 370, 371, 372, 373, 374, 375, 376, 377, 378,
        379, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 570, 571, 572,
        573, 574, 575, 576, 577, 578, 579, 670, 671, 672, 673, 674, 675, 676,
        677, 678, 679, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 870,
        871, 872, 873, 874, 875, 876, 877, 878, 879, 970, 971, 972, 973, 974,
        975, 976, 977, 978, 979, 1070, 1071, 1072, 1073, 1074, 1075, 1076,
        1077, 1078, 1079, 1170, 1270, 1370, 1470, 1570, 1670, 1770, 1870, 1970,
        1171, 1172, 1173, 1174, 1175, 1176, 1177, 1178, 1179, 1271, 1272, 1273,
        1274, 1275, 1276, 1277, 1278, 1279, 1371, 1372, 1373, 1374, 1375, 1376,
        1377, 1378, 1379, 1471, 1472, 1473, 1474, 1475, 1476, 1477, 1478, 1479,
        1571, 1572, 1573, 1574, 1575, 1576, 1577, 1578, 1579, 1671, 1672, 1673,
        1674, 1675, 1676, 1677, 1678, 1679, 1771, 1772, 1773, 1774, 1775, 1776,
        1777, 1778, 1779, 1871, 1872, 1873, 1874, 1875, 1876, 1877, 1878, 1879,
        1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979 });
  }

  @Test
  public void testSpanNearUnordered() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "nine"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "six"));
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            4, false);

    checkHits(query, new int[]
      {609, 629, 639, 649, 659, 669, 679, 689, 699, 906, 926, 936, 946, 956,
              966, 976, 986, 996, 1609, 1629, 1639, 1649, 1659, 1669,
              1679, 1689, 1699, 1906, 1926, 1936, 1946, 1956, 1966, 1976, 1986,
              1996});
  }

  @Test
  public void testSpanNearOrdered() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "nine"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "six"));
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            4, true);
    checkHits(query, new int[]
      {906, 926, 936, 946, 956, 966, 976, 986, 996, 1906, 1926, 1936, 1946, 1956, 1966, 1976, 1986, 1996});
  }

  @Test
  public void testSpanNot() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "eight"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "one"));
    SpanNearQuery near = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                           4, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));
    SpanNotQuery query = new SpanNotQuery(near, term3);

    checkHits(query, new int[]
      {801, 821, 831, 851, 861, 871, 881, 891, 1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});

    assertTrue(searcher.explain(query, 801).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue() > 0.0f);
  }
  
  @Test
  public void testSpanWithMultipleNotSingle() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "eight"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "one"));
    SpanNearQuery near = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                           4, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));

    SpanOrQuery or = new SpanOrQuery(term3);

    SpanNotQuery query = new SpanNotQuery(near, or);

    checkHits(query, new int[]
      {801, 821, 831, 851, 861, 871, 881, 891,
              1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});

    assertTrue(searcher.explain(query, 801).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue() > 0.0f);
  }

  @Test
  public void testSpanWithMultipleNotMany() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "eight"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "one"));
    SpanNearQuery near = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                           4, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));
    SpanTermQuery term4 = new SpanTermQuery(new Term("field", "sixty"));
    SpanTermQuery term5 = new SpanTermQuery(new Term("field", "eighty"));

    SpanOrQuery or = new SpanOrQuery(term3, term4, term5);

    SpanNotQuery query = new SpanNotQuery(near, or);

    checkHits(query, new int[]
      {801, 821, 831, 851, 871, 891, 1801, 1821, 1831, 1851, 1871, 1891});

    assertTrue(searcher.explain(query, 801).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue() > 0.0f);
  }

  @Test
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
      {801, 821, 831, 851, 861, 871, 881, 891,
              1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});

    assertTrue(searcher.explain(query, 801).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue() > 0.0f);
  }

  @Test
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
    
    checkHits(q, new int[]{40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048,
            1049, 1140, 1141, 1142, 1143, 1144, 1145, 1146, 1147, 1148, 1149, 1240, 1241, 1242, 1243, 1244,
            1245, 1246, 1247, 1248, 1249, 1340, 1341, 1342, 1343, 1344, 1345, 1346, 1347, 1348, 1349, 1440, 1441, 1442,
            1443, 1444, 1445, 1446, 1447, 1448, 1449, 1540, 1541, 1542, 1543, 1544, 1545, 1546, 1547, 1548, 1549, 1640,
            1641, 1642, 1643, 1644, 1645, 1646, 1647,
            1648, 1649, 1740, 1741, 1742, 1743, 1744, 1745, 1746, 1747, 1748, 1749, 1840, 1841, 1842, 1843, 1844, 1845, 1846,
            1847, 1848, 1849, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949});
  }
  
  @Test
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

  @Test
  public void testSpanPositionRange() throws Exception {
    SpanPositionRangeQuery query;
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "five"));
    query = new SpanPositionRangeQuery(term1, 1, 2);
    checkHits(query, new int[]
      {25,35, 45, 55, 65, 75, 85, 95});
    assertTrue(searcher.explain(query, 25).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 95).getValue() > 0.0f);

    query = new SpanPositionRangeQuery(term1, 0, 1);
    checkHits(query, new int[]
      {5, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512,
              513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525,
              526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538,
              539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551,
              552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564,
              565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577,
              578, 579, 580, 581, 582, 583, 584,
              585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597,
              598, 599});

    query = new SpanPositionRangeQuery(term1, 6, 7);
    checkHits(query, new int[]{});
  }

  @Test
  public void testSpanPayloadCheck() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "five"));
    BytesRef pay = new BytesRef(("pos: " + 5).getBytes("UTF-8"));
    SpanQuery query = new SpanPayloadCheckQuery(term1, Collections.singletonList(pay.bytes));
    checkHits(query, new int[]
      {1125, 1135, 1145, 1155, 1165, 1175, 1185, 1195, 1225, 1235, 1245, 1255, 1265, 1275, 1285, 1295, 1325, 1335, 1345, 1355, 1365, 1375, 1385, 1395, 1425, 1435, 1445, 1455, 1465, 1475, 1485, 1495, 1525, 1535, 1545, 1555, 1565, 1575, 1585, 1595, 1625, 1635, 1645, 1655, 1665, 1675, 1685, 1695, 1725, 1735, 1745, 1755, 1765, 1775, 1785, 1795, 1825, 1835, 1845, 1855, 1865, 1875, 1885, 1895, 1925, 1935, 1945, 1955, 1965, 1975, 1985, 1995});
    assertTrue(searcher.explain(query, 1125).getValue() > 0.0f);

    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "hundred"));
    SpanNearQuery snq;
    SpanQuery[] clauses;
    List<byte[]> list;
    BytesRef pay2;
    clauses = new SpanQuery[2];
    clauses[0] = term1;
    clauses[1] = term2;
    snq = new SpanNearQuery(clauses, 0, true);
    pay = new BytesRef(("pos: " + 0).getBytes("UTF-8"));
    pay2 = new BytesRef(("pos: " + 1).getBytes("UTF-8"));
    list = new ArrayList<byte[]>();
    list.add(pay.bytes);
    list.add(pay2.bytes);
    query = new SpanNearPayloadCheckQuery(snq, list);
    checkHits(query, new int[]
      {500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599});
    clauses = new SpanQuery[3];
    clauses[0] = term1;
    clauses[1] = term2;
    clauses[2] = new SpanTermQuery(new Term("field", "five"));
    snq = new SpanNearQuery(clauses, 0, true);
    pay = new BytesRef(("pos: " + 0).getBytes("UTF-8"));
    pay2 = new BytesRef(("pos: " + 1).getBytes("UTF-8"));
    BytesRef pay3 = new BytesRef(("pos: " + 2).getBytes("UTF-8"));
    list = new ArrayList<byte[]>();
    list.add(pay.bytes);
    list.add(pay2.bytes);
    list.add(pay3.bytes);
    query = new SpanNearPayloadCheckQuery(snq, list);
    checkHits(query, new int[]
      {505});
  }

  public void testComplexSpanChecks() throws Exception {
    SpanTermQuery one = new SpanTermQuery(new Term("field", "one"));
    SpanTermQuery thous = new SpanTermQuery(new Term("field", "thousand"));
    //should be one position in between
    SpanTermQuery hundred = new SpanTermQuery(new Term("field", "hundred"));
    SpanTermQuery three = new SpanTermQuery(new Term("field", "three"));

    SpanNearQuery oneThous = new SpanNearQuery(new SpanQuery[]{one, thous}, 0, true);
    SpanNearQuery hundredThree = new SpanNearQuery(new SpanQuery[]{hundred, three}, 0, true);
    SpanNearQuery oneThousHunThree = new SpanNearQuery(new SpanQuery[]{oneThous, hundredThree}, 1, true);
    SpanQuery query;
    //this one's too small
    query = new SpanPositionRangeQuery(oneThousHunThree, 1, 2);
    checkHits(query, new int[]{});
    //this one's just right
    query = new SpanPositionRangeQuery(oneThousHunThree, 0, 6);
    checkHits(query, new int[]{1103, 1203,1303,1403,1503,1603,1703,1803,1903});

    Collection<byte[]> payloads = new ArrayList<byte[]>();
    BytesRef pay = new BytesRef(("pos: " + 0).getBytes("UTF-8"));
    BytesRef pay2 = new BytesRef(("pos: " + 1).getBytes("UTF-8"));
    BytesRef pay3 = new BytesRef(("pos: " + 3).getBytes("UTF-8"));
    BytesRef pay4 = new BytesRef(("pos: " + 4).getBytes("UTF-8"));
    payloads.add(pay.bytes);
    payloads.add(pay2.bytes);
    payloads.add(pay3.bytes);
    payloads.add(pay4.bytes);
    query = new SpanNearPayloadCheckQuery(oneThousHunThree, payloads);
    checkHits(query, new int[]{1103, 1203,1303,1403,1503,1603,1703,1803,1903});

  }


  @Test
  public void testSpanOr() throws Exception {
    SpanTermQuery term1 = new SpanTermQuery(new Term("field", "thirty"));
    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "three"));
    SpanNearQuery near1 = new SpanNearQuery(new SpanQuery[] {term1, term2},
                                            0, true);
    SpanTermQuery term3 = new SpanTermQuery(new Term("field", "forty"));
    SpanTermQuery term4 = new SpanTermQuery(new Term("field", "seven"));
    SpanNearQuery near2 = new SpanNearQuery(new SpanQuery[] {term3, term4},
                                            0, true);

    SpanOrQuery query = new SpanOrQuery(near1, near2);

    checkHits(query, new int[]
      {33, 47, 133, 147, 233, 247, 333, 347, 433, 447, 533, 547, 633, 647, 733,
              747, 833, 847, 933, 947, 1033, 1047, 1133, 1147, 1233, 1247, 1333,
              1347, 1433, 1447, 1533, 1547, 1633, 1647, 1733, 1747, 1833, 1847, 1933, 1947});

    assertTrue(searcher.explain(query, 33).getValue() > 0.0f);
    assertTrue(searcher.explain(query, 947).getValue() > 0.0f);
  }

  @Test
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

    checkHits(query, new int[] {333, 1333});

    assertTrue(searcher.explain(query, 333).getValue() > 0.0f);
  }

  @Test
  public void testSpanNearOr() throws Exception {

    SpanTermQuery t1 = new SpanTermQuery(new Term("field","six"));
    SpanTermQuery t3 = new SpanTermQuery(new Term("field","seven"));
    
    SpanTermQuery t5 = new SpanTermQuery(new Term("field","seven"));
    SpanTermQuery t6 = new SpanTermQuery(new Term("field","six"));

    SpanOrQuery to1 = new SpanOrQuery(t1, t3);
    SpanOrQuery to2 = new SpanOrQuery(t5, t6);
    
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {to1, to2},
                                            10, true);

    checkHits(query, new int[]
      {606, 607, 626, 627, 636, 637, 646, 647, 656, 657, 666, 667, 676, 677,
              686, 687, 696, 697, 706, 707, 726, 727, 736, 737, 746, 747, 756,
              757, 766, 767, 776, 777, 786, 787, 796, 797, 1606, 1607, 1626,
              1627, 1636, 1637, 1646, 1647, 1656, 1657, 1666, 1667, 1676, 1677,
              1686, 1687, 1696, 1697, 1706, 1707, 1726, 1727, 1736, 1737,
              1746, 1747, 1756, 1757, 1766, 1767, 1776, 1777, 1786, 1787, 1796,
              1797});
  }

  @Test
  public void testSpanComplex1() throws Exception {
      
    SpanTermQuery t1 = new SpanTermQuery(new Term("field","six"));
    SpanTermQuery t2 = new SpanTermQuery(new Term("field","hundred"));
    SpanNearQuery tt1 = new SpanNearQuery(new SpanQuery[] {t1, t2}, 0,true);

    SpanTermQuery t3 = new SpanTermQuery(new Term("field","seven"));
    SpanTermQuery t4 = new SpanTermQuery(new Term("field","hundred"));
    SpanNearQuery tt2 = new SpanNearQuery(new SpanQuery[] {t3, t4}, 0,true);
    
    SpanTermQuery t5 = new SpanTermQuery(new Term("field","seven"));
    SpanTermQuery t6 = new SpanTermQuery(new Term("field","six"));

    SpanOrQuery to1 = new SpanOrQuery(tt1, tt2);
    SpanOrQuery to2 = new SpanOrQuery(t5, t6);
    
    SpanNearQuery query = new SpanNearQuery(new SpanQuery[] {to1, to2},
                                            100, true);
    
    checkHits(query, new int[]
      {606, 607, 626, 627, 636, 637, 646, 647, 656, 657, 666, 667, 676, 677, 686, 687, 696,
              697, 706, 707, 726, 727, 736, 737, 746, 747, 756, 757,
              766, 767, 776, 777, 786, 787, 796, 797, 1606, 1607, 1626, 1627, 1636, 1637, 1646,
              1647, 1656, 1657,
              1666, 1667, 1676, 1677, 1686, 1687, 1696, 1697, 1706, 1707, 1726, 1727, 1736, 1737,
              1746, 1747, 1756, 1757, 1766, 1767, 1776, 1777, 1786, 1787, 1796, 1797});
  }
  
  @Test
  public void testSpansSkipTo() throws Exception {
    SpanTermQuery t1 = new SpanTermQuery(new Term("field", "seventy"));
    SpanTermQuery t2 = new SpanTermQuery(new Term("field", "seventy"));
    Spans s1 = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), t1);
    Spans s2 = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), t2);

    assertTrue(s1.next());
    assertTrue(s2.next());

    boolean hasMore = true;

    do {
      hasMore = skipToAccoringToJavaDocs(s1, s1.doc() + 1);
      assertEquals(hasMore, s2.skipTo(s2.doc() + 1));
      assertEquals(s1.doc(), s2.doc());
    } while (hasMore);
  }

  /** Skips to the first match beyond the current, whose document number is
   * greater than or equal to <i>target</i>. <p>Returns true iff there is such
   * a match.  <p>Behaves as if written: <pre>
   *   boolean skipTo(int target) {
   *     do {
   *       if (!next())
   *       return false;
   *     } while (target > doc());
   *     return true;
   *   }
   * </pre>
   */
  private boolean skipToAccoringToJavaDocs(Spans s, int target)
      throws Exception {
    do {
      if (!s.next())
        return false;
    } while (target > s.doc());
    return true;

  }

  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, "field", searcher, results);
  }
}
