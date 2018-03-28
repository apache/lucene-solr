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
package org.apache.lucene.search.spans;


import java.io.IOException;

import org.apache.lucene.analysis.*;
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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.lucene.search.spans.SpanTestUtil.*;

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

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true))
            .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000)).setMergePolicy(newLogMergePolicy()));
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
  }

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

  public void testTerm2() throws Exception {
    Query query = new TermQuery(new Term("field", "seventish"));
    checkHits(query, new int[] {});
  }

  public void testPhrase() throws Exception {
    PhraseQuery query = new PhraseQuery("field", "seventy", "seven");
    checkHits(query, new int[]
      {77, 177, 277, 377, 477, 577, 677, 777, 877,
              977, 1077, 1177, 1277, 1377, 1477, 1577, 1677, 1777, 1877, 1977});
  }

  public void testPhrase2() throws Exception {
    PhraseQuery query = new PhraseQuery("field", "seventish", "sevenon");
    checkHits(query, new int[] {});
  }

  public void testBoolean() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("field", "seventy")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("field", "seven")), BooleanClause.Occur.MUST);
    checkHits(query.build(), new int[]
      {77, 177, 277, 377, 477, 577, 677, 770, 771, 772, 773, 774, 775, 776, 777,
              778, 779, 877, 977, 1077, 1177, 1277, 1377, 1477, 1577, 1677,
              1770, 1771, 1772, 1773, 1774, 1775, 1776, 1777, 1778, 1779, 1877,
              1977});
  }

  public void testBoolean2() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("field", "sevento")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("field", "sevenly")), BooleanClause.Occur.MUST);
    checkHits(query.build(), new int[] {});
  }

  public void testSpanNearExact() throws Exception {
    SpanQuery query = spanNearOrderedQuery("field", 0, "seventy", "seven");
    
    checkHits(query, new int[]
      {77, 177, 277, 377, 477, 577, 677, 777, 877, 977, 1077, 1177, 1277, 1377, 1477, 1577, 1677, 1777, 1877, 1977});

    assertTrue(searcher.explain(query, 77).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 977).getValue().doubleValue() > 0.0f);
  }
  
  public void testSpanTermQuery() throws Exception {
    checkHits(spanTermQuery("field", "seventy"), 
              new int[] { 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 170,
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

  public void testSpanNearUnordered() throws Exception {
    checkHits(spanNearUnorderedQuery("field", 4, "nine", "six"), 
              new int[] { 609, 629, 639, 649, 659, 669, 679, 689, 699, 906, 926, 936, 946, 956,
              966, 976, 986, 996, 1609, 1629, 1639, 1649, 1659, 1669,
              1679, 1689, 1699, 1906, 1926, 1936, 1946, 1956, 1966, 1976, 1986,
              1996});
  }

  public void testSpanNearOrdered() throws Exception {
    checkHits(spanNearOrderedQuery("field", 4, "nine", "six"), 
              new int[] { 906, 926, 936, 946, 956, 966, 976, 986, 996, 1906, 1926, 
                          1936, 1946, 1956, 1966, 1976, 1986, 1996});
  }

  public void testSpanNot() throws Exception {
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "one");
    SpanQuery query = spanNotQuery(near, spanTermQuery("field", "forty"));

    checkHits(query, new int[]
      {801, 821, 831, 851, 861, 871, 881, 891, 1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});

    assertTrue(searcher.explain(query, 801).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue().doubleValue() > 0.0f);
  }

  public void testSpanNotNoOverflowOnLargeSpans() throws Exception {
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "one");
    SpanQuery query = spanNotQuery(near, spanTermQuery("field", "forty"), Integer.MAX_VALUE, Integer.MAX_VALUE);

    checkHits(query, new int[]
        {801, 821, 831, 851, 861, 871, 881, 891, 1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});
  }

  public void testSpanWithMultipleNotSingle() throws Exception {
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "one");
    SpanQuery or = spanOrQuery("field", "forty");
    SpanQuery query = spanNotQuery(near, or);

    checkHits(query, new int[]
      {801, 821, 831, 851, 861, 871, 881, 891,
              1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});

    assertTrue(searcher.explain(query, 801).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue().doubleValue() > 0.0f);
  }

  public void testSpanWithMultipleNotMany() throws Exception {
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "one");
    SpanQuery or = spanOrQuery("field", "forty", "sixty", "eighty");
    SpanQuery query = spanNotQuery(near, or);

    checkHits(query, new int[]
      {801, 821, 831, 851, 871, 891, 1801, 1821, 1831, 1851, 1871, 1891});

    assertTrue(searcher.explain(query, 801).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue().doubleValue() > 0.0f);
  }

  public void testNpeInSpanNearWithSpanNot() throws Exception {
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "one");
    SpanQuery exclude = spanNearOrderedQuery("field", 1, "hundred", "forty");
    SpanQuery query = spanNotQuery(near, exclude);

    checkHits(query, new int[]
      {801, 821, 831, 851, 861, 871, 881, 891,
              1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});

    assertTrue(searcher.explain(query, 801).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue().doubleValue() > 0.0f);
  }

  public void testNpeInSpanNearInSpanFirstInSpanNot() throws Exception {
    final int n = 5;
    SpanQuery include = spanFirstQuery(spanTermQuery("field", "forty"), n);
    SpanQuery near = spanNearOrderedQuery("field", n-1, "hundred", "forty");
    SpanQuery exclude = spanFirstQuery(near, n-1);
    SpanQuery q = spanNotQuery(include, exclude);
    
    checkHits(q, new int[]{40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048,
            1049, 1140, 1141, 1142, 1143, 1144, 1145, 1146, 1147, 1148, 1149, 1240, 1241, 1242, 1243, 1244,
            1245, 1246, 1247, 1248, 1249, 1340, 1341, 1342, 1343, 1344, 1345, 1346, 1347, 1348, 1349, 1440, 1441, 1442,
            1443, 1444, 1445, 1446, 1447, 1448, 1449, 1540, 1541, 1542, 1543, 1544, 1545, 1546, 1547, 1548, 1549, 1640,
            1641, 1642, 1643, 1644, 1645, 1646, 1647,
            1648, 1649, 1740, 1741, 1742, 1743, 1744, 1745, 1746, 1747, 1748, 1749, 1840, 1841, 1842, 1843, 1844, 1845, 1846,
            1847, 1848, 1849, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949});
  }
  
  public void testSpanNotWindowOne() throws Exception {
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "forty");
    SpanQuery query = spanNotQuery(near, spanTermQuery("field", "one"), 1, 1);

    checkHits(query, new int[]
      {840, 842, 843, 844, 845, 846, 847, 848, 849,
          1840, 1842, 1843, 1844, 1845, 1846, 1847, 1848, 1849});

    assertTrue(searcher.explain(query, 840).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 1842).getValue().doubleValue() > 0.0f);
  }
  
  public void testSpanNotWindowTwoBefore() throws Exception {
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "forty");
    SpanQuery query = spanNotQuery(near, spanTermQuery("field", "one"), 2, 0);

    checkHits(query, new int[]
      {840, 841, 842, 843, 844, 845, 846, 847, 848, 849});

    assertTrue(searcher.explain(query, 840).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 849).getValue().doubleValue() > 0.0f);
  }

  public void testSpanNotWindowNegPost() throws Exception {
    //test handling of invalid window < 0
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "one");
    SpanQuery or = spanOrQuery("field", "forty");
    SpanQuery query = spanNotQuery(near, or, 0, -1);
    checkHits(query, new int[]
       {801, 821, 831, 851, 861, 871, 881, 891,
               1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});

    query = spanNotQuery(near, or, 0, -2);
    checkHits(query, new int[]
       {801, 821, 831, 841, 851, 861, 871, 881, 891,
               1801, 1821, 1831, 1841, 1851, 1861, 1871, 1881, 1891});

    assertTrue(searcher.explain(query, 801).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue().doubleValue() > 0.0f);
  }

  public void testSpanNotWindowNegPre() throws Exception {
    //test handling of invalid window < 0
    SpanQuery near = spanNearOrderedQuery("field", 4, "eight", "one");
    SpanQuery or = spanOrQuery("field", "forty");
    SpanQuery query = spanNotQuery(near, or, -2, 0);
    checkHits(query, new int[]
        {801, 821, 831, 851, 861, 871, 881, 891,
            1801, 1821, 1831, 1851, 1861, 1871, 1881, 1891});

    query = spanNotQuery(near, or, -3, 0);
    checkHits(query, new int[]
        {801, 821, 831, 841, 851, 861, 871, 881, 891,
            1801, 1821, 1831, 1841, 1851, 1861, 1871, 1881, 1891});

    assertTrue(searcher.explain(query, 801).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 891).getValue().doubleValue() > 0.0f);
  }

  public void testSpanNotWindowDoubleExcludesBefore() throws Exception {
    //test hitting two excludes before an include
    SpanQuery near = spanNearOrderedQuery("field", 2, "forty", "two");
    SpanQuery exclude = spanTermQuery("field", "one");
    SpanQuery query = spanNotQuery(near, exclude, 4, 1);

    checkHits(query, new int[]
       {42, 242, 342, 442, 542, 642, 742, 842, 942});

    assertTrue(searcher.explain(query, 242).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 942).getValue().doubleValue() > 0.0f);
  }
  
  public void testSpanFirst() throws Exception {
    SpanQuery query = spanFirstQuery(spanTermQuery("field", "five"), 1);

    checkHits(query, new int[]
      {5, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513,
       514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527,
       528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541,
       542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555,
       556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569,
       570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583,
       584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597,
       598, 599});

    assertTrue(searcher.explain(query, 5).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 599).getValue().doubleValue() > 0.0f);

  }

  public void testSpanPositionRange() throws Exception {
    SpanQuery term1 = spanTermQuery("field", "five");
    SpanQuery query = spanPositionRangeQuery(term1, 1, 2);
    
    checkHits(query, new int[]
      {25,35, 45, 55, 65, 75, 85, 95});
    assertTrue(searcher.explain(query, 25).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 95).getValue().doubleValue() > 0.0f);

    query = spanPositionRangeQuery(term1, 0, 1);
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

    query = spanPositionRangeQuery(term1, 6, 7);
    checkHits(query, new int[]{});
  }

  public void testSpanOr() throws Exception {
    SpanQuery near1 = spanNearOrderedQuery("field", 0, "thirty", "three");
    SpanQuery near2 = spanNearOrderedQuery("field", 0, "forty", "seven");
    SpanQuery query = spanOrQuery(near1, near2);

    checkHits(query, new int[]
      {33, 47, 133, 147, 233, 247, 333, 347, 433, 447, 533, 547, 633, 647, 733,
              747, 833, 847, 933, 947, 1033, 1047, 1133, 1147, 1233, 1247, 1333,
              1347, 1433, 1447, 1533, 1547, 1633, 1647, 1733, 1747, 1833, 1847, 1933, 1947});

    assertTrue(searcher.explain(query, 33).getValue().doubleValue() > 0.0f);
    assertTrue(searcher.explain(query, 947).getValue().doubleValue() > 0.0f);
  }

  public void testSpanExactNested() throws Exception {
    SpanQuery near1 = spanNearOrderedQuery("field", 0, "three", "hundred");
    SpanQuery near2 = spanNearOrderedQuery("field", 0, "thirty", "three");
    SpanQuery query = spanNearOrderedQuery(0, near1, near2);
    
    checkHits(query, new int[] {333, 1333});

    assertTrue(searcher.explain(query, 333).getValue().doubleValue() > 0.0f);
  }

  public void testSpanNearOr() throws Exception {
    SpanQuery to1 = spanOrQuery("field", "six", "seven");
    SpanQuery to2 = spanOrQuery("field", "seven", "six");
    SpanQuery query = spanNearOrderedQuery(10, to1, to2);
    
    checkHits(query, new int[]
      {606, 607, 626, 627, 636, 637, 646, 647, 656, 657, 666, 667, 676, 677,
              686, 687, 696, 697, 706, 707, 726, 727, 736, 737, 746, 747, 756,
              757, 766, 767, 776, 777, 786, 787, 796, 797, 1606, 1607, 1626,
              1627, 1636, 1637, 1646, 1647, 1656, 1657, 1666, 1667, 1676, 1677,
              1686, 1687, 1696, 1697, 1706, 1707, 1726, 1727, 1736, 1737,
              1746, 1747, 1756, 1757, 1766, 1767, 1776, 1777, 1786, 1787, 1796,
              1797});
  }

  public void testSpanComplex1() throws Exception {
    SpanQuery tt1 = spanNearOrderedQuery("field", 0, "six", "hundred");
    SpanQuery tt2 = spanNearOrderedQuery("field", 0, "seven", "hundred");

    SpanQuery to1 = spanOrQuery(tt1, tt2);
    SpanQuery to2 = spanOrQuery("field", "seven", "six");
    SpanQuery query = spanNearOrderedQuery(100, to1, to2);
    
    checkHits(query, new int[]
      {606, 607, 626, 627, 636, 637, 646, 647, 656, 657, 666, 667, 676, 677, 686, 687, 696,
              697, 706, 707, 726, 727, 736, 737, 746, 747, 756, 757,
              766, 767, 776, 777, 786, 787, 796, 797, 1606, 1607, 1626, 1627, 1636, 1637, 1646,
              1647, 1656, 1657,
              1666, 1667, 1676, 1677, 1686, 1687, 1696, 1697, 1706, 1707, 1726, 1727, 1736, 1737,
              1746, 1747, 1756, 1757, 1766, 1767, 1776, 1777, 1786, 1787, 1796, 1797});
  }
  
  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, "field", searcher, results);
  }
}
