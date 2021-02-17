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
package org.apache.lucene.queries.payloads;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.SimplePayloadFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery.MatchOperation;
import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery.PayloadType;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanPositionRangeQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** basic test of payload-spans */
public class TestPayloadCheckQuery extends LuceneTestCase {
  private static IndexSearcher searcher;
  private static IndexReader reader;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Analyzer simplePayloadAnalyzer =
        new Analyzer() {
          @Override
          public TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
            return new TokenStreamComponents(tokenizer, new SimplePayloadFilter(tokenizer));
          }
        };

    directory = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            directory,
            newIndexWriterConfig(simplePayloadAnalyzer)
                .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
                .setMergePolicy(newLogMergePolicy()));
    // writer.infoStream = System.out;
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

  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, "field", searcher, results);
  }

  public void testSpanPayloadCheck() throws Exception {
    SpanQuery term1 = new SpanTermQuery(new Term("field", "five"));
    BytesRef pay = new BytesRef("pos: " + 5);
    SpanQuery query = new SpanPayloadCheckQuery(term1, Collections.singletonList(pay));
    checkHits(
        query,
        new int[] {
          1125, 1135, 1145, 1155, 1165, 1175, 1185, 1195, 1225, 1235, 1245, 1255, 1265, 1275, 1285,
          1295, 1325, 1335, 1345, 1355, 1365, 1375, 1385, 1395, 1425, 1435, 1445, 1455, 1465, 1475,
          1485, 1495, 1525, 1535, 1545, 1555, 1565, 1575, 1585, 1595, 1625, 1635, 1645, 1655, 1665,
          1675, 1685, 1695, 1725, 1735, 1745, 1755, 1765, 1775, 1785, 1795, 1825, 1835, 1845, 1855,
          1865, 1875, 1885, 1895, 1925, 1935, 1945, 1955, 1965, 1975, 1985, 1995
        });
    assertTrue(searcher.explain(query, 1125).getValue().doubleValue() > 0.0f);

    SpanTermQuery term2 = new SpanTermQuery(new Term("field", "hundred"));
    SpanNearQuery snq;
    SpanQuery[] clauses;
    List<BytesRef> list;
    BytesRef pay2;
    clauses = new SpanQuery[2];
    clauses[0] = term1;
    clauses[1] = term2;
    snq = new SpanNearQuery(clauses, 0, true);
    pay = new BytesRef("pos: " + 0);
    pay2 = new BytesRef("pos: " + 1);
    list = new ArrayList<>();
    list.add(pay);
    list.add(pay2);
    query = new SpanPayloadCheckQuery(snq, list);
    checkHits(
        query,
        new int[] {
          500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517,
          518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535,
          536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553,
          554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571,
          572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589,
          590, 591, 592, 593, 594, 595, 596, 597, 598, 599
        });
    clauses = new SpanQuery[3];
    clauses[0] = term1;
    clauses[1] = term2;
    clauses[2] = new SpanTermQuery(new Term("field", "five"));
    snq = new SpanNearQuery(clauses, 0, true);
    pay = new BytesRef("pos: " + 0);
    pay2 = new BytesRef("pos: " + 1);
    BytesRef pay3 = new BytesRef("pos: " + 2);
    list = new ArrayList<>();
    list.add(pay);
    list.add(pay2);
    list.add(pay3);
    query = new SpanPayloadCheckQuery(snq, list);
    checkHits(query, new int[] {505});
  }

  public void testInequalityPayloadChecks() throws Exception {
    // searching for the term five with a payload of either "pos: 0" or a payload of "pos: 1"
    SpanQuery termFive = new SpanTermQuery(new Term("field", "five"));
    SpanQuery termFifty = new SpanTermQuery(new Term("field", "fifty"));
    BytesRef payloadZero = new BytesRef("pos: " + 0);
    BytesRef payloadOne = new BytesRef("pos: " + 1);
    BytesRef payloadTwo = new BytesRef("pos: " + 2);
    BytesRef payloadThree = new BytesRef("pos: " + 3);
    BytesRef payloadFour = new BytesRef("pos: " + 4);
    BytesRef payloadFive = new BytesRef("pos: " + 5);
    // Terms that equal five with a payload of "pos: 1"
    SpanQuery stringEQ1 =
        new SpanPayloadCheckQuery(
            termFive,
            Collections.singletonList(payloadOne),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.EQ);
    checkHits(stringEQ1, new int[] {25, 35, 45, 55, 65, 75, 85, 95});

    // These queries return the same thing
    SpanQuery stringLT =
        new SpanPayloadCheckQuery(
            termFive,
            Collections.singletonList(payloadOne),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.LT);
    SpanQuery stringLTE =
        new SpanPayloadCheckQuery(
            termFive,
            Collections.singletonList(payloadZero),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.LTE);
    // string less than and string less than or equal
    checkHits(
        stringLT,
        new int[] {
          5, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516,
          517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534,
          535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552,
          553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570,
          571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588,
          589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599
        });
    checkHits(
        stringLTE,
        new int[] {
          5, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516,
          517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534,
          535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552,
          553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570,
          571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588,
          589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599
        });
    // greater than and greater than or equal tests.
    SpanQuery stringGT =
        new SpanPayloadCheckQuery(
            termFive,
            Collections.singletonList(payloadFour),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.GT);
    SpanQuery stringGTE =
        new SpanPayloadCheckQuery(
            termFive,
            Collections.singletonList(payloadFive),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.GTE);
    checkHits(
        stringGT,
        new int[] {
          1125, 1135, 1145, 1155, 1165, 1175, 1185, 1195, 1225, 1235, 1245, 1255, 1265, 1275, 1285,
          1295, 1325, 1335, 1345, 1355, 1365, 1375, 1385, 1395, 1425, 1435, 1445, 1455, 1465, 1475,
          1485, 1495, 1525, 1535, 1545, 1555, 1565, 1575, 1585, 1595, 1625, 1635, 1645, 1655, 1665,
          1675, 1685, 1695, 1725, 1735, 1745, 1755, 1765, 1775, 1785, 1795, 1825, 1835, 1845, 1855,
          1865, 1875, 1885, 1895, 1925, 1935, 1945, 1955, 1965, 1975, 1985, 1995
        });
    checkHits(
        stringGTE,
        new int[] {
          1125, 1135, 1145, 1155, 1165, 1175, 1185, 1195, 1225, 1235, 1245, 1255, 1265, 1275, 1285,
          1295, 1325, 1335, 1345, 1355, 1365, 1375, 1385, 1395, 1425, 1435, 1445, 1455, 1465, 1475,
          1485, 1495, 1525, 1535, 1545, 1555, 1565, 1575, 1585, 1595, 1625, 1635, 1645, 1655, 1665,
          1675, 1685, 1695, 1725, 1735, 1745, 1755, 1765, 1775, 1785, 1795, 1825, 1835, 1845, 1855,
          1865, 1875, 1885, 1895, 1925, 1935, 1945, 1955, 1965, 1975, 1985, 1995
        });

    // now a not so happy path...
    SpanQuery stringEQ2many =
        new SpanPayloadCheckQuery(
            termFive,
            Arrays.asList(payloadOne, payloadZero),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.EQ);
    // fewer terms than payloads should not match anything, one wonders if this should be an error,
    // but it's been explicitly ignored previously, so changing it is a possible back compat issue.
    checkHits(stringEQ2many, new int[] {});

    // now some straight forward two term cases...
    SpanQuery stringEQ2 =
        new SpanPayloadCheckQuery(
            new SpanNearQuery(new SpanQuery[] {termFifty, termFive}, 0, true),
            Arrays.asList(payloadZero, payloadOne),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.EQ);
    checkHits(stringEQ2, new int[] {55});

    SpanQuery stringGT2 =
        new SpanPayloadCheckQuery(
            new SpanNearQuery(new SpanQuery[] {termFifty, termFive}, 0, true),
            Arrays.asList(payloadZero, payloadOne),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.GT);
    checkHits(
        stringGT2,
        new int[] { // spotless:off
            55,  155,  255,  355,  455,  555,  655,  755,  855,  955,
          1055, 1155, 1255, 1355, 1455, 1555, 1655, 1755, 1855, 1955
        }); // spotless:on
    SpanQuery stringGTE2 =
        new SpanPayloadCheckQuery(
            new SpanNearQuery(new SpanQuery[] {termFifty, termFive}, 0, true),
            Arrays.asList(payloadZero, payloadOne),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.GTE);
    checkHits(
        stringGTE2,
        new int[] { // spotless:off
            55,  155,  255,  355,  455,  555,  655,  755,  855,  955,
          1055, 1155, 1255, 1355, 1455, 1555, 1655, 1755, 1855, 1955
        });  // spotless:on

    SpanQuery stringLT2 =
        new SpanPayloadCheckQuery(
            new SpanNearQuery(new SpanQuery[] {termFifty, termFive}, 0, true),
            Arrays.asList(payloadTwo, payloadThree),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.LT);
    checkHits(stringLT2, new int[] {55});
    SpanQuery stringLTE2 =
        new SpanPayloadCheckQuery(
            new SpanNearQuery(new SpanQuery[] {termFifty, termFive}, 0, true),
            Arrays.asList(payloadTwo, payloadThree),
            SpanPayloadCheckQuery.PayloadType.STRING,
            MatchOperation.LTE);
    checkHits(stringLTE2, new int[] {55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 1055});

    // note: I can imagine support for SpanOrQuery might be interesting but that's for some other
    // time, currently such support is  made intractable by the fact that reset() gets called and
    // sets "upto" back to zero between SpanOrQuery subclauses.
  }

  public void testUnorderedPayloadChecks() throws Exception {

    SpanTermQuery term5 = new SpanTermQuery(new Term("field", "five"));
    SpanTermQuery term100 = new SpanTermQuery(new Term("field", "hundred"));
    SpanTermQuery term4 = new SpanTermQuery(new Term("field", "four"));
    SpanNearQuery nearQuery = new SpanNearQuery(new SpanQuery[] {term5, term100, term4}, 0, false);

    List<BytesRef> payloads = new ArrayList<>();
    payloads.add(new BytesRef("pos: " + 2));
    payloads.add(new BytesRef("pos: " + 1));
    payloads.add(new BytesRef("pos: " + 0));

    SpanPayloadCheckQuery payloadQuery = new SpanPayloadCheckQuery(nearQuery, payloads);
    checkHits(payloadQuery, new int[] {405});

    payloads.clear();
    payloads.add(new BytesRef("pos: " + 0));
    payloads.add(new BytesRef("pos: " + 1));
    payloads.add(new BytesRef("pos: " + 2));

    payloadQuery = new SpanPayloadCheckQuery(nearQuery, payloads);
    checkHits(payloadQuery, new int[] {504});
  }

  public void testComplexSpanChecks() throws Exception {
    SpanTermQuery one = new SpanTermQuery(new Term("field", "one"));
    SpanTermQuery thous = new SpanTermQuery(new Term("field", "thousand"));
    // should be one position in between
    SpanTermQuery hundred = new SpanTermQuery(new Term("field", "hundred"));
    SpanTermQuery three = new SpanTermQuery(new Term("field", "three"));

    SpanNearQuery oneThous = new SpanNearQuery(new SpanQuery[] {one, thous}, 0, true);
    SpanNearQuery hundredThree = new SpanNearQuery(new SpanQuery[] {hundred, three}, 0, true);
    SpanNearQuery oneThousHunThree =
        new SpanNearQuery(new SpanQuery[] {oneThous, hundredThree}, 1, true);
    SpanQuery query;
    // this one's too small
    query = new SpanPositionRangeQuery(oneThousHunThree, 1, 2);
    checkHits(query, new int[] {});
    // this one's just right
    query = new SpanPositionRangeQuery(oneThousHunThree, 0, 6);
    checkHits(query, new int[] {1103, 1203, 1303, 1403, 1503, 1603, 1703, 1803, 1903});

    List<BytesRef> payloads = new ArrayList<>();
    BytesRef pay = new BytesRef(("pos: " + 0).getBytes(StandardCharsets.UTF_8));
    BytesRef pay2 = new BytesRef(("pos: " + 1).getBytes(StandardCharsets.UTF_8));
    BytesRef pay3 = new BytesRef(("pos: " + 3).getBytes(StandardCharsets.UTF_8));
    BytesRef pay4 = new BytesRef(("pos: " + 4).getBytes(StandardCharsets.UTF_8));
    payloads.add(pay);
    payloads.add(pay2);
    payloads.add(pay3);
    payloads.add(pay4);
    query = new SpanPayloadCheckQuery(oneThousHunThree, payloads);
    checkHits(query, new int[] {1103, 1203, 1303, 1403, 1503, 1603, 1703, 1803, 1903});
  }

  public void testEquality() {
    SpanQuery sq1 = new SpanTermQuery(new Term("field", "one"));
    SpanQuery sq2 = new SpanTermQuery(new Term("field", "two"));
    BytesRef payload1 = new BytesRef("pay1");
    BytesRef payload2 = new BytesRef("pay2");
    SpanQuery query1 = new SpanPayloadCheckQuery(sq1, Collections.singletonList(payload1));
    SpanQuery query2 = new SpanPayloadCheckQuery(sq2, Collections.singletonList(payload1));
    SpanQuery query3 = new SpanPayloadCheckQuery(sq1, Collections.singletonList(payload2));
    SpanQuery query4 = new SpanPayloadCheckQuery(sq2, Collections.singletonList(payload2));
    SpanQuery query5 = new SpanPayloadCheckQuery(sq1, Collections.singletonList(payload1));

    assertEquals(query1, query5);
    assertFalse(query1.equals(query2));
    assertFalse(query1.equals(query3));
    assertFalse(query1.equals(query4));
    assertFalse(query2.equals(query3));
    assertFalse(query2.equals(query4));
    assertFalse(query3.equals(query4));

    // Create an integer and a float encoded payload
    Integer i = 451;
    BytesRef intPayload = new BytesRef(ByteBuffer.allocate(4).putInt(i).array());
    Float e = 2.71828f;
    BytesRef floatPayload = new BytesRef(ByteBuffer.allocate(4).putFloat(e).array());
    SpanQuery floatLTQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(floatPayload), PayloadType.FLOAT, MatchOperation.LT);
    SpanQuery floatLTEQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(floatPayload), PayloadType.FLOAT, MatchOperation.LTE);
    SpanQuery floatGTQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(floatPayload), PayloadType.FLOAT, MatchOperation.GT);
    SpanQuery floatGTEQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(floatPayload), PayloadType.FLOAT, MatchOperation.GTE);

    SpanQuery intLTQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.INT, MatchOperation.LT);
    SpanQuery intLTEQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.INT, MatchOperation.LTE);
    SpanQuery intGTQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.INT, MatchOperation.GT);
    SpanQuery intGTEQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.INT, MatchOperation.GT);

    // string inequality checks
    SpanQuery stringLTQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.STRING, MatchOperation.LT);
    SpanQuery stringLTEQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.STRING, MatchOperation.LTE);
    SpanQuery stringGTQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.STRING, MatchOperation.GT);
    SpanQuery stringGTEQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.STRING, MatchOperation.GT);
    SpanQuery stringEQQuery =
        new SpanPayloadCheckQuery(
            sq1, Collections.singletonList(intPayload), PayloadType.STRING, MatchOperation.EQ);

    SpanQuery stringDefaultQuery =
        new SpanPayloadCheckQuery(sq1, Collections.singletonList(intPayload));

    assertTrue(stringDefaultQuery.equals(stringEQQuery));
    assertFalse(stringDefaultQuery.equals(stringGTQuery));
    assertFalse(stringDefaultQuery.equals(stringGTEQuery));
    assertFalse(stringDefaultQuery.equals(stringLTQuery));
    assertFalse(stringDefaultQuery.equals(stringLTEQuery));

    assertFalse(floatLTQuery.equals(floatLTEQuery));
    assertFalse(floatLTQuery.equals(floatGTQuery));
    assertFalse(floatLTQuery.equals(floatGTEQuery));
    assertFalse(floatLTQuery.equals(intLTQuery));
    assertFalse(floatLTQuery.equals(intLTEQuery));
    assertFalse(floatLTQuery.equals(intGTQuery));
    assertFalse(floatLTQuery.equals(intGTEQuery));
  }

  public void testRewrite() throws IOException {
    SpanMultiTermQueryWrapper<WildcardQuery> fiv =
        new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("field", "fiv*")));
    SpanMultiTermQueryWrapper<WildcardQuery> hund =
        new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("field", "hund*")));
    SpanMultiTermQueryWrapper<WildcardQuery> twent =
        new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("field", "twent*")));
    SpanMultiTermQueryWrapper<WildcardQuery> nin =
        new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("field", "nin*")));

    SpanNearQuery sq = new SpanNearQuery(new SpanQuery[] {fiv, hund, twent, nin}, 0, true);

    List<BytesRef> payloads = new ArrayList<>();
    payloads.add(new BytesRef("pos: 0"));
    payloads.add(new BytesRef("pos: 1"));
    payloads.add(new BytesRef("pos: 2"));
    payloads.add(new BytesRef("pos: 3"));

    SpanPayloadCheckQuery query = new SpanPayloadCheckQuery(sq, payloads);

    // if query wasn't rewritten properly, the query would have failed with "Rewrite first!"
    checkHits(query, new int[] {529});
  }
}
