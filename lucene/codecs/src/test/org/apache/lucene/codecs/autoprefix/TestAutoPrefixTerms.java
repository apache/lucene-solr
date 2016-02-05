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
package org.apache.lucene.codecs.autoprefix;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CompiledAutomaton;

public class TestAutoPrefixTerms extends LuceneTestCase {

  private int minItemsPerBlock = TestUtil.nextInt(random(), 2, 100);
  private int maxItemsPerBlock = 2*(Math.max(2, minItemsPerBlock-1)) + random().nextInt(100);
  private int minTermsAutoPrefix = TestUtil.nextInt(random(), 2, 100);
  private int maxTermsAutoPrefix = random().nextBoolean() ? Math.max(2, (minTermsAutoPrefix-1)*2 + random().nextInt(100)) : Integer.MAX_VALUE;

  public TestAutoPrefixTerms() {
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: using minItemsPerBlock=" + minItemsPerBlock + " maxItemsPerBlock=" + maxItemsPerBlock);
      System.out.println("TEST: using minTermsAutoPrefix=" + minTermsAutoPrefix + " maxTermsAutoPrefix=" + maxTermsAutoPrefix);
    }
  }

  private final Codec codec = TestUtil.alwaysPostingsFormat(new AutoPrefixPostingsFormat(minItemsPerBlock, maxItemsPerBlock,
                                                                                         minTermsAutoPrefix, maxTermsAutoPrefix));

  // Numbers in a restricted range, encoded in decimal, left-0-padded:
  public void testBasicNumericRanges() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codec);
    IndexWriter w = new IndexWriter(dir, iwc);
    int numTerms = TestUtil.nextInt(random(), 3000, 50000);
    Set<String> terms = new HashSet<>();
    int digits = TestUtil.nextInt(random(), 5, 10);
    int maxValue = 1;
    for(int i=0;i<digits;i++) {
      maxValue *= 10;
    }
    String format = "%0" + digits + "d";
    while (terms.size() < numTerms) {
      terms.add(String.format(Locale.ROOT, format, random().nextInt(maxValue)));
    }

    for(String term : terms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Field.Store.NO));
      doc.add(new NumericDocValuesField("field", Long.parseLong(term)));
      w.addDocument(doc);
    }

    if (VERBOSE) System.out.println("\nTEST: now optimize");
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }

    if (VERBOSE) System.out.println("\nTEST: now done");
    IndexReader r = DirectoryReader.open(w);

    List<String> sortedTerms = new ArrayList<>(terms);
    Collections.sort(sortedTerms);

    if (VERBOSE) {
      System.out.println("TEST: sorted terms:");
      int idx = 0;
      for(String term : sortedTerms) {
        System.out.println(idx + ": " + term);
        idx++;
      }
    }

    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {
      int min, max;
      while (true) {
        min = random().nextInt(maxValue);
        max = random().nextInt(maxValue);
        if (min == max) {
          continue;
        } else if (min > max) {
          int x = min;
          min = max;
          max = x;
        }
        break;
      }
      
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " min=" + min + " max=" + max);
      }

      boolean minInclusive = random().nextBoolean();
      boolean maxInclusive = random().nextBoolean();
      BytesRef minTerm = new BytesRef(String.format(Locale.ROOT, format, min));
      BytesRef maxTerm = new BytesRef(String.format(Locale.ROOT, format, max));
      CompiledAutomaton ca = new CompiledAutomaton(Automata.makeBinaryInterval(minTerm, minInclusive, maxTerm, maxInclusive),
                                                   true, false, Integer.MAX_VALUE, true);

      TermsEnum te = ca.getTermsEnum(MultiFields.getTerms(r, "field"));
      NumericDocValues docValues = MultiDocValues.getNumericValues(r, "field");
      PostingsEnum postingsEnum = null;

      VerifyAutoPrefixTerms verifier = new VerifyAutoPrefixTerms(r.maxDoc(), minTerm, maxTerm);

      while (te.next() != null) {
        if (VERBOSE) {
          System.out.println("  got term=" + te.term().utf8ToString());
        }
        verifier.sawTerm(te.term());
        postingsEnum = te.postings(postingsEnum);
        int docID;
        while ((docID = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
          long v = docValues.get(docID);
          assert v >= min && v <= max: "docID=" + docID + " v=" + v;
          // The auto-prefix terms should never "overlap" one another, so we should only ever see a given docID one time:
          if (VERBOSE) {
            System.out.println("    got docID=" + docID + " v=" + v);
          }
          verifier.sawDoc(docID);
        }
      }
      
      int startLoc = Collections.binarySearch(sortedTerms, String.format(Locale.ROOT, format, min));
      if (startLoc < 0) {
        startLoc = -startLoc-1;
      } else if (minInclusive == false) {
        startLoc++;
      }
      int endLoc = Collections.binarySearch(sortedTerms, String.format(Locale.ROOT, format, max));
      if (endLoc < 0) {
        endLoc = -endLoc-2;
      } else if (maxInclusive == false) {
        endLoc--;
      }
      verifier.finish(endLoc-startLoc+1, maxTermsAutoPrefix);
    }

    r.close();
    w.close();
    dir.close();
  }

  private static BytesRef intToBytes(int v) {
    int sortableBits = v ^ 0x80000000;
    BytesRef token = new BytesRef(4);
    token.length = 4;
    int index = 3;
    while (index >= 0) {
      token.bytes[index] = (byte) (sortableBits & 0xff);
      index--;
      sortableBits >>>= 8;
    }
    return token;
  }

  // Numbers are encoded in full binary (4 byte ints):
  public void testBinaryNumericRanges() throws Exception {
    if (VERBOSE) {
      System.out.println("TEST: minItemsPerBlock=" + minItemsPerBlock);
      System.out.println("TEST: maxItemsPerBlock=" + maxItemsPerBlock);
      System.out.println("TEST: minTermsAutoPrefix=" + minTermsAutoPrefix);
      System.out.println("TEST: maxTermsAutoPrefix=" + maxTermsAutoPrefix);
    }
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codec);
    IndexWriter w = new IndexWriter(dir, iwc);
    int numTerms = TestUtil.nextInt(random(), 3000, 50000);
    Set<Integer> terms = new HashSet<>();
    while (terms.size() < numTerms) {
      terms.add(random().nextInt());
    }

    for(Integer term : terms) {
      Document doc = new Document();
      doc.add(newStringField("field", intToBytes(term), Field.Store.NO));
      doc.add(new NumericDocValuesField("field", term));
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      if (VERBOSE) System.out.println("TEST: now force merge");
      w.forceMerge(1);
    }

    IndexReader r = DirectoryReader.open(w);

    List<Integer> sortedTerms = new ArrayList<>(terms);
    Collections.sort(sortedTerms);

    if (VERBOSE) {
      System.out.println("TEST: sorted terms:");
      int idx = 0;
      for(Integer term : sortedTerms) {
        System.out.println(idx + ": " + term);
        idx++;
      }
    }

    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {

      int min, max;
      while (true) {
        min = random().nextInt();
        max = random().nextInt();
        if (min == max) {
          continue;
        } else if (min > max) {
          int x = min;
          min = max;
          max = x;
        }
        break;
      }

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " min=" + min + " (" + intToBytes(min) + ") max=" + max + " (" + intToBytes(max) + ")");
      }
      
      boolean minInclusive = random().nextBoolean();
      BytesRef minTerm = intToBytes(min);
      boolean maxInclusive = random().nextBoolean();
      BytesRef maxTerm = intToBytes(max);
      CompiledAutomaton ca = new CompiledAutomaton(Automata.makeBinaryInterval(minTerm, minInclusive, maxTerm, maxInclusive),
                                                   true, false, Integer.MAX_VALUE, true);

      TermsEnum te = ca.getTermsEnum(MultiFields.getTerms(r, "field"));
      NumericDocValues docValues = MultiDocValues.getNumericValues(r, "field");
      PostingsEnum postingsEnum = null;
      VerifyAutoPrefixTerms verifier = new VerifyAutoPrefixTerms(r.maxDoc(), minTerm, maxTerm);
      while (te.next() != null) {
        if (VERBOSE) {
          System.out.println("  got term=" + te.term() + " docFreq=" + te.docFreq());
        }
        verifier.sawTerm(te.term());        
        postingsEnum = te.postings(postingsEnum);
        int docID;
        while ((docID = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
          long v = docValues.get(docID);
          assert v >= min && v <= max: "docID=" + docID + " v=" + v;
          verifier.sawDoc(docID);
        }
      }
      
      int startLoc = Collections.binarySearch(sortedTerms, min);
      if (startLoc < 0) {
        startLoc = -startLoc-1;
      } else if (minInclusive == false) {
        startLoc++;
      }
      int endLoc = Collections.binarySearch(sortedTerms, max);
      if (endLoc < 0) {
        endLoc = -endLoc-2;
      } else if (maxInclusive == false) {
        endLoc--;
      }
      int expectedHits = endLoc-startLoc+1;
      try {
        verifier.finish(expectedHits, maxTermsAutoPrefix);
      } catch (AssertionError ae) {
        for(int i=0;i<numTerms;i++) {
          if (verifier.allHits.get(i) == false) {
            int v = (int) docValues.get(i);
            boolean accept = (v > min || (v == min && minInclusive)) &&
              (v < max || (v == max && maxInclusive));
            if (accept) {
              System.out.println("MISSING: docID=" + i + " v=" + v + " term=" + intToBytes(v));
            }
          }
        }

        throw ae;
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  // Non-numeric, simple prefix query
  public void testBasicPrefixTerms() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codec);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    IndexWriter w = new IndexWriter(dir, iwc);
    int numTerms = TestUtil.nextInt(random(), 3000, 50000);
    Set<String> terms = new HashSet<>();
    while (terms.size() < numTerms) {
      terms.add(TestUtil.randomSimpleString(random()));
    }

    for(String term : terms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Field.Store.NO));
      doc.add(new BinaryDocValuesField("field", new BytesRef(term)));
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: now force merge");
      }
      w.forceMerge(1);
    }

    IndexReader r = DirectoryReader.open(w);

    List<String> sortedTerms = new ArrayList<>(terms);
    Collections.sort(sortedTerms);

    if (VERBOSE) {
      System.out.println("TEST: sorted terms:");
      int idx = 0;
      for(String term : sortedTerms) {
        System.out.println(idx + ": " + term);
        idx++;
      }
    }

    if (VERBOSE) {
      System.out.println("TEST: r=" + r);
    }

    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      String prefix;
      if (random().nextInt(100) == 42) {
        prefix = "";
      } else {
        prefix = TestUtil.randomSimpleString(random(), 1, 4);
      }
      BytesRef prefixBR = new BytesRef(prefix);
      if (VERBOSE) {
        System.out.println("  prefix=" + prefix);
      }

      CompiledAutomaton ca = new CompiledAutomaton(PrefixQuery.toAutomaton(prefixBR), true, false, Integer.MAX_VALUE, true);
      TermsEnum te = ca.getTermsEnum(MultiFields.getTerms(r, "field"));
      BinaryDocValues docValues = MultiDocValues.getBinaryValues(r, "field");
      PostingsEnum postingsEnum = null;

      VerifyAutoPrefixTerms verifier = new VerifyAutoPrefixTerms(r.maxDoc(), prefixBR);

      while (te.next() != null) {
        if (VERBOSE) {
          System.out.println("TEST: got term=" + te.term().utf8ToString() + " docFreq=" + te.docFreq());
        }
        verifier.sawTerm(te.term());        
        postingsEnum = te.postings(postingsEnum);
        int docID;
        while ((docID = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
          assertTrue("prefixBR=" + prefixBR + " docBR=" + docValues.get(docID), StringHelper.startsWith(docValues.get(docID), prefixBR));
          // The auto-prefix terms should never "overlap" one another, so we should only ever see a given docID one time:
          verifier.sawDoc(docID);
        }
      }
      
      int startLoc = Collections.binarySearch(sortedTerms, prefix);
      if (startLoc < 0) {
        startLoc = -startLoc-1;
      }
      int endLoc = Collections.binarySearch(sortedTerms, prefix + (char) ('z'+1));
      if (endLoc < 0) {
        endLoc = -endLoc-2;
      }
      int expectedHits = endLoc-startLoc+1;
      try {
        verifier.finish(expectedHits, maxTermsAutoPrefix);
      } catch (AssertionError ae) {
        for(int i=0;i<numTerms;i++) {
          if (verifier.allHits.get(i) == false) {
            String s = docValues.get(i).utf8ToString();
            if (s.startsWith(prefix)) {
              System.out.println("MISSING: docID=" + i + " term=" + s);
            }
          }
        }

        throw ae;
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDemoPrefixTerms() throws Exception {
    if (VERBOSE) {
      System.out.println("\nTEST: minTermsAutoPrefix=" + minTermsAutoPrefix + " maxTermsAutoPrefix=" + maxTermsAutoPrefix);
      System.out.println("\nTEST: minItemsPerBlock=" + minItemsPerBlock + " maxItemsPerBlock=" + maxItemsPerBlock);
    }
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codec);
    IndexWriter w = new IndexWriter(dir, iwc);
    int numDocs = 30;

    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      doc.add(new StringField("field", "" + (char) (97+i), Field.Store.NO));
      w.addDocument(doc);
      doc = new Document();
      doc.add(new StringField("field", "a" + (char) (97+i), Field.Store.NO));
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }

    IndexReader r = DirectoryReader.open(w);
    Terms terms = MultiFields.getTerms(r, "field");
    if (VERBOSE) {
      System.out.println("\nTEST: now intersect");
    }
    CompiledAutomaton ca = new CompiledAutomaton(PrefixQuery.toAutomaton(new BytesRef("a")), false, false, Integer.MAX_VALUE, true);
    TermsEnum te = ca.getTermsEnum(terms);
    PostingsEnum postingsEnum = null;

    VerifyAutoPrefixTerms verifier = new VerifyAutoPrefixTerms(r.maxDoc(), new BytesRef("a"));
    //TermsEnum te = terms.intersect(new CompiledAutomaton(a, true, false), null);
    while (te.next() != null) {
      verifier.sawTerm(te.term());
      postingsEnum = te.postings(postingsEnum);
      int docID;
      while ((docID = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
        // The auto-prefix terms should never "overlap" one another, so we should only ever see a given docID one time:
        verifier.sawDoc(docID);
      }
    }
    // 1 document has exactly "a", and 30 documents had "a?"
    verifier.finish(31, maxTermsAutoPrefix);
    PrefixQuery q = new PrefixQuery(new Term("field", "a"));
    assertEquals(31, newSearcher(r).search(q, 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  /** Helper class to ensure auto-prefix terms 1) never overlap one another, and 2) are used when they should be. */
  private static class VerifyAutoPrefixTerms {
    final FixedBitSet allHits;
    private final Map<BytesRef,Integer> prefixCounts = new HashMap<>();
    private int totPrefixCount;
    private final BytesRef[] bounds;
    private int totTermCount;
    private BytesRef lastTerm;

    public VerifyAutoPrefixTerms(int maxDoc, BytesRef... bounds) {
      allHits = new FixedBitSet(maxDoc);
      assert bounds.length > 0;
      this.bounds = bounds;
    }

    public void sawTerm(BytesRef term) {
      //System.out.println("saw term=" + term);
      if (lastTerm != null) {
        assertTrue(lastTerm.compareTo(term) < 0);
      }
      lastTerm = BytesRef.deepCopyOf(term);
      totTermCount++;
      totPrefixCount += term.length;
      for(int i=1;i<=term.length;i++) {
        BytesRef prefix = BytesRef.deepCopyOf(term);
        prefix.length = i;
        Integer count = prefixCounts.get(prefix);
        if (count == null) {
          count = 1;
        } else {
          count += 1;
        }
        prefixCounts.put(prefix, count);
      }
    }

    public void sawDoc(int docID) {
      // The auto-prefix terms should never "overlap" one another, so we should only ever see a given docID one time:
      assertFalse(allHits.getAndSet(docID));
    }

    public void finish(int expectedNumHits, int maxPrefixCount) {

      if (maxPrefixCount != -1) {
        // Auto-prefix terms were used in this test
        long allowedMaxTerms;

        if (bounds.length == 1) {
          // Simple prefix query: we should never see more than maxPrefixCount terms, except for the empty string:
          if (bounds[0].length == 0) {
            allowedMaxTerms = Integer.MAX_VALUE;
          } else {
            allowedMaxTerms = maxPrefixCount;
          }
        } else {
          // Trickier: we need to allow for maxPrefixTerms for each different leading byte in the min and max:
          assert bounds.length == 2;
          BytesRef minTerm = bounds[0];
          BytesRef maxTerm = bounds[1];

          int commonPrefix = 0;
          for(int i=0;i<minTerm.length && i<maxTerm.length;i++) {
            if (minTerm.bytes[minTerm.offset+i] != maxTerm.bytes[maxTerm.offset+i]) {
              commonPrefix = i;
              break;
            }
          }

          allowedMaxTerms = maxPrefixCount * (long) ((minTerm.length-commonPrefix) + (maxTerm.length-commonPrefix));
          if (commonPrefix == 0) {
            int min;
            if (minTerm.length == 0) {
              min = 0;
            } else {
              min = minTerm.bytes[minTerm.offset] & 0xff;
            }
            int max;
            if (maxTerm.length == 0) {
              max = 0;
            } else {
              max = maxTerm.bytes[maxTerm.offset] & 0xff;
            }
            if (max > min) {
              // When maxPrefixCount is small (< 16), each byte of the term can require more than one "level" of auto-prefixing:
              // NOTE: this is still only approximate ... it's tricky to get a closed form max bound that's "tight"
              allowedMaxTerms += MathUtil.log(max-min, maxPrefixCount);
            }
          }
        }

        assertTrue("totTermCount=" + totTermCount + " is > allowedMaxTerms=" + allowedMaxTerms, totTermCount <= allowedMaxTerms);
      }

      assertEquals(expectedNumHits, allHits.cardinality());
      int sum = 0;
      for(Map.Entry<BytesRef,Integer> ent : prefixCounts.entrySet()) {

        BytesRef prefix = ent.getKey();
        if (VERBOSE) {
          System.out.println("  verify prefix=" + TestUtil.bytesRefToString(prefix) + " count=" + ent.getValue());
        }

        if (maxPrefixCount != -1) {
          // Auto-prefix terms were used in this test

          int sumLeftoverSuffix = 0;
          for(BytesRef bound : bounds) {

            int minSharedLength = Math.min(bound.length, prefix.length);
            int commonPrefix = minSharedLength;
            for(int i=0;i<minSharedLength;i++) {
              if (bound.bytes[bound.offset+i] != prefix.bytes[prefix.offset+i]) {
                commonPrefix = i;
                break;
              }
            }
            sumLeftoverSuffix += bound.length - commonPrefix;
          }

          long limit = (1+sumLeftoverSuffix) * (long) maxPrefixCount;

          assertTrue("maxPrefixCount=" + maxPrefixCount + " prefix=" + prefix + " sumLeftoverSuffix=" + sumLeftoverSuffix + " limit=" + limit + " vs actual=" +ent.getValue(),
                     ent.getValue() <= limit);
        }

        sum += ent.getValue();
      }

      // Make sure no test bug:
      assertEquals(totPrefixCount, sum);
    }
  }

  /** Make sure you get clear exc. if you try to use this within anything but IndexOptions.DOCS fields. */
  public void testWithFreqs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codec);
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldType ft = new FieldType(StringField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Document doc = new Document();
    doc.add(new Field("foo", "bar bar", ft));
    w.addDocument(doc);
    try {
      w.commit();
    } catch (IllegalStateException ise) {
      assertEquals("ranges can only be indexed with IndexOptions.DOCS (field: foo)", ise.getMessage());
    }
    w.close();
    dir.close();
  }

  /** Make sure you get clear exc. if you try to use this within anything but IndexOptions.DOCS fields. */
  public void testWithPositions() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codec);
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldType ft = new FieldType(StringField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    Document doc = new Document();
    doc.add(new Field("foo", "bar bar", ft));
    w.addDocument(doc);
    try {
      w.commit();
    } catch (IllegalStateException ise) {
      assertEquals("ranges can only be indexed with IndexOptions.DOCS (field: foo)", ise.getMessage());
    }
    w.close();
    dir.close();
  }

  /** Make sure you get clear exc. if you try to use this within anything but IndexOptions.DOCS fields. */
  public void testWithOffsets() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codec);
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldType ft = new FieldType(StringField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();
    doc.add(new Field("foo", "bar bar", ft));
    w.addDocument(doc);
    try {
      w.commit();
    } catch (IllegalStateException ise) {
      assertEquals("ranges can only be indexed with IndexOptions.DOCS (field: foo)", ise.getMessage());
    }
    w.close();
    dir.close();
  }

  /** Make sure auto prefix terms are used with TermRangeQuery */
  public void testTermRange() throws Exception {

    List<String> prefixes = new ArrayList<>();
    for(int i=1;i<5;i++) {
      char[] chars = new char[i];
      Arrays.fill(chars, 'a');
      prefixes.add(new String(chars));
    }

    Set<String> randomTerms = new HashSet<>();
    int numTerms = atLeast(10000);
    while (randomTerms.size() < numTerms) {
      for(String prefix : prefixes) {
        randomTerms.add(prefix + TestUtil.randomSimpleString(random()));
      }
    }

    // We make term range aa<start> - aa<end>
    char start;
    char end;

    int actualCount;
    boolean startInclusive = random().nextBoolean();
    boolean endInclusive = random().nextBoolean();
    String startTerm;
    String endTerm;

    while (true) {
      start = (char) TestUtil.nextInt(random(), 'a', 'm');
      end = (char) TestUtil.nextInt(random(), start+1, 'z');

      actualCount = 0;

      startTerm = "aa" + start;
      endTerm = "aa" + end;

      for(String term : randomTerms) {
        int cmpStart = startTerm.compareTo(term);
        int cmpEnd = endTerm.compareTo(term);
        if ((cmpStart < 0 || (startInclusive && cmpStart == 0)) &&
            (cmpEnd > 0 || (endInclusive && cmpEnd == 0))) {
          actualCount++;
        }
      }

      if (actualCount > 2000) {
        break;
      }
    }

    if (VERBOSE) {
      System.out.println("start " + startTerm + " inclusive? " + startInclusive);
      System.out.println("end " + endTerm + " inclusive? " + endInclusive);
      System.out.println("actual count " + actualCount);
    }

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    int minTermsInBlock = TestUtil.nextInt(random(), 2, 100);
    int maxTermsInBlock = Math.max(2, (minTermsInBlock-1)*2 + random().nextInt(100));

    int minTermsAutoPrefix = TestUtil.nextInt(random(), 2, 100);
    int maxTermsAutoPrefix = random().nextBoolean() ? Math.max(2, (minTermsAutoPrefix-1)*2 + random().nextInt(100)) : Integer.MAX_VALUE;

    if (VERBOSE) {
      System.out.println("minTermsAutoPrefix " + minTermsAutoPrefix);
      System.out.println("maxTermsAutoPrefix " + maxTermsAutoPrefix);
    }

    iwc.setCodec(TestUtil.alwaysPostingsFormat(new AutoPrefixPostingsFormat(minTermsInBlock, maxTermsInBlock,
                                                                            minTermsAutoPrefix, maxTermsAutoPrefix)));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    if (VERBOSE) {
      System.out.println("TEST: index terms");
    }
    for (String term : randomTerms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Field.Store.NO));
      w.addDocument(doc);
      if (VERBOSE) {
        System.out.println("  " + term);
      }
    }

    if (VERBOSE) {
      System.out.println("TEST: now force merge");
    }

    w.forceMerge(1);
    IndexReader r = w.getReader();
    final Terms terms = MultiFields.getTerms(r, "field");
    IndexSearcher s = new IndexSearcher(r);
    final int finalActualCount = actualCount;
    if (VERBOSE) {
      System.out.println("start=" + startTerm + " end=" + endTerm + " startIncl=" + startInclusive + " endIncl=" + endInclusive);
    }
    TermRangeQuery q = new TermRangeQuery("field", new BytesRef(startTerm), new BytesRef(endTerm), startInclusive, endInclusive) {
      public TermRangeQuery checkTerms() throws IOException {
        TermsEnum termsEnum = getTermsEnum(terms, new AttributeSource());
        int count = 0;
        while (termsEnum.next() != null) {
          if (VERBOSE) {
            System.out.println("got term: " + termsEnum.term().utf8ToString());
          }
          count++;
        }
        if (VERBOSE) {
          System.out.println("count " + count + " vs finalActualCount=" + finalActualCount);
        }

        // Auto-prefix term(s) should have kicked in, so we should have visited fewer than the total number of aa* terms:
        assertTrue(count < finalActualCount);

        return this;
      }
    }.checkTerms();

    int maxClauseCount = BooleanQuery.getMaxClauseCount();

    try {

      // TODO test with boolean rewrite as well once we can create term
      // queries on fake terms
      /*if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
        BooleanQuery.setMaxClauseCount(actualCount);
      } else if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
        BooleanQuery.setMaxClauseCount(actualCount);
      }*/

      if (VERBOSE) {
        System.out.println("TEST: use rewrite method " + q.getRewriteMethod());
      }
      assertEquals(actualCount, s.search(q, 1).totalHits);
    } finally {
      BooleanQuery.setMaxClauseCount(maxClauseCount);
    }

    // Test when min == max:
    List<String> randomTermsList = new ArrayList<>(randomTerms);
    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {
      String term = randomTermsList.get(random().nextInt(randomTermsList.size()));
      q = new TermRangeQuery("field", new BytesRef(term), new BytesRef(term), true, true);
      assertEquals(1, s.search(q, 1).totalHits);
    }

    r.close();
    w.close();
    dir.close();
  }


  /** Make sure auto prefix terms are used with PrefixQuery. */
  public void testPrefixQuery() throws Exception {

    List<String> prefixes = new ArrayList<>();
    for(int i=1;i<5;i++) {
      char[] chars = new char[i];
      Arrays.fill(chars, 'a');
      prefixes.add(new String(chars));
    }

    Set<String> randomTerms = new HashSet<>();
    int numTerms = atLeast(10000);
    while (randomTerms.size() < numTerms) {
      for(String prefix : prefixes) {
        randomTerms.add(prefix + TestUtil.randomRealisticUnicodeString(random()));
      }
    }

    int actualCount = 0;
    for(String term : randomTerms) {
      if (term.startsWith("aa")) {
        actualCount++;
      }
    }

    //System.out.println("actual count " + actualCount);

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    int minTermsInBlock = TestUtil.nextInt(random(), 2, 100);
    int maxTermsInBlock = Math.max(2, (minTermsInBlock-1)*2 + random().nextInt(100));

    // As long as this is never > actualCount, aa should always see at least one auto-prefix term:
    int minTermsAutoPrefix = TestUtil.nextInt(random(), 2, actualCount);
    int maxTermsAutoPrefix = random().nextBoolean() ? Math.max(2, (minTermsAutoPrefix-1)*2 + random().nextInt(100)) : Integer.MAX_VALUE;

    iwc.setCodec(TestUtil.alwaysPostingsFormat(new AutoPrefixPostingsFormat(minTermsInBlock, maxTermsInBlock,
                                                                            minTermsAutoPrefix, maxTermsAutoPrefix)));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for (String term : randomTerms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }

    w.forceMerge(1);
    IndexReader r = w.getReader();
    final Terms terms = MultiFields.getTerms(r, "field");
    IndexSearcher s = new IndexSearcher(r);
    final int finalActualCount = actualCount;
    PrefixQuery q = new PrefixQuery(new Term("field", "aa")) {
      public PrefixQuery checkTerms() throws IOException {
        TermsEnum termsEnum = getTermsEnum(terms, new AttributeSource());
        int count = 0;
        while (termsEnum.next() != null) {
          //System.out.println("got term: " + termsEnum.term().utf8ToString());
          count++;
        }

        // Auto-prefix term(s) should have kicked in, so we should have visited fewer than the total number of aa* terms:
        assertTrue(count < finalActualCount);

        return this;
      }
    }.checkTerms();

    int x = BooleanQuery.getMaxClauseCount();
    try {
      // TODO test with boolean rewrite as well once we can create term
      // queries on fake terms
      /*BooleanQuery.setMaxClauseCount(randomTerms.size());
      if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
      } else if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
      }*/

      assertEquals(actualCount, s.search(q, 1).totalHits);
    } finally {
      BooleanQuery.setMaxClauseCount(x);
    }

    r.close();
    w.close();
    dir.close();
  }
}
