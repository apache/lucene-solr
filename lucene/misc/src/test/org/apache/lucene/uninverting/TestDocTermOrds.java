package org.apache.lucene.uninverting;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

// TODO:
//   - test w/ del docs
//   - test prefix
//   - test w/ cutoff
//   - crank docs way up so we get some merging sometimes

public class TestDocTermOrds extends LuceneTestCase {

  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    Document doc = w.newDocument();
    doc.addLargeText("field", "a b c");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addLargeText("field", "d e f");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addLargeText("field", "a f");
    w.addDocument(doc);
    
    final IndexReader r = w.getReader();
    w.close();

    final LeafReader ar = SlowCompositeReaderWrapper.wrap(r);
    final DocTermOrds dto = new DocTermOrds(ar, ar.getLiveDocs(), "field");
    SortedSetDocValues iter = dto.iterator(ar);
    
    iter.setDocument(0);
    assertEquals(0, iter.nextOrd());
    assertEquals(1, iter.nextOrd());
    assertEquals(2, iter.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, iter.nextOrd());
    
    iter.setDocument(1);
    assertEquals(3, iter.nextOrd());
    assertEquals(4, iter.nextOrd());
    assertEquals(5, iter.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, iter.nextOrd());

    iter.setDocument(2);
    assertEquals(0, iter.nextOrd());
    assertEquals(5, iter.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, iter.nextOrd());

    r.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    Directory dir = newDirectory();

    final int NUM_TERMS = atLeast(20);
    final Set<BytesRef> terms = new HashSet<>();
    while(terms.size() < NUM_TERMS) {
      final String s = TestUtil.randomRealisticUnicodeString(random());
      //final String s = _TestUtil.randomSimpleString(random);
      if (s.length() > 0) {
        terms.add(new BytesRef(s));
      }
    }
    final BytesRef[] termsArray = terms.toArray(new BytesRef[terms.size()]);
    Arrays.sort(termsArray);
    
    final int NUM_DOCS = atLeast(100);

    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));

    // Sometimes swap in codec that impls ord():
    if (random().nextInt(10) == 7) {
      // Make sure terms index has ords:
      Codec codec = TestUtil.alwaysPostingsFormat(TestUtil.getPostingsFormatWithOrds(random()));
      conf.setCodec(codec);
    }
    
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, conf);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableSorting("id");
    fieldTypes.disableSorting("field");
    fieldTypes.setMultiValued("field");

    final int[][] idToOrds = new int[NUM_DOCS][];
    final Set<Integer> ordsForDocSet = new HashSet<>();

    for(int id=0;id<NUM_DOCS;id++) {
      Document doc = w.newDocument();
      doc.addInt("id", id);
      
      final int termCount = TestUtil.nextInt(random(), 0, 20 * RANDOM_MULTIPLIER);
      while(ordsForDocSet.size() < termCount) {
        ordsForDocSet.add(random().nextInt(termsArray.length));
      }
      final int[] ordsForDoc = new int[termCount];
      int upto = 0;
      if (VERBOSE) {
        System.out.println("TEST: doc id=" + id);
      }
      for(int ord : ordsForDocSet) {
        ordsForDoc[upto++] = ord;
        if (VERBOSE) {
          System.out.println("  f=" + termsArray[ord].utf8ToString());
        }
        doc.addAtom("field", termsArray[ord].utf8ToString());
      }
      ordsForDocSet.clear();
      Arrays.sort(ordsForDoc);
      idToOrds[id] = ordsForDoc;
      w.addDocument(doc);
    }
    
    final DirectoryReader r = w.getReader();
    w.close();

    if (VERBOSE) {
      System.out.println("TEST: reader=" + r);
    }

    for(LeafReaderContext ctx : r.leaves()) {
      if (VERBOSE) {
        System.out.println("\nTEST: sub=" + ctx.reader());
      }
      verify(ctx.reader(), idToOrds, termsArray, null);
    }

    // Also test top-level reader: its enum does not support
    // ord, so this forces the OrdWrapper to run:
    if (VERBOSE) {
      System.out.println("TEST: top reader");
    }
    LeafReader slowR = SlowCompositeReaderWrapper.wrap(r);
    verify(slowR, idToOrds, termsArray, null);

    FieldCache.DEFAULT.purgeByCacheKey(slowR.getCoreCacheKey());

    r.close();
    dir.close();
  }

  public void testRandomWithPrefix() throws Exception {
    Directory dir = newDirectory();

    final Set<String> prefixes = new HashSet<>();
    final int numPrefix = TestUtil.nextInt(random(), 2, 7);
    if (VERBOSE) {
      System.out.println("TEST: use " + numPrefix + " prefixes");
    }
    while(prefixes.size() < numPrefix) {
      prefixes.add(TestUtil.randomRealisticUnicodeString(random()));
      //prefixes.add(_TestUtil.randomSimpleString(random));
    }
    final String[] prefixesArray = prefixes.toArray(new String[prefixes.size()]);

    final int NUM_TERMS = atLeast(20);
    final Set<BytesRef> terms = new HashSet<>();
    while(terms.size() < NUM_TERMS) {
      final String s = prefixesArray[random().nextInt(prefixesArray.length)] + TestUtil.randomRealisticUnicodeString(random());
      //final String s = prefixesArray[random.nextInt(prefixesArray.length)] + _TestUtil.randomSimpleString(random);
      if (s.length() > 0) {
        terms.add(new BytesRef(s));
      }
    }
    final BytesRef[] termsArray = terms.toArray(new BytesRef[terms.size()]);
    Arrays.sort(termsArray);
    
    final int NUM_DOCS = atLeast(100);

    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));

    // Sometimes swap in codec that impls ord():
    if (random().nextInt(10) == 7) {
      Codec codec = TestUtil.alwaysPostingsFormat(TestUtil.getPostingsFormatWithOrds(random()));
      conf.setCodec(codec);
    }
    
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, conf);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableSorting("id");
    fieldTypes.disableSorting("field");
    fieldTypes.setMultiValued("field");

    final int[][] idToOrds = new int[NUM_DOCS][];
    final Set<Integer> ordsForDocSet = new HashSet<>();

    for(int id=0;id<NUM_DOCS;id++) {
      Document doc = w.newDocument();

      doc.addInt("id", id);
      
      final int termCount = TestUtil.nextInt(random(), 0, 20 * RANDOM_MULTIPLIER);
      while(ordsForDocSet.size() < termCount) {
        ordsForDocSet.add(random().nextInt(termsArray.length));
      }
      final int[] ordsForDoc = new int[termCount];
      int upto = 0;
      if (VERBOSE) {
        System.out.println("TEST: doc id=" + id);
      }
      for(int ord : ordsForDocSet) {
        ordsForDoc[upto++] = ord;
        if (VERBOSE) {
          System.out.println("  f=" + termsArray[ord].utf8ToString());
        }
        doc.addAtom("field", termsArray[ord].utf8ToString());
      }
      ordsForDocSet.clear();
      Arrays.sort(ordsForDoc);
      idToOrds[id] = ordsForDoc;
      w.addDocument(doc);
    }
    
    final DirectoryReader r = w.getReader();
    w.close();

    if (VERBOSE) {
      System.out.println("TEST: reader=" + r);
    }
    
    LeafReader slowR = SlowCompositeReaderWrapper.wrap(r);
    for(String prefix : prefixesArray) {

      final BytesRef prefixRef = prefix == null ? null : new BytesRef(prefix);

      final int[][] idToOrdsPrefix = new int[NUM_DOCS][];
      for(int id=0;id<NUM_DOCS;id++) {
        final int[] docOrds = idToOrds[id];
        final List<Integer> newOrds = new ArrayList<>();
        for(int ord : idToOrds[id]) {
          if (StringHelper.startsWith(termsArray[ord], prefixRef)) {
            newOrds.add(ord);
          }
        }
        final int[] newOrdsArray = new int[newOrds.size()];
        int upto = 0;
        for(int ord : newOrds) {
          newOrdsArray[upto++] = ord;
        }
        idToOrdsPrefix[id] = newOrdsArray;
      }

      for(LeafReaderContext ctx : r.leaves()) {
        if (VERBOSE) {
          System.out.println("\nTEST: sub=" + ctx.reader());
        }
        verify(ctx.reader(), idToOrdsPrefix, termsArray, prefixRef);
      }

      // Also test top-level reader: its enum does not support
      // ord, so this forces the OrdWrapper to run:
      if (VERBOSE) {
        System.out.println("TEST: top reader");
      }
      verify(slowR, idToOrdsPrefix, termsArray, prefixRef);
    }

    FieldCache.DEFAULT.purgeByCacheKey(slowR.getCoreCacheKey());

    r.close();
    dir.close();
  }

  private void verify(LeafReader r, int[][] idToOrds, BytesRef[] termsArray, BytesRef prefixRef) throws Exception {

    final DocTermOrds dto = new DocTermOrds(r, r.getLiveDocs(),
                                            "field",
                                            prefixRef,
                                            Integer.MAX_VALUE,
                                            TestUtil.nextInt(random(), 2, 10));
                                            

    final NumericDocValues docIDToID = FieldCache.DEFAULT.getNumerics(r, "id", FieldCache.DOCUMENT2_INT_PARSER, false);
    /*
      for(int docID=0;docID<subR.maxDoc();docID++) {
      System.out.println("  docID=" + docID + " id=" + docIDToID[docID]);
      }
    */

    if (VERBOSE) {
      System.out.println("TEST: verify prefix=" + (prefixRef==null ? "null" : prefixRef.utf8ToString()));
      System.out.println("TEST: all TERMS:");
      TermsEnum allTE = MultiFields.getTerms(r, "field").iterator(null);
      int ord = 0;
      while(allTE.next() != null) {
        System.out.println("  ord=" + (ord++) + " term=" + allTE.term().utf8ToString());
      }
    }

    //final TermsEnum te = subR.fields().terms("field").iterator();
    final TermsEnum te = dto.getOrdTermsEnum(r);
    if (dto.numTerms() == 0) {
      if (prefixRef == null) {
        assertNull(MultiFields.getTerms(r, "field"));
      } else {
        Terms terms = MultiFields.getTerms(r, "field");
        if (terms != null) {
          TermsEnum termsEnum = terms.iterator(null);
          TermsEnum.SeekStatus result = termsEnum.seekCeil(prefixRef);
          if (result != TermsEnum.SeekStatus.END) {
            assertFalse("term=" + termsEnum.term().utf8ToString() + " matches prefix=" + prefixRef.utf8ToString(), StringHelper.startsWith(termsEnum.term(), prefixRef));
          } else {
            // ok
          }
        } else {
          // ok
        }
      }
      return;
    }

    if (VERBOSE) {
      System.out.println("TEST: TERMS:");
      te.seekExact(0);
      while(true) {
        System.out.println("  ord=" + te.ord() + " term=" + te.term().utf8ToString());
        if (te.next() == null) {
          break;
        }
      }
    }

    SortedSetDocValues iter = dto.iterator(r);
    for(int docID=0;docID<r.maxDoc();docID++) {
      if (VERBOSE) {
        System.out.println("TEST: docID=" + docID + " of " + r.maxDoc() + " (id=" + docIDToID.get(docID) + ")");
      }
      iter.setDocument(docID);
      final int[] answers = idToOrds[(int) docIDToID.get(docID)];
      int upto = 0;
      long ord;
      while ((ord = iter.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        te.seekExact(ord);
        final BytesRef expected = termsArray[answers[upto++]];
        if (VERBOSE) {
          System.out.println("  exp=" + expected.utf8ToString() + " actual=" + te.term().utf8ToString());
        }
        assertEquals("expected=" + expected.utf8ToString() + " actual=" + te.term().utf8ToString() + " ord=" + ord, expected, te.term());
      }
      assertEquals(answers.length, upto);
    }
  }
  
  public void testBackToTheFuture() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.setMultiValued("foo");

    Document doc = iw.newDocument();
    doc.addAtom("foo", "bar");
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addAtom("foo", "baz");
    // we need a second value for a doc, or we don't actually test DocTermOrds!
    doc.addAtom("foo", "car");
    iw.addDocument(doc);
    
    DirectoryReader r1 = DirectoryReader.open(iw, true);
    
    iw.deleteDocuments(new Term("foo", "baz"));
    DirectoryReader r2 = DirectoryReader.open(iw, true);
    
    FieldCache.DEFAULT.getDocTermOrds(getOnlySegmentReader(r2), "foo", null);
    
    SortedSetDocValues v = FieldCache.DEFAULT.getDocTermOrds(getOnlySegmentReader(r1), "foo", null);
    assertEquals(3, v.getValueCount());
    v.setDocument(1);
    assertEquals(1, v.nextOrd());
    
    iw.close();
    r1.close();
    r2.close();
    dir.close();
  }
  
  public void testNumericEncoded32() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.disableSorting("foo");
    fieldTypes.setMultiValued("foo");

    Document doc = iw.newDocument();
    doc.addInt("foo", 5);
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addInt("foo", 5);
    doc.addInt("foo", -3);
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = DirectoryReader.open(dir);
    LeafReader ar = getOnlySegmentReader(ir);
    
    SortedSetDocValues v = FieldCache.DEFAULT.getDocTermOrds(ar, "foo", null);
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(-3, Document.bytesToInt(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, Document.bytesToInt(value));
    
    ir.close();
    dir.close();
  }
  
  public void testNumericEncoded64() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.disableSorting("foo");
    fieldTypes.setMultiValued("foo");
    
    Document doc = iw.newDocument();
    doc.addLong("foo", 5);
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addLong("foo", 5);
    doc.addLong("foo", -3);
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = DirectoryReader.open(dir);
    LeafReader ar = getOnlySegmentReader(ir);
    
    SortedSetDocValues v = FieldCache.DEFAULT.getDocTermOrds(ar, "foo", null);
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(-3, Document.bytesToLong(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, Document.bytesToLong(value));
    
    ir.close();
    dir.close();
  }
  
  public void testSortedTermsEnum() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    FieldTypes fieldTypes = iwriter.getFieldTypes();
    fieldTypes.setMultiValued("field");
    
    Document doc = iwriter.newDocument();
    doc.addAtom("field", "hello");
    iwriter.addDocument(doc);
    
    doc = iwriter.newDocument();
    doc.addAtom("field", "world");
    // we need a second value for a doc, or we don't actually test DocTermOrds!
    doc.addAtom("field", "hello");
    iwriter.addDocument(doc);

    doc = iwriter.newDocument();
    doc.addAtom("field", "beer");
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    LeafReader ar = getOnlySegmentReader(ireader);
    SortedSetDocValues dv = FieldCache.DEFAULT.getDocTermOrds(ar, "field", null);
    assertEquals(3, dv.getValueCount());
    
    TermsEnum termsEnum = dv.termsEnum();
    
    // next()
    assertEquals("beer", termsEnum.next().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertEquals("hello", termsEnum.next().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertEquals("world", termsEnum.next().utf8ToString());
    assertEquals(2, termsEnum.ord());
    
    // seekCeil()
    assertEquals(SeekStatus.NOT_FOUND, termsEnum.seekCeil(new BytesRef("ha!")));
    assertEquals("hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef("beer")));
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertEquals(SeekStatus.END, termsEnum.seekCeil(new BytesRef("zzz")));
    
    // seekExact()
    assertTrue(termsEnum.seekExact(new BytesRef("beer")));
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    assertTrue(termsEnum.seekExact(new BytesRef("hello")));
    assertEquals("hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    assertTrue(termsEnum.seekExact(new BytesRef("world")));
    assertEquals("world", termsEnum.term().utf8ToString());
    assertEquals(2, termsEnum.ord());
    assertFalse(termsEnum.seekExact(new BytesRef("bogus")));
    
    // seek(ord)
    termsEnum.seekExact(0);
    assertEquals("beer", termsEnum.term().utf8ToString());
    assertEquals(0, termsEnum.ord());
    termsEnum.seekExact(1);
    assertEquals("hello", termsEnum.term().utf8ToString());
    assertEquals(1, termsEnum.ord());
    termsEnum.seekExact(2);
    assertEquals("world", termsEnum.term().utf8ToString());
    assertEquals(2, termsEnum.ord());
    
    // lookupTerm(BytesRef) 
    assertEquals(-1, dv.lookupTerm(new BytesRef("apple")));
    assertEquals(0, dv.lookupTerm(new BytesRef("beer")));
    assertEquals(-2, dv.lookupTerm(new BytesRef("car")));
    assertEquals(1, dv.lookupTerm(new BytesRef("hello")));
    assertEquals(-3, dv.lookupTerm(new BytesRef("matter")));
    assertEquals(2, dv.lookupTerm(new BytesRef("world")));
    assertEquals(-4, dv.lookupTerm(new BytesRef("zany")));

    ireader.close();
    directory.close();
  }
  
  public void testActuallySingleValued() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwconfig =  newIndexWriterConfig(null);
    iwconfig.setMergePolicy(newLogMergePolicy());
    IndexWriter iw = new IndexWriter(dir, iwconfig);
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.setMultiValued("foo");
    fieldTypes.disableSorting("foo");
    
    Document doc = iw.newDocument();
    doc.addAtom("foo", "bar");
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addAtom("foo", "baz");
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    iw.addDocument(doc);
    
    doc = iw.newDocument();
    doc.addAtom("foo", "baz");
    doc.addAtom("foo", "baz");
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = DirectoryReader.open(dir);
    LeafReader ar = getOnlySegmentReader(ir);
    
    SortedSetDocValues v = FieldCache.DEFAULT.getDocTermOrds(ar, "foo", null);
    assertNotNull(DocValues.unwrapSingleton(v)); // actually a single-valued field
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(0, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(2);
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(3);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals("bar", value.utf8ToString());
    
    value = v.lookupOrd(1);
    assertEquals("baz", value.utf8ToString());
    
    ir.close();
    dir.close();
  }
}
