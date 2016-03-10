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
package org.apache.lucene.uninverting;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LegacyIntField;
import org.apache.lucene.document.LegacyLongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LegacyNumericUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

// TODO:
//   - test w/ del docs
//   - test prefix
//   - test w/ cutoff
//   - crank docs way up so we get some merging sometimes

public class TestDocTermOrds extends LuceneTestCase {

  public void testEmptyIndex() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    iw.close();
    
    final DirectoryReader ir = DirectoryReader.open(dir);
    TestUtil.checkReader(ir);
    
    final LeafReader composite = SlowCompositeReaderWrapper.wrap(ir);
    TestUtil.checkReader(composite);
    
    // check the leaves
    // (normally there are none for an empty index, so this is really just future
    // proofing in case that changes for some reason)
    for (LeafReaderContext rc : ir.leaves()) {
      final LeafReader r = rc.reader();
      final DocTermOrds dto = new DocTermOrds(r, r.getLiveDocs(), "any_field");
      assertNull("OrdTermsEnum should be null (leaf)", dto.getOrdTermsEnum(r));
      assertEquals("iterator should be empty (leaf)", 0, dto.iterator(r).getValueCount());
    }

    // check the composite 
    final DocTermOrds dto = new DocTermOrds(composite, composite.getLiveDocs(), "any_field");
    assertNull("OrdTermsEnum should be null (composite)", dto.getOrdTermsEnum(composite));
    assertEquals("iterator should be empty (composite)", 0, dto.iterator(composite).getValueCount());

    ir.close();
    dir.close();
  }

  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    Field field = newTextField("field", "", Field.Store.NO);
    doc.add(field);
    field.setStringValue("a b c");
    w.addDocument(doc);

    field.setStringValue("d e f");
    w.addDocument(doc);

    field.setStringValue("a f");
    w.addDocument(doc);
    
    final IndexReader r = w.getReader();
    w.close();

    final LeafReader ar = SlowCompositeReaderWrapper.wrap(r);
    TestUtil.checkReader(ar);
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

    final int[][] idToOrds = new int[NUM_DOCS][];
    final Set<Integer> ordsForDocSet = new HashSet<>();

    for(int id=0;id<NUM_DOCS;id++) {
      Document doc = new Document();

      doc.add(new LegacyIntField("id", id, Field.Store.YES));
      
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
        Field field = newStringField("field", termsArray[ord].utf8ToString(), Field.Store.NO);
        if (VERBOSE) {
          System.out.println("  f=" + termsArray[ord].utf8ToString());
        }
        doc.add(field);
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
    TestUtil.checkReader(slowR);
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

    final int[][] idToOrds = new int[NUM_DOCS][];
    final Set<Integer> ordsForDocSet = new HashSet<>();

    for(int id=0;id<NUM_DOCS;id++) {
      Document doc = new Document();

      doc.add(new LegacyIntField("id", id, Field.Store.YES));
      
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
        Field field = newStringField("field", termsArray[ord].utf8ToString(), Field.Store.NO);
        if (VERBOSE) {
          System.out.println("  f=" + termsArray[ord].utf8ToString());
        }
        doc.add(field);
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
    TestUtil.checkReader(slowR);
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
                                            

    final NumericDocValues docIDToID = FieldCache.DEFAULT.getNumerics(r, "id", FieldCache.LEGACY_INT_PARSER, false);
    /*
      for(int docID=0;docID<subR.maxDoc();docID++) {
      System.out.println("  docID=" + docID + " id=" + docIDToID[docID]);
      }
    */

    if (VERBOSE) {
      System.out.println("TEST: verify prefix=" + (prefixRef==null ? "null" : prefixRef.utf8ToString()));
      System.out.println("TEST: all TERMS:");
      TermsEnum allTE = MultiFields.getTerms(r, "field").iterator();
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
          TermsEnum termsEnum = terms.iterator();
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
    
    Document doc = new Document();
    doc.add(newStringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(newStringField("foo", "baz", Field.Store.NO));
    // we need a second value for a doc, or we don't actually test DocTermOrds!
    doc.add(newStringField("foo", "car", Field.Store.NO));
    iw.addDocument(doc);
    
    DirectoryReader r1 = DirectoryReader.open(iw);
    
    iw.deleteDocuments(new Term("foo", "baz"));
    DirectoryReader r2 = DirectoryReader.open(iw);
    
    FieldCache.DEFAULT.getDocTermOrds(getOnlyLeafReader(r2), "foo", null);
    
    SortedSetDocValues v = FieldCache.DEFAULT.getDocTermOrds(getOnlyLeafReader(r1), "foo", null);
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
    
    Document doc = new Document();
    doc.add(new LegacyIntField("foo", 5, Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LegacyIntField("foo", 5, Field.Store.NO));
    doc.add(new LegacyIntField("foo", -3, Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = DirectoryReader.open(dir);
    LeafReader ar = getOnlyLeafReader(ir);
    
    SortedSetDocValues v = FieldCache.DEFAULT.getDocTermOrds(ar, "foo", FieldCache.INT32_TERM_PREFIX);
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(-3, LegacyNumericUtils.prefixCodedToInt(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, LegacyNumericUtils.prefixCodedToInt(value));
    
    ir.close();
    dir.close();
  }
  
  public void testNumericEncoded64() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new LegacyLongField("foo", 5, Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LegacyLongField("foo", 5, Field.Store.NO));
    doc.add(new LegacyLongField("foo", -3, Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = DirectoryReader.open(dir);
    LeafReader ar = getOnlyLeafReader(ir);
    
    SortedSetDocValues v = FieldCache.DEFAULT.getDocTermOrds(ar, "foo", FieldCache.INT64_TERM_PREFIX);
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(-3, LegacyNumericUtils.prefixCodedToLong(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, LegacyNumericUtils.prefixCodedToLong(value));
    
    ir.close();
    dir.close();
  }
  
  public void testSortedTermsEnum() throws IOException {
    Directory directory = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
    iwconfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);
    
    Document doc = new Document();
    doc.add(new StringField("field", "hello", Field.Store.NO));
    iwriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new StringField("field", "world", Field.Store.NO));
    // we need a second value for a doc, or we don't actually test DocTermOrds!
    doc.add(new StringField("field", "hello", Field.Store.NO));
    iwriter.addDocument(doc);

    doc = new Document();
    doc.add(new StringField("field", "beer", Field.Store.NO));
    iwriter.addDocument(doc);
    iwriter.forceMerge(1);
    
    DirectoryReader ireader = iwriter.getReader();
    iwriter.close();

    LeafReader ar = getOnlyLeafReader(ireader);
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
    
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new StringField("foo", "baz", Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new StringField("foo", "baz", Field.Store.NO));
    doc.add(new StringField("foo", "baz", Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = DirectoryReader.open(dir);
    LeafReader ar = getOnlyLeafReader(ir);
    
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
