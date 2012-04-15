package org.apache.lucene.index;

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

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocTermOrds.TermOrdsIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util._TestUtil;

// TODO:
//   - test w/ del docs
//   - test prefix
//   - test w/ cutoff
//   - crank docs way up so we get some merging sometimes

public class TestDocTermOrds extends LuceneTestCase {

  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    Field field = newField("field", "", TextField.TYPE_UNSTORED);
    doc.add(field);
    field.setStringValue("a b c");
    w.addDocument(doc);

    field.setStringValue("d e f");
    w.addDocument(doc);

    field.setStringValue("a f");
    w.addDocument(doc);
    
    final IndexReader r = w.getReader();
    w.close();

    final DocTermOrds dto = new DocTermOrds(SlowCompositeReaderWrapper.wrap(r), "field");

    TermOrdsIterator iter = dto.lookup(0, null);
    final int[] buffer = new int[5];
    assertEquals(3, iter.read(buffer));
    assertEquals(0, buffer[0]);
    assertEquals(1, buffer[1]);
    assertEquals(2, buffer[2]);

    iter = dto.lookup(1, iter);
    assertEquals(3, iter.read(buffer));
    assertEquals(3, buffer[0]);
    assertEquals(4, buffer[1]);
    assertEquals(5, buffer[2]);

    iter = dto.lookup(2, iter);
    assertEquals(2, iter.read(buffer));
    assertEquals(0, buffer[0]);
    assertEquals(5, buffer[1]);

    r.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    MockDirectoryWrapper dir = newDirectory();

    final int NUM_TERMS = atLeast(20);
    final Set<BytesRef> terms = new HashSet<BytesRef>();
    while(terms.size() < NUM_TERMS) {
      final String s = _TestUtil.randomRealisticUnicodeString(random());
      //final String s = _TestUtil.randomSimpleString(random);
      if (s.length() > 0) {
        terms.add(new BytesRef(s));
      }
    }
    final BytesRef[] termsArray = terms.toArray(new BytesRef[terms.size()]);
    Arrays.sort(termsArray);
    
    final int NUM_DOCS = atLeast(100);

    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));

    // Sometimes swap in codec that impls ord():
    if (random().nextInt(10) == 7) {
      // Make sure terms index has ords:
      Codec codec = _TestUtil.alwaysPostingsFormat(PostingsFormat.forName("Lucene40WithOrds"));
      conf.setCodec(codec);
    }
    
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, conf);

    final int[][] idToOrds = new int[NUM_DOCS][];
    final Set<Integer> ordsForDocSet = new HashSet<Integer>();

    for(int id=0;id<NUM_DOCS;id++) {
      Document doc = new Document();

      doc.add(new IntField("id", id));
      
      final int termCount = _TestUtil.nextInt(random(), 0, 20*RANDOM_MULTIPLIER);
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
        Field field = newField("field", termsArray[ord].utf8ToString(), StringField.TYPE_UNSTORED);
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

    for(IndexReader subR : r.getSequentialSubReaders()) {
      if (VERBOSE) {
        System.out.println("\nTEST: sub=" + subR);
      }
      verify((AtomicReader) subR, idToOrds, termsArray, null);
    }

    // Also test top-level reader: its enum does not support
    // ord, so this forces the OrdWrapper to run:
    if (VERBOSE) {
      System.out.println("TEST: top reader");
    }
    AtomicReader slowR = SlowCompositeReaderWrapper.wrap(r);
    verify(slowR, idToOrds, termsArray, null);

    FieldCache.DEFAULT.purge(slowR);

    r.close();
    dir.close();
  }

  public void testRandomWithPrefix() throws Exception {
    MockDirectoryWrapper dir = newDirectory();

    final Set<String> prefixes = new HashSet<String>();
    final int numPrefix = _TestUtil.nextInt(random(), 2, 7);
    if (VERBOSE) {
      System.out.println("TEST: use " + numPrefix + " prefixes");
    }
    while(prefixes.size() < numPrefix) {
      prefixes.add(_TestUtil.randomRealisticUnicodeString(random()));
      //prefixes.add(_TestUtil.randomSimpleString(random));
    }
    final String[] prefixesArray = prefixes.toArray(new String[prefixes.size()]);

    final int NUM_TERMS = atLeast(20);
    final Set<BytesRef> terms = new HashSet<BytesRef>();
    while(terms.size() < NUM_TERMS) {
      final String s = prefixesArray[random().nextInt(prefixesArray.length)] + _TestUtil.randomRealisticUnicodeString(random());
      //final String s = prefixesArray[random.nextInt(prefixesArray.length)] + _TestUtil.randomSimpleString(random);
      if (s.length() > 0) {
        terms.add(new BytesRef(s));
      }
    }
    final BytesRef[] termsArray = terms.toArray(new BytesRef[terms.size()]);
    Arrays.sort(termsArray);
    
    final int NUM_DOCS = atLeast(100);

    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));

    // Sometimes swap in codec that impls ord():
    if (random().nextInt(10) == 7) {
      Codec codec = _TestUtil.alwaysPostingsFormat(PostingsFormat.forName("Lucene40WithOrds"));
      conf.setCodec(codec);
    }
    
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, conf);

    final int[][] idToOrds = new int[NUM_DOCS][];
    final Set<Integer> ordsForDocSet = new HashSet<Integer>();

    for(int id=0;id<NUM_DOCS;id++) {
      Document doc = new Document();

      doc.add(new IntField("id", id));
      
      final int termCount = _TestUtil.nextInt(random(), 0, 20*RANDOM_MULTIPLIER);
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
        Field field = newField("field", termsArray[ord].utf8ToString(), StringField.TYPE_UNSTORED);
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
    
    AtomicReader slowR = SlowCompositeReaderWrapper.wrap(r);
    for(String prefix : prefixesArray) {

      final BytesRef prefixRef = prefix == null ? null : new BytesRef(prefix);

      final int[][] idToOrdsPrefix = new int[NUM_DOCS][];
      for(int id=0;id<NUM_DOCS;id++) {
        final int[] docOrds = idToOrds[id];
        final List<Integer> newOrds = new ArrayList<Integer>();
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

      for(IndexReader subR : r.getSequentialSubReaders()) {
        if (VERBOSE) {
          System.out.println("\nTEST: sub=" + subR);
        }
        verify((AtomicReader) subR, idToOrdsPrefix, termsArray, prefixRef);
      }

      // Also test top-level reader: its enum does not support
      // ord, so this forces the OrdWrapper to run:
      if (VERBOSE) {
        System.out.println("TEST: top reader");
      }
      verify(slowR, idToOrdsPrefix, termsArray, prefixRef);
    }

    FieldCache.DEFAULT.purge(slowR);

    r.close();
    dir.close();
  }

  private void verify(AtomicReader r, int[][] idToOrds, BytesRef[] termsArray, BytesRef prefixRef) throws Exception {

    final DocTermOrds dto = new DocTermOrds(r,
                                            "field",
                                            prefixRef,
                                            Integer.MAX_VALUE,
                                            _TestUtil.nextInt(random(), 2, 10));
                                            

    final int[] docIDToID = FieldCache.DEFAULT.getInts(r, "id", false);
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
    if (te == null) {
      if (prefixRef == null) {
        assertNull(MultiFields.getTerms(r, "field"));
      } else {
        Terms terms = MultiFields.getTerms(r, "field");
        if (terms != null) {
          TermsEnum termsEnum = terms.iterator(null);
          TermsEnum.SeekStatus result = termsEnum.seekCeil(prefixRef, false);
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

    TermOrdsIterator iter = null;
    final int[] buffer = new int[5];
    for(int docID=0;docID<r.maxDoc();docID++) {
      if (VERBOSE) {
        System.out.println("TEST: docID=" + docID + " of " + r.maxDoc() + " (id=" + docIDToID[docID] + ")");
      }
      iter = dto.lookup(docID, iter);
      final int[] answers = idToOrds[docIDToID[docID]];
      int upto = 0;
      while(true) {
        final int chunk = iter.read(buffer);
        for(int idx=0;idx<chunk;idx++) {
          te.seekExact((long) buffer[idx]);
          final BytesRef expected = termsArray[answers[upto++]];
          if (VERBOSE) {
            System.out.println("  exp=" + expected.utf8ToString() + " actual=" + te.term().utf8ToString());
          }
          assertEquals("expected=" + expected.utf8ToString() + " actual=" + te.term().utf8ToString() + " ord=" + buffer[idx], expected, te.term());
        }
        
        if (chunk < buffer.length) {
          assertEquals(answers.length, upto);
          break;
        }
      }
    }
  }
}
