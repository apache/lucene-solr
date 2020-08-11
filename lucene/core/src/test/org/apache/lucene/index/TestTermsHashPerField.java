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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.LuceneTestCase;

public class TestTermsHashPerField extends LuceneTestCase  {

  private static TermsHashPerField createNewHash(AtomicInteger newCalled, AtomicInteger addCalled) {
    IntBlockPool intBlockPool = new IntBlockPool();
    ByteBlockPool byteBlockPool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    ByteBlockPool termBlockPool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());

    TermsHashPerField hash = new TermsHashPerField(1, intBlockPool, byteBlockPool, termBlockPool, Counter.newCounter(),
        null, "testfield", IndexOptions.DOCS_AND_FREQS) {

      private FreqProxTermsWriterPerField.FreqProxPostingsArray freqProxPostingsArray;

      @Override
      void newTerm(int termID, int docID) {
        newCalled.incrementAndGet();
        FreqProxTermsWriterPerField.FreqProxPostingsArray postings = freqProxPostingsArray;
        postings.lastDocIDs[termID] = docID;
        postings.lastDocCodes[termID] = docID << 1;
        postings.termFreqs[termID] = 1;
      }

      @Override
      void addTerm(int termID, int docID) {
        addCalled.incrementAndGet();
        FreqProxTermsWriterPerField.FreqProxPostingsArray postings = freqProxPostingsArray;
        if (docID != postings.lastDocIDs[termID]) {
          if (1 == postings.termFreqs[termID]) {
            writeVInt(0, postings.lastDocCodes[termID]|1);
          } else {
            writeVInt(0, postings.lastDocCodes[termID]);
            writeVInt(0, postings.termFreqs[termID]);
          }
          postings.termFreqs[termID] = 1;
          postings.lastDocCodes[termID] = (docID - postings.lastDocIDs[termID]) << 1;
          postings.lastDocIDs[termID] = docID;
        } else {
          postings.termFreqs[termID] = Math.addExact(postings.termFreqs[termID], 1);
        }
      }

      @Override
      void newPostingsArray() {
        freqProxPostingsArray = (FreqProxTermsWriterPerField.FreqProxPostingsArray) postingsArray;
      }

      @Override
      ParallelPostingsArray createPostingsArray(int size) {
        return new FreqProxTermsWriterPerField.FreqProxPostingsArray(size, true, false, false);
      }
    };
    return hash;
  }

  boolean assertDocAndFreq(ByteSliceReader reader, FreqProxTermsWriterPerField.FreqProxPostingsArray postingsArray, int prevDoc, int termId, int doc, int frequency) throws IOException {
    int docId = prevDoc;
    int freq;
    boolean eof = reader.eof();
    if (eof) {
      docId = postingsArray.lastDocIDs[termId];
      freq = postingsArray.termFreqs[termId];
    } else {
      int code = reader.readVInt();
      docId += code >>> 1;
      if ((code & 1) != 0) {
        freq = 1;
      } else {
        freq = reader.readVInt();
      }
    }
    assertEquals("docID mismatch eof: " + eof, doc, docId);
    assertEquals("freq mismatch eof: " + eof, frequency, freq);
    return eof;
  }

  public void testAddAndUpdateTerm() throws IOException {
    AtomicInteger newCalled = new AtomicInteger(0);
    AtomicInteger addCalled = new AtomicInteger(0);
    TermsHashPerField hash = createNewHash(newCalled, addCalled);
    hash.start(null, true);

    hash.add(new BytesRef("start"), 0); // tid = 0;
    hash.add(new BytesRef("foo"), 0); // tid = 1;
    hash.add(new BytesRef("bar"), 0); // tid = 2;
    hash.finish();
    hash.add(new BytesRef("bar"), 1);
    hash.add(new BytesRef("foobar"), 1); // tid = 3;
    hash.add(new BytesRef("bar"), 1);
    hash.add(new BytesRef("bar"), 1);
    hash.add(new BytesRef("foobar"), 1);
    hash.add(new BytesRef("verylongfoobarbaz"), 1); // tid = 4;
    hash.finish();
    hash.add(new BytesRef("verylongfoobarbaz"), 2);
    hash.add(new BytesRef("boom"), 2); // tid = 5;
    hash.finish();
    hash.add(new BytesRef("verylongfoobarbaz"), 3);
    hash.add(new BytesRef("end"), 3); // tid = 6;
    hash.finish();

    assertEquals(7, newCalled.get());
    assertEquals(6, addCalled.get());
    final ByteSliceReader reader = new ByteSliceReader();
    hash.initReader(reader, 0, 0);
    assertTrue(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 0, 0, 0, 1));
    hash.initReader(reader, 1, 0);
    assertTrue(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 0, 1, 0, 1));
    hash.initReader(reader, 2, 0);
    assertFalse(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 0, 2, 0, 1));
    assertTrue(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 2, 2, 1, 3));
    hash.initReader(reader, 3, 0);
    assertTrue(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 0, 3, 1, 2));
    hash.initReader(reader, 4, 0);
    assertFalse(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 0, 4, 1, 1));
    assertFalse(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 1, 4, 2, 1));
    assertTrue(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 2, 4, 3, 1));
    hash.initReader(reader, 5, 0);
    assertTrue(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 0, 5, 2, 1));
    hash.initReader(reader, 6, 0);
    assertTrue(assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray, 0, 6, 3, 1));
  }

  public void testAddAndUpdateRandom() throws IOException {
    AtomicInteger newCalled = new AtomicInteger(0);
    AtomicInteger addCalled = new AtomicInteger(0);
    TermsHashPerField hash = createNewHash(newCalled, addCalled);
    hash.start(null, true);
    class Posting {
      int termId = -1;
      final TreeMap<Integer, Integer> docAndFreq = new TreeMap<>();
    }
    Map<BytesRef, Posting> postingMap = new HashMap<>();
    int numStrings = 1 + random().nextInt(200);
    for (int i = 0; i < numStrings; i++) {
      String randomString = RandomStrings.randomRealisticUnicodeOfCodepointLengthBetween(random(), 1, 10);
      postingMap.putIfAbsent(new BytesRef(randomString), new Posting());
    }
    List<BytesRef> bytesRefs = Arrays.asList(postingMap.keySet().toArray(new BytesRef[0]));
    Collections.sort(bytesRefs);
    int numDocs = 1 + random().nextInt(200);
    int termOrd = 0;
    for (int i = 0; i < numDocs; i++) {
      int numTerms = 1 + random().nextInt(200);
      int doc = i;
      for (int j = 0; i < numTerms; i++) {
        BytesRef ref = RandomPicks.randomFrom(random(), bytesRefs);
        Posting posting = postingMap.get(ref);
        if (posting.termId == -1) {
          posting.termId = termOrd++;
        }
        posting.docAndFreq.putIfAbsent(doc, 0);
        posting.docAndFreq.compute(doc, (key, oldVal) -> oldVal+1);
        hash.add(ref, doc);
      }
      hash.finish();
    }
    List<Posting> values = postingMap.values().stream().filter( x -> x.termId != -1)
        .collect(Collectors.toList());
    Collections.shuffle(values, random()); // term order doesn't matter
    final ByteSliceReader reader = new ByteSliceReader();
    for (Posting p : values) {
      hash.initReader(reader, p.termId, 0);
      boolean eof = false;
      int prefDoc = 0;
      for (Map.Entry<Integer, Integer> entry : p.docAndFreq.entrySet()) {
        assertFalse("the reader must not be EOF here", eof);
        eof = assertDocAndFreq(reader, (FreqProxTermsWriterPerField.FreqProxPostingsArray) hash.postingsArray,
            prefDoc, p.termId, entry.getKey(), entry.getValue());
        prefDoc = entry.getKey();
      }
      assertTrue("the last posting must be EOF on the reader", eof);
    }
  }
}
