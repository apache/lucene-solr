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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.IntToLongFunction;
import java.util.stream.Collectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.AutomatonTestUtil.RandomAcceptedStrings;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/** Helper class extracted from BasePostingsFormatTestCase to exercise a postings format. */
public class RandomPostingsTester {

  private static final IntToLongFunction DOC_TO_NORM = doc -> 1 + (doc & 0x0f);
  private static final long MAX_NORM = 0x10;

  /** Which features to test. */
  public enum Option {
    // Sometimes use .advance():
    SKIPPING,

    // Sometimes reuse the PostingsEnum across terms:
    REUSE_ENUMS,

    // Sometimes pass non-null live docs:
    LIVE_DOCS,

    // Sometimes seek to term using previously saved TermState:
    TERM_STATE,

    // Sometimes don't fully consume docs from the enum
    PARTIAL_DOC_CONSUME,

    // Sometimes don't fully consume positions at each doc
    PARTIAL_POS_CONSUME,

    // Sometimes check payloads
    PAYLOADS,

    // Test w/ multiple threads
    THREADS
  };

  private long totalPostings;
  private long totalPayloadBytes;

  // Holds all postings:
  private Map<String,SortedMap<BytesRef,SeedAndOrd>> fields;

  private FieldInfos fieldInfos;

  List<FieldAndTerm> allTerms;
  private int maxDoc;

  final Random random;

  public RandomPostingsTester(Random random) throws IOException {
    fields = new TreeMap<>();

    this.random = random;

    final int numFields = TestUtil.nextInt(random, 1, 5);
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: " + numFields + " fields");
    }
    maxDoc = 0;

    FieldInfo[] fieldInfoArray = new FieldInfo[numFields];
    int fieldUpto = 0;
    while (fieldUpto < numFields) {
      String field = TestUtil.randomSimpleString(random);
      if (fields.containsKey(field)) {
        continue;
      }

      fieldInfoArray[fieldUpto] = new FieldInfo(field, fieldUpto, false, false, true,
                                                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                                                DocValuesType.NONE, -1, new HashMap<>(),
                                                0, 0, 0, false);
      fieldUpto++;

      SortedMap<BytesRef,SeedAndOrd> postings = new TreeMap<>();
      fields.put(field, postings);
      Set<String> seenTerms = new HashSet<>();

      int numTerms;
      if (random.nextInt(10) == 7) {
        numTerms = LuceneTestCase.atLeast(random, 50);
      } else {
        numTerms = TestUtil.nextInt(random, 2, 20);
      }

      while (postings.size() < numTerms) {
        int termUpto = postings.size();
        // Cannot contain surrogates else default Java string sort order (by UTF16 code unit) is different from Lucene:
        String term = TestUtil.randomSimpleString(random);
        if (seenTerms.contains(term)) {
          continue;
        }
        seenTerms.add(term);

        if (LuceneTestCase.TEST_NIGHTLY && termUpto == 0 && fieldUpto == 1) {
          // Make 1 big term:
          term = "big_" + term;
        } else if (termUpto == 1 && fieldUpto == 1) {
          // Make 1 medium term:
          term = "medium_" + term;
        } else if (random.nextBoolean()) {
          // Low freq term:
          term = "low_" + term;
        } else {
          // Very low freq term (don't multiply by RANDOM_MULTIPLIER):
          term = "verylow_" + term;
        }

        long termSeed = random.nextLong();
        postings.put(new BytesRef(term), new SeedAndOrd(termSeed));

        // NOTE: sort of silly: we enum all the docs just to
        // get the maxDoc
        PostingsEnum postingsEnum = getSeedPostings(term, termSeed, IndexOptions.DOCS, true);
        int doc;
        int lastDoc = 0;
        while((doc = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
          lastDoc = doc;
        }
        maxDoc = Math.max(lastDoc, maxDoc);
      }

      // assign ords
      long ord = 0;
      for(SeedAndOrd ent : postings.values()) {
        ent.ord = ord++;
      }
    }

    fieldInfos = new FieldInfos(fieldInfoArray);

    // It's the count, not the last docID:
    maxDoc++;

    allTerms = new ArrayList<>();
    for(Map.Entry<String,SortedMap<BytesRef,SeedAndOrd>> fieldEnt : fields.entrySet()) {
      String field = fieldEnt.getKey();
      long ord = 0;
      for(Map.Entry<BytesRef,SeedAndOrd> termEnt : fieldEnt.getValue().entrySet()) {
        allTerms.add(new FieldAndTerm(field, termEnt.getKey(), ord++));
      }
    }

    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: done init postings; " + allTerms.size() + " total terms, across " + fieldInfos.size() + " fields");
    }
  }

  public static SeedPostings getSeedPostings(String term, long seed, IndexOptions options, boolean allowPayloads) {
    int minDocFreq, maxDocFreq;
    if (term.startsWith("big_")) {
      minDocFreq = LuceneTestCase.RANDOM_MULTIPLIER * 50000;
      maxDocFreq = LuceneTestCase.RANDOM_MULTIPLIER * 70000;
    } else if (term.startsWith("medium_")) {
      minDocFreq = LuceneTestCase.RANDOM_MULTIPLIER * 3000;
      maxDocFreq = LuceneTestCase.RANDOM_MULTIPLIER * 6000;
    } else if (term.startsWith("low_")) {
      minDocFreq = LuceneTestCase.RANDOM_MULTIPLIER;
      maxDocFreq = LuceneTestCase.RANDOM_MULTIPLIER * 40;
    } else {
      minDocFreq = 1;
      maxDocFreq = 3;
    }

    return new SeedPostings(seed, minDocFreq, maxDocFreq, options, allowPayloads);
  }

  /** Given the same random seed this always enumerates the
   *  same random postings */
  public static class SeedPostings extends PostingsEnum {
    // Used only to generate docIDs; this way if you pull w/
    // or w/o positions you get the same docID sequence:
    private final Random docRandom;
    private final Random random;
    public int docFreq;
    private final int maxDocSpacing;
    private final int payloadSize;
    private final boolean fixedPayloads;
    private final BytesRef payload;
    private final boolean doPositions;
    private final boolean allowPayloads;

    private int docID = -1;
    private int freq;
    public int upto;

    private int pos;
    private int offset;
    private int startOffset;
    private int endOffset;
    private int posSpacing;
    private int posUpto;

    public SeedPostings(long seed, int minDocFreq, int maxDocFreq, IndexOptions options, boolean allowPayloads) {
      random = new Random(seed);
      docRandom = new Random(random.nextLong());
      docFreq = TestUtil.nextInt(random, minDocFreq, maxDocFreq);
      this.allowPayloads = allowPayloads;

      // TODO: more realistic to inversely tie this to numDocs:
      maxDocSpacing = TestUtil.nextInt(random, 1, 100);

      if (random.nextInt(10) == 7) {
        // 10% of the time create big payloads:
        payloadSize = 1 + random.nextInt(3);
      } else {
        payloadSize = 1 + random.nextInt(1);
      }

      fixedPayloads = random.nextBoolean();
      byte[] payloadBytes = new byte[payloadSize];
      payload = new BytesRef(payloadBytes);
      doPositions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.compareTo(options) <= 0;
    }

    @Override
    public int nextDoc() {
      while(true) {
        _nextDoc();
        return docID;
      }
    }

    private int _nextDoc() {
      if (docID == -1) {
        docID = 0;
      }
      // Must consume random:
      while(posUpto < freq) {
        nextPosition();
      }

      if (upto < docFreq) {
        if (upto == 0 && docRandom.nextBoolean()) {
          // Sometimes index docID = 0
        } else if (maxDocSpacing == 1) {
          docID++;
        } else {
          // TODO: sometimes have a biggish gap here!
          docID += TestUtil.nextInt(docRandom, 1, maxDocSpacing);
        }

        if (random.nextInt(200) == 17) {
          freq = TestUtil.nextInt(random, 1, 1000);
        } else if (random.nextInt(10) == 17) {
          freq = TestUtil.nextInt(random, 1, 20);
        } else {
          freq = TestUtil.nextInt(random, 1, 4);
        }

        pos = 0;
        offset = 0;
        posUpto = 0;
        posSpacing = TestUtil.nextInt(random, 1, 100);

        upto++;
        return docID;
      } else {
        return docID = NO_MORE_DOCS;
      }
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int nextPosition() {
      if (!doPositions) {
        posUpto = freq;
        return -1;
      }
      assert posUpto < freq;
      
      if (posUpto == 0 && random.nextBoolean()) {
        // Sometimes index pos = 0
      } else if (posSpacing == 1) {
        pos++;
      } else {
        pos += TestUtil.nextInt(random, 1, posSpacing);
      }

      if (payloadSize != 0) {
        if (fixedPayloads) {
          payload.length = payloadSize;
          random.nextBytes(payload.bytes); 
        } else {
          int thisPayloadSize = random.nextInt(payloadSize);
          if (thisPayloadSize != 0) {
            payload.length = payloadSize;
            random.nextBytes(payload.bytes); 
          } else {
            payload.length = 0;
          }
        } 
      } else {
        payload.length = 0;
      }
      if (!allowPayloads) {
        payload.length = 0;
      }

      startOffset = offset + random.nextInt(5);
      endOffset = startOffset + random.nextInt(10);
      offset = endOffset;

      posUpto++;
      return pos;
    }

    @Override
    public int startOffset() {
      return startOffset;
    }

    @Override
    public int endOffset() {
      return endOffset;
    }

    @Override
    public BytesRef getPayload() {
      return payload.length == 0 ? null : payload;
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }
    
    @Override
    public long cost() {
      return docFreq;
    } 
  }

  /** Holds one field, term and ord. */
  public static class FieldAndTerm {
    final String field;
    final BytesRef term;
    final long ord;

    public FieldAndTerm(String field, BytesRef term, long ord) {
      this.field = field;
      this.term = BytesRef.deepCopyOf(term);
      this.ord = ord;
    }
  }

  private static class SeedAndOrd {
    final long seed;
    long ord;

    public SeedAndOrd(long seed) {
      this.seed = seed;
    }
  }

  private static class SeedFields extends Fields {
    final Map<String,SortedMap<BytesRef,SeedAndOrd>> fields;
    final FieldInfos fieldInfos;
    final IndexOptions maxAllowed;
    final boolean allowPayloads;

    public SeedFields(Map<String,SortedMap<BytesRef,SeedAndOrd>> fields, FieldInfos fieldInfos, IndexOptions maxAllowed, boolean allowPayloads) {
      this.fields = fields;
      this.fieldInfos = fieldInfos;
      this.maxAllowed = maxAllowed;
      this.allowPayloads = allowPayloads;
    }

    @Override
    public Iterator<String> iterator() {
      return fields.keySet().iterator();
    }

    @Override
    public Terms terms(String field) {
      SortedMap<BytesRef,SeedAndOrd> terms = fields.get(field);
      if (terms == null) {
        return null;
      } else {
        return new SeedTerms(terms, fieldInfos.fieldInfo(field), maxAllowed, allowPayloads);
      }
    }

    @Override
    public int size() {
      return fields.size();
    }
  }

  private static class SeedTerms extends Terms {
    final SortedMap<BytesRef,SeedAndOrd> terms;
    final FieldInfo fieldInfo;
    final IndexOptions maxAllowed;
    final boolean allowPayloads;

    public SeedTerms(SortedMap<BytesRef,SeedAndOrd> terms, FieldInfo fieldInfo, IndexOptions maxAllowed, boolean allowPayloads) {
      this.terms = terms;
      this.fieldInfo = fieldInfo;
      this.maxAllowed = maxAllowed;
      this.allowPayloads = allowPayloads;
    }

    @Override
    public TermsEnum iterator() {
      SeedTermsEnum termsEnum = new SeedTermsEnum(terms, maxAllowed, allowPayloads);
      termsEnum.reset();

      return termsEnum;
    }

    @Override
    public long size() {
      return terms.size();
    }

    @Override
    public long getSumTotalTermFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFreqs() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }
  
    @Override
    public boolean hasPositions() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }
  
    @Override
    public boolean hasPayloads() {
      return allowPayloads && fieldInfo.hasPayloads();
    }
  }

  private static class SeedTermsEnum extends BaseTermsEnum {
    final SortedMap<BytesRef,SeedAndOrd> terms;
    final IndexOptions maxAllowed;
    final boolean allowPayloads;

    private Iterator<Map.Entry<BytesRef,SeedAndOrd>> iterator;

    private Map.Entry<BytesRef,SeedAndOrd> current;

    public SeedTermsEnum(SortedMap<BytesRef,SeedAndOrd> terms, IndexOptions maxAllowed, boolean allowPayloads) {
      this.terms = terms;
      this.maxAllowed = maxAllowed;
      this.allowPayloads = allowPayloads;
    }

    void reset() {
      iterator = terms.entrySet().iterator();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) {
      SortedMap<BytesRef,SeedAndOrd> tailMap = terms.tailMap(text);
      if (tailMap.isEmpty()) {
        return SeekStatus.END;
      } else {
        iterator = tailMap.entrySet().iterator();
        current = iterator.next();
        if (tailMap.firstKey().equals(text)) {
          return SeekStatus.FOUND;
        } else {
          return SeekStatus.NOT_FOUND;
        }
      }
    }

    @Override
    public BytesRef next() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return term();
      } else {
        return null;
      }
    }

    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef term() {
      return current.getKey();
    }

    @Override
    public long ord() {
      return current.getValue().ord;
    }

    @Override
    public int docFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public final PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
        if (maxAllowed.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
          return null;
        }
        if (PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS) && maxAllowed.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) < 0) {
          return null;
        }
        if (PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS) && allowPayloads == false) {
          return null;
        }
      }
      if (PostingsEnum.featureRequested(flags, PostingsEnum.FREQS) && maxAllowed.compareTo(IndexOptions.DOCS_AND_FREQS) < 0) {
        return null;
      }
      return getSeedPostings(current.getKey().utf8ToString(), current.getValue().seed, maxAllowed, allowPayloads);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private static class ThreadState {
    // Only used with REUSE option:
    public PostingsEnum reusePostingsEnum;
  }

  private FieldInfos currentFieldInfos;

  // maxAllowed = the "highest" we can index, but we will still
  // randomly index at lower IndexOption
  public FieldsProducer buildIndex(Codec codec, Directory dir, IndexOptions maxAllowed, boolean allowPayloads, boolean alwaysTestMax) throws IOException {
    SegmentInfo segmentInfo = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "_0", maxDoc, false, codec, Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);

    int maxIndexOption = Arrays.asList(IndexOptions.values()).indexOf(maxAllowed);
    if (LuceneTestCase.VERBOSE) {
      System.out.println("\nTEST: now build index");
    }

    // TODO use allowPayloads

    FieldInfo[] newFieldInfoArray = new FieldInfo[fields.size()];
    for(int fieldUpto=0;fieldUpto<fields.size();fieldUpto++) {
      FieldInfo oldFieldInfo = fieldInfos.fieldInfo(fieldUpto);
    
      // Randomly picked the IndexOptions to index this
      // field with:
      IndexOptions indexOptions = IndexOptions.values()[alwaysTestMax ? maxIndexOption : TestUtil.nextInt(random, 1, maxIndexOption)];
      boolean doPayloads = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 && allowPayloads;

      newFieldInfoArray[fieldUpto] = new FieldInfo(oldFieldInfo.name,
                                                   fieldUpto,
                                                   false,
                                                   false,
                                                   doPayloads,
                                                   indexOptions,
                                                   DocValuesType.NONE,
                                                   -1,
                                                   new HashMap<>(),
                                                   0, 0, 0, false);
    }

    FieldInfos newFieldInfos = new FieldInfos(newFieldInfoArray);

    // Estimate that flushed segment size will be 25% of
    // what we use in RAM:
    long bytes =  totalPostings * 8 + totalPayloadBytes;

    SegmentWriteState writeState = new SegmentWriteState(null, dir,
                                                         segmentInfo, newFieldInfos,
                                                         null, new IOContext(new FlushInfo(maxDoc, bytes)));

    Fields seedFields = new SeedFields(fields, newFieldInfos, maxAllowed, allowPayloads);

    NormsProducer fakeNorms = new NormsProducer() {

      @Override
      public void close() throws IOException {}

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public NumericDocValues getNorms(FieldInfo field) throws IOException {
        if (newFieldInfos.fieldInfo(field.number).hasNorms()) {
          return new NumericDocValues() {
            
            int doc = -1;
            
            @Override
            public int nextDoc() throws IOException {
              if (++doc == segmentInfo.maxDoc()) {
                return doc = NO_MORE_DOCS;
              }
              return doc;
            }
            
            @Override
            public int docID() {
              return doc;
            }
            
            @Override
            public long cost() {
              return segmentInfo.maxDoc();
            }
            
            @Override
            public int advance(int target) throws IOException {
              return doc = target >= segmentInfo.maxDoc() ? DocIdSetIterator.NO_MORE_DOCS : target;
            }
            
            @Override
            public boolean advanceExact(int target) throws IOException {
              doc = target;
              return true;
            }
            
            @Override
            public long longValue() throws IOException {
              return DOC_TO_NORM.applyAsLong(doc);
            }
          };
        } else {
          return null;
        }
      }

      @Override
      public void checkIntegrity() throws IOException {}
      
    };
    FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(writeState);
    boolean success = false;
    try {
      consumer.write(seedFields, fakeNorms);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }

    if (LuceneTestCase.VERBOSE) {
      System.out.println("TEST: after indexing: files=");
      for(String file : dir.listAll()) {
        System.out.println("  " + file + ": " + dir.fileLength(file) + " bytes");
      }
    }

    currentFieldInfos = newFieldInfos;

    SegmentReadState readState = new SegmentReadState(dir, segmentInfo, newFieldInfos, IOContext.READ);

    return codec.postingsFormat().fieldsProducer(readState);
  }

  private void verifyEnum(Random random,
                          ThreadState threadState,
                          String field,
                          BytesRef term,
                          TermsEnum termsEnum,

                          // Maximum options (docs/freqs/positions/offsets) to test:
                          IndexOptions maxTestOptions,

                          IndexOptions maxIndexOptions,

                          EnumSet<Option> options,
                          boolean alwaysTestMax) throws IOException {
        
    if (LuceneTestCase.VERBOSE) {
      System.out.println("  verifyEnum: options=" + options + " maxTestOptions=" + maxTestOptions);
    }

    // Make sure TermsEnum really is positioned on the
    // expected term:
    assertEquals(term, termsEnum.term());

    FieldInfo fieldInfo = currentFieldInfos.fieldInfo(field);

    // NOTE: can be empty list if we are using liveDocs:
    SeedPostings expected = getSeedPostings(term.utf8ToString(), 
                                            fields.get(field).get(term).seed,
                                            maxIndexOptions,
                                            true);
    assertEquals(expected.docFreq, termsEnum.docFreq());

    boolean allowFreqs = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0 &&
      maxTestOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean doCheckFreqs = allowFreqs && (alwaysTestMax || random.nextInt(3) <= 2);

    boolean allowPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 &&
      maxTestOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean doCheckPositions = allowPositions && (alwaysTestMax || random.nextInt(3) <= 2);

    boolean allowOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0 &&
      maxTestOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    boolean doCheckOffsets = allowOffsets && (alwaysTestMax || random.nextInt(3) <= 2);

    boolean doCheckPayloads = options.contains(Option.PAYLOADS) && allowPositions && fieldInfo.hasPayloads() && (alwaysTestMax || random.nextInt(3) <= 2);

    PostingsEnum prevPostingsEnum = null;

    PostingsEnum postingsEnum;

    if (!doCheckPositions) {
      if (allowPositions && random.nextInt(10) == 7) {
        // 10% of the time, even though we will not check positions, pull a DocsAndPositions enum
        
        if (options.contains(Option.REUSE_ENUMS) && random.nextInt(10) < 9) {
          prevPostingsEnum = threadState.reusePostingsEnum;
        }

        int flags = PostingsEnum.POSITIONS;
        if (alwaysTestMax || random.nextBoolean()) {
          flags |= PostingsEnum.OFFSETS;
        }
        if (alwaysTestMax || random.nextBoolean()) {
          flags |= PostingsEnum.PAYLOADS;
        }

        if (LuceneTestCase.VERBOSE) {
          System.out.println("  get DocsEnum (but we won't check positions) flags=" + flags);
        }

        threadState.reusePostingsEnum = termsEnum.postings(prevPostingsEnum, flags);
        postingsEnum = threadState.reusePostingsEnum;
      } else {
        if (LuceneTestCase.VERBOSE) {
          System.out.println("  get DocsEnum");
        }
        if (options.contains(Option.REUSE_ENUMS) && random.nextInt(10) < 9) {
          prevPostingsEnum = threadState.reusePostingsEnum;
        }
        threadState.reusePostingsEnum = termsEnum.postings(prevPostingsEnum, doCheckFreqs ? PostingsEnum.FREQS : PostingsEnum.NONE);
        postingsEnum = threadState.reusePostingsEnum;
      }
    } else {
      if (options.contains(Option.REUSE_ENUMS) && random.nextInt(10) < 9) {
        prevPostingsEnum = threadState.reusePostingsEnum;
      }

      int flags = PostingsEnum.POSITIONS;
      if (alwaysTestMax || doCheckOffsets || random.nextInt(3) == 1) {
        flags |= PostingsEnum.OFFSETS;
      }
      if (alwaysTestMax || doCheckPayloads|| random.nextInt(3) == 1) {
        flags |= PostingsEnum.PAYLOADS;
      }

      if (LuceneTestCase.VERBOSE) {
        System.out.println("  get DocsEnum flags=" + flags);
      }

      threadState.reusePostingsEnum = termsEnum.postings(prevPostingsEnum, flags);
      postingsEnum = threadState.reusePostingsEnum;
    }

    assertNotNull("null DocsEnum", postingsEnum);
    int initialDocID = postingsEnum.docID();
    assertEquals("inital docID should be -1" + postingsEnum, -1, initialDocID);

    if (LuceneTestCase.VERBOSE) {
      if (prevPostingsEnum == null) {
        System.out.println("  got enum=" + postingsEnum);
      } else if (prevPostingsEnum == postingsEnum) {
        System.out.println("  got reuse enum=" + postingsEnum);
      } else {
        System.out.println("  got enum=" + postingsEnum + " (reuse of " + prevPostingsEnum + " failed)");
      }
    }

    // 10% of the time don't consume all docs:
    int stopAt;
    if (!alwaysTestMax && options.contains(Option.PARTIAL_DOC_CONSUME) && expected.docFreq > 1 && random.nextInt(10) == 7) {
      stopAt = random.nextInt(expected.docFreq-1);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("  will not consume all docs (" + stopAt + " vs " + expected.docFreq + ")");
      }
    } else {
      stopAt = expected.docFreq;
      if (LuceneTestCase.VERBOSE) {
        System.out.println("  consume all docs");
      }
    }

    double skipChance = alwaysTestMax ? 0.5 : random.nextDouble();
    int numSkips = expected.docFreq < 3 ? 1 : TestUtil.nextInt(random, 1, Math.min(20, expected.docFreq / 3));
    int skipInc = expected.docFreq/numSkips;
    int skipDocInc = maxDoc/numSkips;

    // Sometimes do 100% skipping:
    boolean doAllSkipping = options.contains(Option.SKIPPING) && random.nextInt(7) == 1;

    double freqAskChance = alwaysTestMax ? 1.0 : random.nextDouble();
    double payloadCheckChance = alwaysTestMax ? 1.0 : random.nextDouble();
    double offsetCheckChance = alwaysTestMax ? 1.0 : random.nextDouble();

    if (LuceneTestCase.VERBOSE) {
      if (options.contains(Option.SKIPPING)) {
        System.out.println("  skipChance=" + skipChance + " numSkips=" + numSkips);
      } else {
        System.out.println("  no skipping");
      }
      if (doCheckFreqs) {
        System.out.println("  freqAskChance=" + freqAskChance);
      }
      if (doCheckPayloads) {
        System.out.println("  payloadCheckChance=" + payloadCheckChance);
      }
      if (doCheckOffsets) {
        System.out.println("  offsetCheckChance=" + offsetCheckChance);
      }
    }

    while (expected.upto <= stopAt) {
      if (expected.upto == stopAt) {
        if (stopAt == expected.docFreq) {
          assertEquals("DocsEnum should have ended but didn't", PostingsEnum.NO_MORE_DOCS, postingsEnum.nextDoc());

          // Common bug is to forget to set this.doc=NO_MORE_DOCS in the enum!:
          assertEquals("DocsEnum should have ended but didn't", PostingsEnum.NO_MORE_DOCS, postingsEnum.docID());
        }
        break;
      }

      if (options.contains(Option.SKIPPING) && (doAllSkipping || random.nextDouble() <= skipChance)) {
        int targetDocID = -1;
        if (expected.upto < stopAt && random.nextBoolean()) {
          // Pick target we know exists:
          final int skipCount = TestUtil.nextInt(random, 1, skipInc);
          for(int skip=0;skip<skipCount;skip++) {
            if (expected.nextDoc() == PostingsEnum.NO_MORE_DOCS) {
              break;
            }
          }
        } else {
          // Pick random target (might not exist):
          final int skipDocIDs = TestUtil.nextInt(random, 1, skipDocInc);
          if (skipDocIDs > 0) {
            targetDocID = expected.docID() + skipDocIDs;
            expected.advance(targetDocID);
          }
        }

        if (expected.upto >= stopAt) {
          int target = random.nextBoolean() ? maxDoc : PostingsEnum.NO_MORE_DOCS;
          if (LuceneTestCase.VERBOSE) {
            System.out.println("  now advance to end (target=" + target + ")");
          }
          assertEquals("DocsEnum should have ended but didn't", PostingsEnum.NO_MORE_DOCS, postingsEnum.advance(target));
          break;
        } else {
          if (LuceneTestCase.VERBOSE) {
            if (targetDocID != -1) {
              System.out.println("  now advance to random target=" + targetDocID + " (" + expected.upto + " of " + stopAt + ") current=" + postingsEnum.docID());
            } else {
              System.out.println("  now advance to known-exists target=" + expected.docID() + " (" + expected.upto + " of " + stopAt + ") current=" + postingsEnum.docID());
            }
          }
          int docID = postingsEnum.advance(targetDocID != -1 ? targetDocID : expected.docID());
          assertEquals("docID is wrong", expected.docID(), docID);
        }
      } else {
        expected.nextDoc();
        if (LuceneTestCase.VERBOSE) {
          System.out.println("  now nextDoc to " + expected.docID() + " (" + expected.upto + " of " + stopAt + ")");
        }
        int docID = postingsEnum.nextDoc();
        assertEquals("docID is wrong", expected.docID(), docID);
        if (docID == PostingsEnum.NO_MORE_DOCS) {
          break;
        }
      }

      if (doCheckFreqs && random.nextDouble() <= freqAskChance) {
        if (LuceneTestCase.VERBOSE) {
          System.out.println("    now freq()=" + expected.freq());
        }
        int freq = postingsEnum.freq();
        assertEquals("freq is wrong", expected.freq(), freq);
      }

      if (doCheckPositions) {
        int freq = postingsEnum.freq();
        int numPosToConsume;
        if (!alwaysTestMax && options.contains(Option.PARTIAL_POS_CONSUME) && random.nextInt(5) == 1) {
          numPosToConsume = random.nextInt(freq);
        } else {
          numPosToConsume = freq;
        }

        for(int i=0;i<numPosToConsume;i++) {
          int pos = expected.nextPosition();
          if (LuceneTestCase.VERBOSE) {
            System.out.println("    now nextPosition to " + pos);
          }
          assertEquals("position is wrong", pos, postingsEnum.nextPosition());

          if (doCheckPayloads) {
            BytesRef expectedPayload = expected.getPayload();
            if (random.nextDouble() <= payloadCheckChance) {
              if (LuceneTestCase.VERBOSE) {
                System.out.println("      now check expectedPayload length=" + (expectedPayload == null ? 0 : expectedPayload.length));
              }
              if (expectedPayload == null || expectedPayload.length == 0) {
                assertNull("should not have payload", postingsEnum.getPayload());
              } else {
                BytesRef payload = postingsEnum.getPayload();
                assertNotNull("should have payload but doesn't", payload);

                assertEquals("payload length is wrong", expectedPayload.length, payload.length);
                for(int byteUpto=0;byteUpto<expectedPayload.length;byteUpto++) {
                  assertEquals("payload bytes are wrong",
                               expectedPayload.bytes[expectedPayload.offset + byteUpto],
                               payload.bytes[payload.offset+byteUpto]);
                }
                
                // make a deep copy
                payload = BytesRef.deepCopyOf(payload);
                assertEquals("2nd call to getPayload returns something different!", payload, postingsEnum.getPayload());
              }
            } else {
              if (LuceneTestCase.VERBOSE) {
                System.out.println("      skip check payload length=" + (expectedPayload == null ? 0 : expectedPayload.length));
              }
            }
          }

          if (doCheckOffsets) {
            if (random.nextDouble() <= offsetCheckChance) {
              if (LuceneTestCase.VERBOSE) {
                System.out.println("      now check offsets: startOff=" + expected.startOffset() + " endOffset=" + expected.endOffset());
              }
              assertEquals("startOffset is wrong", expected.startOffset(), postingsEnum.startOffset());
              assertEquals("endOffset is wrong", expected.endOffset(), postingsEnum.endOffset());
            } else {
              if (LuceneTestCase.VERBOSE) {
                System.out.println("      skip check offsets");
              }
            }
          } else if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) < 0) {
            if (LuceneTestCase.VERBOSE) {
              System.out.println("      now check offsets are -1");
            }
            assertEquals("startOffset isn't -1", -1, postingsEnum.startOffset());
            assertEquals("endOffset isn't -1", -1, postingsEnum.endOffset());
          }
        }
      }
    }

    if (options.contains(Option.SKIPPING)) {
      final IntToLongFunction docToNorm;
      if (fieldInfo.hasNorms()) {
        docToNorm = DOC_TO_NORM;
      } else {
        docToNorm = doc -> 1L;
      }

      // First check impacts and block uptos
      int max = -1;
      List<Impact> impactsCopy = null;
      int flags = PostingsEnum.FREQS;
      if (doCheckPositions) {
        flags |= PostingsEnum.POSITIONS;
        if (doCheckOffsets) {
          flags |= PostingsEnum.OFFSETS;
        }
        if (doCheckPayloads) {
          flags |= PostingsEnum.PAYLOADS;
        }
      }

      ImpactsEnum impactsEnum = termsEnum.impacts(flags);
      PostingsEnum postings = termsEnum.postings(null, flags);
      for (int doc = impactsEnum.nextDoc(); ; doc = impactsEnum.nextDoc()) {
        assertEquals(postings.nextDoc(), doc);
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        int freq = postings.freq();
        assertEquals("freq is wrong", freq, impactsEnum.freq());
        for (int i = 0; i < freq; ++i) {
          int pos = postings.nextPosition();
          assertEquals("position is wrong", pos, impactsEnum.nextPosition());
          if (doCheckOffsets) {
            assertEquals("startOffset is wrong", postings.startOffset(), impactsEnum.startOffset());
            assertEquals("endOffset is wrong", postings.endOffset(), impactsEnum.endOffset());
          }
          if (doCheckPayloads) {
            assertEquals("payload is wrong", postings.getPayload(), impactsEnum.getPayload());
          }
        }
        if (doc > max) {
          impactsEnum.advanceShallow(doc);
          Impacts impacts = impactsEnum.getImpacts();
          CheckIndex.checkImpacts(impacts, doc);
          impactsCopy = impacts.getImpacts(0)
              .stream()
              .map(i -> new Impact(i.freq, i.norm))
              .collect(Collectors.toList());
        }
        freq = impactsEnum.freq();
        long norm = docToNorm.applyAsLong(doc);
        int idx = Collections.binarySearch(impactsCopy, new Impact(freq, norm), Comparator.comparing(i -> i.freq));
        if (idx < 0) {
          idx = -1 - idx;
        }
        assertTrue("Got " + new Impact(freq, norm) + " in postings, but no impact triggers equal or better scores in " + impactsCopy,
            idx <= impactsCopy.size() && impactsCopy.get(idx).norm <= norm);
      }

      // Now check advancing
      impactsEnum = termsEnum.impacts(flags);
      postings = termsEnum.postings(postings, flags);

      max = -1;
      while (true) {
        int doc = impactsEnum.docID();
        boolean advance;
        int target;
        if (random.nextBoolean()) {
          advance = false;
          target = doc + 1;
        } else {
          advance = true;
          int delta = Math.min(1 + random.nextInt(512), DocIdSetIterator.NO_MORE_DOCS - doc);
          target = impactsEnum.docID() + delta;
        }

        if (target > max && random.nextBoolean()) {
          int delta = Math.min(random.nextInt(512), DocIdSetIterator.NO_MORE_DOCS - target);
          max = target + delta;
          
          impactsEnum.advanceShallow(target);
          Impacts impacts = impactsEnum.getImpacts();
          CheckIndex.checkImpacts(impacts, target);
          impactsCopy = Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));
          for (int level = 0; level < impacts.numLevels(); ++level) {
            if (impacts.getDocIdUpTo(level) >= max) {
              impactsCopy = impacts.getImpacts(level)
                  .stream()
                  .map(i -> new Impact(i.freq, i.norm))
                  .collect(Collectors.toList());
              break;
            }
          }
        }

        if (advance) {
          doc = impactsEnum.advance(target);
        } else {
          doc = impactsEnum.nextDoc();
        }

        assertEquals(postings.advance(target), doc);
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        int freq = postings.freq();
        assertEquals("freq is wrong", freq, impactsEnum.freq());
        for (int i = 0; i < postings.freq(); ++i) {
          int pos = postings.nextPosition();
          assertEquals("position is wrong", pos, impactsEnum.nextPosition());
          if (doCheckOffsets) {
            assertEquals("startOffset is wrong", postings.startOffset(), impactsEnum.startOffset());
            assertEquals("endOffset is wrong", postings.endOffset(), impactsEnum.endOffset());
          }
          if (doCheckPayloads) {
            assertEquals("payload is wrong", postings.getPayload(), impactsEnum.getPayload());
          }
        }

        if (doc > max) {
          int delta = Math.min(1 + random.nextInt(512), DocIdSetIterator.NO_MORE_DOCS - doc);
          max = doc + delta;
          Impacts impacts = impactsEnum.getImpacts();
          CheckIndex.checkImpacts(impacts, doc);
          impactsCopy = Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));
          for (int level = 0; level < impacts.numLevels(); ++level) {
            if (impacts.getDocIdUpTo(level) >= max) {
              impactsCopy = impacts.getImpacts(level)
                  .stream()
                  .map(i -> new Impact(i.freq, i.norm))
                  .collect(Collectors.toList());
              break;
            }
          }
        }

        freq = impactsEnum.freq();
        long norm = docToNorm.applyAsLong(doc);
        int idx = Collections.binarySearch(impactsCopy, new Impact(freq, norm), Comparator.comparing(i -> i.freq));
        if (idx < 0) {
          idx = -1 - idx;
        }
        assertTrue("Got " + new Impact(freq, norm) + " in postings, but no impact triggers equal or better scores in " + impactsCopy,
            idx <= impactsCopy.size() && impactsCopy.get(idx).norm <= norm);
      }
    }
  }

  private static class TestThread extends Thread {
    private Fields fieldsSource;
    private EnumSet<Option> options;
    private IndexOptions maxIndexOptions;
    private IndexOptions maxTestOptions;
    private boolean alwaysTestMax;
    private RandomPostingsTester postingsTester;
    private Random random;

    public TestThread(Random random, RandomPostingsTester postingsTester, Fields fieldsSource, EnumSet<Option> options, IndexOptions maxTestOptions,
                      IndexOptions maxIndexOptions, boolean alwaysTestMax) {
      this.random = random;
      this.fieldsSource = fieldsSource;
      this.options = options;
      this.maxTestOptions = maxTestOptions;
      this.maxIndexOptions = maxIndexOptions;
      this.alwaysTestMax = alwaysTestMax;
      this.postingsTester = postingsTester;
    }

    @Override
    public void run() {
      try {
        try {
          postingsTester.testTermsOneThread(random, fieldsSource, options, maxTestOptions, maxIndexOptions, alwaysTestMax);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      } finally {
        fieldsSource = null;
        postingsTester = null;
      }
    }
  }

  public void testTerms(final Fields fieldsSource, final EnumSet<Option> options,
                        final IndexOptions maxTestOptions,
                        final IndexOptions maxIndexOptions,
                        final boolean alwaysTestMax) throws Exception {

    if (options.contains(Option.THREADS)) {
      int numThreads = LuceneTestCase.TEST_NIGHTLY ? TestUtil.nextInt(random, 2, 5) : 2;
      Thread[] threads = new Thread[numThreads];
      for(int threadUpto=0;threadUpto<numThreads;threadUpto++) {
        threads[threadUpto] = new TestThread(new Random(random.nextLong()), this, fieldsSource, options, maxTestOptions, maxIndexOptions, alwaysTestMax);
        threads[threadUpto].start();
      }
      for(int threadUpto=0;threadUpto<numThreads;threadUpto++) {
        threads[threadUpto].join();
      }
    } else {
      testTermsOneThread(random, fieldsSource, options, maxTestOptions, maxIndexOptions, alwaysTestMax);
    }
  }

  private void testTermsOneThread(Random random, Fields fieldsSource, EnumSet<Option> options,
                                  IndexOptions maxTestOptions,
                                  IndexOptions maxIndexOptions, boolean alwaysTestMax) throws IOException {

    ThreadState threadState = new ThreadState();

    // Test random terms/fields:
    List<TermState> termStates = new ArrayList<>();
    List<FieldAndTerm> termStateTerms = new ArrayList<>();

    boolean supportsOrds = true;
    
    Collections.shuffle(allTerms, random);
    int upto = 0;
    while (upto < allTerms.size()) {

      boolean useTermState = termStates.size() != 0 && random.nextInt(5) == 1;
      boolean useTermOrd = supportsOrds && useTermState == false && random.nextInt(5) == 1;

      FieldAndTerm fieldAndTerm;
      TermsEnum termsEnum;

      TermState termState = null;

      if (!useTermState) {
        // Seek by random field+term:
        fieldAndTerm = allTerms.get(upto++);
        if (LuceneTestCase.VERBOSE) {
          if (useTermOrd) {
            System.out.println("\nTEST: seek to term=" + fieldAndTerm.field + ":" + fieldAndTerm.term.utf8ToString() + " using ord=" + fieldAndTerm.ord);
          } else {
            System.out.println("\nTEST: seek to term=" + fieldAndTerm.field + ":" + fieldAndTerm.term.utf8ToString() );
          }
        }
      } else {
        // Seek by previous saved TermState
        int idx = random.nextInt(termStates.size());
        fieldAndTerm = termStateTerms.get(idx);
        if (LuceneTestCase.VERBOSE) {
          System.out.println("\nTEST: seek using TermState to term=" + fieldAndTerm.field + ":" + fieldAndTerm.term.utf8ToString());
        }
        termState = termStates.get(idx);
      }

      Terms terms = fieldsSource.terms(fieldAndTerm.field);
      assertNotNull(terms);
      termsEnum = terms.iterator();

      if (!useTermState) {
        if (useTermOrd) {
          // Try seek by ord sometimes:
          try {
            termsEnum.seekExact(fieldAndTerm.ord);
          } catch (UnsupportedOperationException uoe) {
            supportsOrds = false;
            assertTrue(termsEnum.seekExact(fieldAndTerm.term));
          }
        } else {
          assertTrue(termsEnum.seekExact(fieldAndTerm.term));
        }
      } else {
        termsEnum.seekExact(fieldAndTerm.term, termState);
      }
      
      // check we really seeked to the right place
      assertEquals(fieldAndTerm.term, termsEnum.term());

      long termOrd;
      if (supportsOrds) {
        try {
          termOrd = termsEnum.ord();
        } catch (UnsupportedOperationException uoe) {
          supportsOrds = false;
          termOrd = -1;
        }
      } else {
        termOrd = -1;
      }

      if (termOrd != -1) {
        // PostingsFormat supports ords
        assertEquals(fieldAndTerm.ord, termsEnum.ord());
      }

      boolean savedTermState = false;

      if (options.contains(Option.TERM_STATE) && !useTermState && random.nextInt(5) == 1) {
        // Save away this TermState:
        termStates.add(termsEnum.termState());
        termStateTerms.add(fieldAndTerm);
        savedTermState = true;
      }

      verifyEnum(random, threadState,
                 fieldAndTerm.field,
                 fieldAndTerm.term,
                 termsEnum,
                 maxTestOptions,
                 maxIndexOptions,
                 options,
                 alwaysTestMax);

      // Sometimes save term state after pulling the enum:
      if (options.contains(Option.TERM_STATE) && !useTermState && !savedTermState && random.nextInt(5) == 1) {
        // Save away this TermState:
        termStates.add(termsEnum.termState());
        termStateTerms.add(fieldAndTerm);
        useTermState = true;
      }

      // 10% of the time make sure you can pull another enum
      // from the same term:
      if (alwaysTestMax || random.nextInt(10) == 7) {
        // Try same term again
        if (LuceneTestCase.VERBOSE) {
          System.out.println("TEST: try enum again on same term");
        }

        verifyEnum(random, threadState,
                   fieldAndTerm.field,
                   fieldAndTerm.term,
                   termsEnum,
                   maxTestOptions,
                   maxIndexOptions,
                   options,
                   alwaysTestMax);
      }
    }

    // Test Terms.intersect:
    for(String field : fields.keySet()) {
      while (true) {
        Automaton a = AutomatonTestUtil.randomAutomaton(random);
        CompiledAutomaton ca = new CompiledAutomaton(a, null, true, Integer.MAX_VALUE, false);
        if (ca.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
          // Keep retrying until we get an A that will really "use" the PF's intersect code:
          continue;
        }
        // System.out.println("A:\n" + a.toDot());

        BytesRef startTerm = null;
        if (random.nextBoolean()) {
          RandomAcceptedStrings ras = new RandomAcceptedStrings(a);
          for (int iter=0;iter<100;iter++) {
            int[] codePoints = ras.getRandomAcceptedString(random);
            if (codePoints.length == 0) {
              continue;
            }
            startTerm = new BytesRef(UnicodeUtil.newString(codePoints, 0, codePoints.length));
            break;
          }
          // Don't allow empty string startTerm:
          if (startTerm == null) {
            continue;
          }
        }
        TermsEnum intersected = fieldsSource.terms(field).intersect(ca, startTerm);

        Set<BytesRef> intersectedTerms = new HashSet<BytesRef>();
        BytesRef term;
        while ((term = intersected.next()) != null) {
          if (startTerm != null) {
            // NOTE: not <=
            assertTrue(startTerm.compareTo(term) < 0);
          }
          intersectedTerms.add(BytesRef.deepCopyOf(term));     
          verifyEnum(random, threadState,
                     field,
                     term,
                     intersected,
                     maxTestOptions,
                     maxIndexOptions,
                     options,
                     alwaysTestMax);
        }

        if (ca.runAutomaton == null) {
          assertTrue(intersectedTerms.isEmpty());
        } else {
          for(BytesRef term2 : fields.get(field).keySet()) {
            boolean expected;
            if (startTerm != null && startTerm.compareTo(term2) >= 0) {
              expected = false;
            } else {
              expected = ca.runAutomaton.run(term2.bytes, term2.offset, term2.length);
            }
            assertEquals("term=" + term2, expected, intersectedTerms.contains(term2));
          }
        }

        break;
      }
    }
  }
  
  public void testFields(Fields fields) throws Exception {
    Iterator<String> iterator = fields.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      try {
        iterator.remove();
        throw new AssertionError("Fields.iterator() allows for removal");
      } catch (UnsupportedOperationException expected) {
        // expected;
      }
    }
    assertFalse(iterator.hasNext());
    LuceneTestCase.expectThrows(NoSuchElementException.class, () -> {
      iterator.next();
    });
  }

  /** Indexes all fields/terms at the specified
   *  IndexOptions, and fully tests at that IndexOptions. */
  public void testFull(Codec codec, Path path, IndexOptions options, boolean withPayloads) throws Exception {
    Directory dir = LuceneTestCase.newFSDirectory(path);

    // TODO test thread safety of buildIndex too
    FieldsProducer fieldsProducer = buildIndex(codec, dir, options, withPayloads, true);

    testFields(fieldsProducer);

    IndexOptions[] allOptions = IndexOptions.values();
    int maxIndexOption = Arrays.asList(allOptions).indexOf(options);

    for(int i=0;i<=maxIndexOption;i++) {
      testTerms(fieldsProducer, EnumSet.allOf(Option.class), allOptions[i], options, true);
      if (withPayloads) {
        // If we indexed w/ payloads, also test enums w/o accessing payloads:
        testTerms(fieldsProducer, EnumSet.complementOf(EnumSet.of(Option.PAYLOADS)), allOptions[i], options, true);
      }
    }

    fieldsProducer.close();
    dir.close();
  }
}
