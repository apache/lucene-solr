package org.apache.lucene.index;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Abstract class to do basic tests for a postings format.
 * NOTE: This test focuses on the postings
 * (docs/freqs/positions/payloads/offsets) impl, not the
 * terms dict.  The [stretch] goal is for this test to be
 * so thorough in testing a new PostingsFormat that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given PostingsFormat that this
 * test fails to catch then this test needs to be improved! */

// TODO can we make it easy for testing to pair up a "random terms dict impl" with your postings base format...

// TODO test when you reuse after skipping a term or two, eg the block reuse case

/* TODO
  - threads
  - assert doc=-1 before any nextDoc
  - if a PF passes this test but fails other tests then this
    test has a bug!!
  - test tricky reuse cases, eg across fields
  - verify you get null if you pass needFreq/needOffset but
    they weren't indexed
*/

public abstract class BasePostingsFormatTestCase extends LuceneTestCase {

  /**
   * Returns the Codec to run tests against
   */
  protected abstract Codec getCodec();

  private enum Option {
    // Sometimes use .advance():
    SKIPPING,

    // Sometimes reuse the Docs/AndPositionsEnum across terms:
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

  /** Given the same random seed this always enumerates the
   *  same random postings */
  private static class SeedPostings extends DocsAndPositionsEnum {
    // Used only to generate docIDs; this way if you pull w/
    // or w/o positions you get the same docID sequence:
    private final Random docRandom;
    private final Random random;
    public int docFreq;
    private final int maxDocSpacing;
    private final int payloadSize;
    private final boolean fixedPayloads;
    private final Bits liveDocs;
    private final BytesRef payload;
    private final IndexOptions options;
    private final boolean doPositions;

    private int docID;
    private int freq;
    public int upto;

    private int pos;
    private int offset;
    private int startOffset;
    private int endOffset;
    private int posSpacing;
    private int posUpto;

    public SeedPostings(long seed, int minDocFreq, int maxDocFreq, Bits liveDocs, IndexOptions options) {
      random = new Random(seed);
      docRandom = new Random(random.nextLong());
      docFreq = _TestUtil.nextInt(random, minDocFreq, maxDocFreq);
      this.liveDocs = liveDocs;

      // TODO: more realistic to inversely tie this to numDocs:
      maxDocSpacing = _TestUtil.nextInt(random, 1, 100);

      if (random.nextInt(10) == 7) {
        // 10% of the time create big payloads:
        payloadSize = 1 + random.nextInt(3);
      } else {
        payloadSize = 1 + random.nextInt(1);
      }

      fixedPayloads = random.nextBoolean();
      byte[] payloadBytes = new byte[payloadSize];
      payload = new BytesRef(payloadBytes);
      this.options = options;
      doPositions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.compareTo(options) <= 0;
    }

    @Override
    public int nextDoc() {
      while(true) {
        _nextDoc();
        if (liveDocs == null || docID == NO_MORE_DOCS || liveDocs.get(docID)) {
          return docID;
        }
      }
    }

    private int _nextDoc() {
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
          docID += _TestUtil.nextInt(docRandom, 1, maxDocSpacing);
        }

        if (random.nextInt(200) == 17) {
          freq = _TestUtil.nextInt(random, 1, 1000);
        } else if (random.nextInt(10) == 17) {
          freq = _TestUtil.nextInt(random, 1, 20);
        } else {
          freq = _TestUtil.nextInt(random, 1, 4);
        }

        pos = 0;
        offset = 0;
        posUpto = 0;
        posSpacing = _TestUtil.nextInt(random, 1, 100);

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
        return 0;
      }
      assert posUpto < freq;

      if (posUpto == 0 && random.nextBoolean()) {
        // Sometimes index pos = 0
      } else if (posSpacing == 1) {
        pos++;
      } else {
        pos += _TestUtil.nextInt(random, 1, posSpacing);
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
  
  private static class FieldAndTerm {
    String field;
    BytesRef term;

    public FieldAndTerm(String field, BytesRef term) {
      this.field = field;
      this.term = BytesRef.deepCopyOf(term);
    }
  }

  // Holds all postings:
  private static Map<String,Map<BytesRef,Long>> fields;

  private static FieldInfos fieldInfos;

  private static FixedBitSet globalLiveDocs;

  private static List<FieldAndTerm> allTerms;
  private static int maxDoc;

  private static long totalPostings;
  private static long totalPayloadBytes;

  private static SeedPostings getSeedPostings(String term, long seed, boolean withLiveDocs, IndexOptions options) {
    int minDocFreq, maxDocFreq;
    if (term.startsWith("big_")) {
      minDocFreq = RANDOM_MULTIPLIER * 50000;
      maxDocFreq = RANDOM_MULTIPLIER * 70000;
    } else if (term.startsWith("medium_")) {
      minDocFreq = RANDOM_MULTIPLIER * 3000;
      maxDocFreq = RANDOM_MULTIPLIER * 6000;
    } else if (term.startsWith("low_")) {
      minDocFreq = RANDOM_MULTIPLIER;
      maxDocFreq = RANDOM_MULTIPLIER * 40;
    } else {
      minDocFreq = 1;
      maxDocFreq = 3;
    }

    return new SeedPostings(seed, minDocFreq, maxDocFreq, withLiveDocs ? globalLiveDocs : null, options);
  }

  @BeforeClass
  public static void createPostings() throws IOException {
    totalPostings = 0;
    totalPayloadBytes = 0;
    fields = new TreeMap<String,Map<BytesRef,Long>>();

    final int numFields = _TestUtil.nextInt(random(), 1, 5);
    if (VERBOSE) {
      System.out.println("TEST: " + numFields + " fields");
    }
    maxDoc = 0;

    FieldInfo[] fieldInfoArray = new FieldInfo[numFields];
    int fieldUpto = 0;
    while (fieldUpto < numFields) {
      String field = _TestUtil.randomSimpleString(random());
      if (fields.containsKey(field)) {
        continue;
      }

      fieldInfoArray[fieldUpto] = new FieldInfo(field, true, fieldUpto, false, false, true,
                                                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                                                null, DocValuesType.NUMERIC, null);
      fieldUpto++;

      Map<BytesRef,Long> postings = new TreeMap<BytesRef,Long>();
      fields.put(field, postings);
      Set<String> seenTerms = new HashSet<String>();

      // TODO:
      //final int numTerms = atLeast(10);
      final int numTerms = 4;

      for(int termUpto=0;termUpto<numTerms;termUpto++) {
        String term = _TestUtil.randomSimpleString(random());
        if (seenTerms.contains(term)) {
          continue;
        }
        seenTerms.add(term);

        if (TEST_NIGHTLY && termUpto == 0 && fieldUpto == 1) {
          // Make 1 big term:
          term = "big_" + term;
        } else if (termUpto == 1 && fieldUpto == 1) {
          // Make 1 medium term:
          term = "medium_" + term;
        } else if (random().nextBoolean()) {
          // Low freq term:
          term = "low_" + term;
        } else {
          // Very low freq term (don't multiply by RANDOM_MULTIPLIER):
          term = "verylow_" + term;
        }

        long termSeed = random().nextLong();
        postings.put(new BytesRef(term), termSeed);

        // NOTE: sort of silly: we enum all the docs just to
        // get the maxDoc
        DocsEnum docsEnum = getSeedPostings(term, termSeed, false, IndexOptions.DOCS_ONLY);
        int doc;
        int lastDoc = 0;
        while((doc = docsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
          lastDoc = doc;
        }
        maxDoc = Math.max(lastDoc, maxDoc);
      }
    }

    fieldInfos = new FieldInfos(fieldInfoArray);

    // It's the count, not the last docID:
    maxDoc++;

    globalLiveDocs = new FixedBitSet(maxDoc);
    double liveRatio = random().nextDouble();
    for(int i=0;i<maxDoc;i++) {
      if (random().nextDouble() <= liveRatio) {
        globalLiveDocs.set(i);
      }
    }

    allTerms = new ArrayList<FieldAndTerm>();
    for(Map.Entry<String,Map<BytesRef,Long>> fieldEnt : fields.entrySet()) {
      String field = fieldEnt.getKey();
      for(Map.Entry<BytesRef,Long> termEnt : fieldEnt.getValue().entrySet()) {
        allTerms.add(new FieldAndTerm(field, termEnt.getKey()));
      }
    }

    if (VERBOSE) {
      System.out.println("TEST: done init postings; " + allTerms.size() + " total terms, across " + fieldInfos.size() + " fields");
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    allTerms = null;
    fieldInfos = null;
    fields = null;
    globalLiveDocs = null;
  }

  // TODO maybe instead of @BeforeClass just make a single test run: build postings & index & test it?

  private FieldInfos currentFieldInfos;

  // maxAllowed = the "highest" we can index, but we will still
  // randomly index at lower IndexOption
  private FieldsProducer buildIndex(Directory dir, IndexOptions maxAllowed, boolean allowPayloads, boolean alwaysTestMax) throws IOException {
    Codec codec = getCodec();
    SegmentInfo segmentInfo = new SegmentInfo(dir, Constants.LUCENE_MAIN_VERSION, "_0", maxDoc, false, codec, null, null);

    int maxIndexOption = Arrays.asList(IndexOptions.values()).indexOf(maxAllowed);
    if (VERBOSE) {
      System.out.println("\nTEST: now build index");
    }

    int maxIndexOptionNoOffsets = Arrays.asList(IndexOptions.values()).indexOf(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    // TODO use allowPayloads

    FieldInfo[] newFieldInfoArray = new FieldInfo[fields.size()];
    for(int fieldUpto=0;fieldUpto<fields.size();fieldUpto++) {
      FieldInfo oldFieldInfo = fieldInfos.fieldInfo(fieldUpto);

      String pf = _TestUtil.getPostingsFormat(codec, oldFieldInfo.name);
      int fieldMaxIndexOption;
      if (doesntSupportOffsets.contains(pf)) {
        fieldMaxIndexOption = Math.min(maxIndexOptionNoOffsets, maxIndexOption);
      } else {
        fieldMaxIndexOption = maxIndexOption;
      }
    
      // Randomly picked the IndexOptions to index this
      // field with:
      IndexOptions indexOptions = IndexOptions.values()[alwaysTestMax ? fieldMaxIndexOption : random().nextInt(1+fieldMaxIndexOption)];
      boolean doPayloads = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 && allowPayloads;

      newFieldInfoArray[fieldUpto] = new FieldInfo(oldFieldInfo.name,
                                                   true,
                                                   fieldUpto,
                                                   false,
                                                   false,
                                                   doPayloads,
                                                   indexOptions,
                                                   null,
                                                   DocValuesType.NUMERIC,
                                                   null);
    }

    FieldInfos newFieldInfos = new FieldInfos(newFieldInfoArray);

    // Estimate that flushed segment size will be 25% of
    // what we use in RAM:
    long bytes =  totalPostings * 8 + totalPayloadBytes;

    SegmentWriteState writeState = new SegmentWriteState(null, dir,
                                                         segmentInfo, newFieldInfos,
                                                         32, null, new IOContext(new FlushInfo(maxDoc, bytes)));
    FieldsConsumer fieldsConsumer = codec.postingsFormat().fieldsConsumer(writeState);

    for(Map.Entry<String,Map<BytesRef,Long>> fieldEnt : fields.entrySet()) {
      String field = fieldEnt.getKey();
      Map<BytesRef,Long> terms = fieldEnt.getValue();

      FieldInfo fieldInfo = newFieldInfos.fieldInfo(field);

      IndexOptions indexOptions = fieldInfo.getIndexOptions();

      if (VERBOSE) {
        System.out.println("field=" + field + " indexOtions=" + indexOptions);
      }

      boolean doFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      boolean doPos = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      boolean doPayloads = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 && allowPayloads;
      boolean doOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      
      TermsConsumer termsConsumer = fieldsConsumer.addField(fieldInfo);
      long sumTotalTF = 0;
      long sumDF = 0;
      FixedBitSet seenDocs = new FixedBitSet(maxDoc);
      for(Map.Entry<BytesRef,Long> termEnt : terms.entrySet()) {
        BytesRef term = termEnt.getKey();
        SeedPostings postings = getSeedPostings(term.utf8ToString(), termEnt.getValue(), false, maxAllowed);
        if (VERBOSE) {
          System.out.println("  term=" + field + ":" + term.utf8ToString() + " docFreq=" + postings.docFreq + " seed=" + termEnt.getValue());
        }
        
        PostingsConsumer postingsConsumer = termsConsumer.startTerm(term);
        long totalTF = 0;
        int docID = 0;
        while((docID = postings.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
          final int freq = postings.freq();
          if (VERBOSE) {
            System.out.println("    " + postings.upto + ": docID=" + docID + " freq=" + postings.freq);
          }
          postingsConsumer.startDoc(docID, doFreq ? postings.freq : -1);
          seenDocs.set(docID);
          if (doPos) {
            totalTF += postings.freq;
            for(int posUpto=0;posUpto<freq;posUpto++) {
              int pos = postings.nextPosition();
              BytesRef payload = postings.getPayload();

              if (VERBOSE) {
                if (doPayloads) {
                  System.out.println("      pos=" + pos + " payload=" + (payload == null ? "null" : payload.length + " bytes"));
                } else {
                  System.out.println("      pos=" + pos);
                }
              }
              postingsConsumer.addPosition(pos, doPayloads ? payload : null,
                                           doOffsets ? postings.startOffset() : -1,
                                           doOffsets ? postings.endOffset() : -1);
            }
          } else if (doFreq) {
            totalTF += freq;
          } else {
            totalTF++;
          }
          postingsConsumer.finishDoc();
        }
        termsConsumer.finishTerm(term, new TermStats(postings.docFreq, doFreq ? totalTF : -1));
        sumTotalTF += totalTF;
        sumDF += postings.docFreq;
      }

      termsConsumer.finish(doFreq ? sumTotalTF : -1, sumDF, seenDocs.cardinality());
    }

    fieldsConsumer.close();

    if (VERBOSE) {
      System.out.println("TEST: after indexing: files=");
      for(String file : dir.listAll()) {
        System.out.println("  " + file + ": " + dir.fileLength(file) + " bytes");
      }
    }

    currentFieldInfos = newFieldInfos;

    SegmentReadState readState = new SegmentReadState(dir, segmentInfo, newFieldInfos, IOContext.DEFAULT, 1);

    return codec.postingsFormat().fieldsProducer(readState);
  }

  private static class ThreadState {
    // Only used with REUSE option:
    public DocsEnum reuseDocsEnum;
    public DocsAndPositionsEnum reuseDocsAndPositionsEnum;
  }

  private void verifyEnum(ThreadState threadState,
                          String field,
                          BytesRef term,
                          TermsEnum termsEnum,

                          // Maximum options (docs/freqs/positions/offsets) to test:
                          IndexOptions maxTestOptions,

                          IndexOptions maxIndexOptions,

                          EnumSet<Option> options,
                          boolean alwaysTestMax) throws IOException {
        
    if (VERBOSE) {
      System.out.println("  verifyEnum: options=" + options + " maxTestOptions=" + maxTestOptions);
    }

    // 50% of the time time pass liveDocs:
    boolean useLiveDocs = options.contains(Option.LIVE_DOCS) && random().nextBoolean();
    Bits liveDocs;
    if (useLiveDocs) {
      liveDocs = globalLiveDocs;
      if (VERBOSE) {
        System.out.println("  use liveDocs");
      }
    } else {
      liveDocs = null;
      if (VERBOSE) {
        System.out.println("  no liveDocs");
      }
    }

    FieldInfo fieldInfo = currentFieldInfos.fieldInfo(field);

    // NOTE: can be empty list if we are using liveDocs:
    SeedPostings expected = getSeedPostings(term.utf8ToString(), 
                                            fields.get(field).get(term),
                                            useLiveDocs,
                                            maxIndexOptions);
    assertEquals(expected.docFreq, termsEnum.docFreq());

    boolean allowFreqs = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0 &&
      maxTestOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean doCheckFreqs = allowFreqs && (alwaysTestMax || random().nextInt(3) <= 2);

    boolean allowPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 &&
      maxTestOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean doCheckPositions = allowPositions && (alwaysTestMax || random().nextInt(3) <= 2);

    boolean allowOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0 &&
      maxTestOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    boolean doCheckOffsets = allowOffsets && (alwaysTestMax || random().nextInt(3) <= 2);

    boolean doCheckPayloads = options.contains(Option.PAYLOADS) && allowPositions && fieldInfo.hasPayloads() && (alwaysTestMax || random().nextInt(3) <= 2);

    DocsEnum prevDocsEnum = null;

    DocsEnum docsEnum;
    DocsAndPositionsEnum docsAndPositionsEnum;

    if (!doCheckPositions) {
      if (allowPositions && random().nextInt(10) == 7) {
        // 10% of the time, even though we will not check positions, pull a DocsAndPositions enum
        
        if (options.contains(Option.REUSE_ENUMS) && random().nextInt(10) < 9) {
          prevDocsEnum = threadState.reuseDocsAndPositionsEnum;
        }

        int flags = 0;
        if (alwaysTestMax || random().nextBoolean()) {
          flags |= DocsAndPositionsEnum.FLAG_OFFSETS;
        }
        if (alwaysTestMax || random().nextBoolean()) {
          flags |= DocsAndPositionsEnum.FLAG_PAYLOADS;
        }

        if (VERBOSE) {
          System.out.println("  get DocsAndPositionsEnum (but we won't check positions) flags=" + flags);
        }

        threadState.reuseDocsAndPositionsEnum = termsEnum.docsAndPositions(liveDocs, (DocsAndPositionsEnum) prevDocsEnum, flags);
        docsEnum = threadState.reuseDocsAndPositionsEnum;
        docsAndPositionsEnum = threadState.reuseDocsAndPositionsEnum;
      } else {
        if (VERBOSE) {
          System.out.println("  get DocsEnum");
        }
        if (options.contains(Option.REUSE_ENUMS) && random().nextInt(10) < 9) {
          prevDocsEnum = threadState.reuseDocsEnum;
        }
        threadState.reuseDocsEnum = termsEnum.docs(liveDocs, prevDocsEnum, doCheckFreqs ? DocsEnum.FLAG_FREQS : DocsEnum.FLAG_NONE);
        docsEnum = threadState.reuseDocsEnum;
        docsAndPositionsEnum = null;
      }
    } else {
      if (options.contains(Option.REUSE_ENUMS) && random().nextInt(10) < 9) {
        prevDocsEnum = threadState.reuseDocsAndPositionsEnum;
      }

      int flags = 0;
      if (alwaysTestMax || doCheckOffsets || random().nextInt(3) == 1) {
        flags |= DocsAndPositionsEnum.FLAG_OFFSETS;
      }
      if (alwaysTestMax || doCheckPayloads|| random().nextInt(3) == 1) {
        flags |= DocsAndPositionsEnum.FLAG_PAYLOADS;
      }

      if (VERBOSE) {
        System.out.println("  get DocsAndPositionsEnum flags=" + flags);
      }

      threadState.reuseDocsAndPositionsEnum = termsEnum.docsAndPositions(liveDocs, (DocsAndPositionsEnum) prevDocsEnum, flags);
      docsEnum = threadState.reuseDocsAndPositionsEnum;
      docsAndPositionsEnum = threadState.reuseDocsAndPositionsEnum;
    }

    assertNotNull("null DocsEnum", docsEnum);
    int initialDocID = docsEnum.docID();
    assertEquals("inital docID should be -1" + docsEnum, -1, initialDocID);

    if (VERBOSE) {
      if (prevDocsEnum == null) {
        System.out.println("  got enum=" + docsEnum);
      } else if (prevDocsEnum == docsEnum) {
        System.out.println("  got reuse enum=" + docsEnum);
      } else {
        System.out.println("  got enum=" + docsEnum + " (reuse of " + prevDocsEnum + " failed)");
      }
    }

    // 10% of the time don't consume all docs:
    int stopAt;
    if (!alwaysTestMax && options.contains(Option.PARTIAL_DOC_CONSUME) && expected.docFreq > 1 && random().nextInt(10) == 7) {
      stopAt = random().nextInt(expected.docFreq-1);
      if (VERBOSE) {
        System.out.println("  will not consume all docs (" + stopAt + " vs " + expected.docFreq + ")");
      }
    } else {
      stopAt = expected.docFreq;
      if (VERBOSE) {
        System.out.println("  consume all docs");
      }
    }

    double skipChance = alwaysTestMax ? 0.5 : random().nextDouble();
    int numSkips = expected.docFreq < 3 ? 1 : _TestUtil.nextInt(random(), 1, Math.min(20, expected.docFreq/3));
    int skipInc = expected.docFreq/numSkips;
    int skipDocInc = maxDoc/numSkips;

    // Sometimes do 100% skipping:
    boolean doAllSkipping = options.contains(Option.SKIPPING) && random().nextInt(7) == 1;

    double freqAskChance = alwaysTestMax ? 1.0 : random().nextDouble();
    double payloadCheckChance = alwaysTestMax ? 1.0 : random().nextDouble();
    double offsetCheckChance = alwaysTestMax ? 1.0 : random().nextDouble();

    if (VERBOSE) {
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
          assertEquals("DocsEnum should have ended but didn't", DocsEnum.NO_MORE_DOCS, docsEnum.nextDoc());

          // Common bug is to forget to set this.doc=NO_MORE_DOCS in the enum!:
          assertEquals("DocsEnum should have ended but didn't", DocsEnum.NO_MORE_DOCS, docsEnum.docID());
        }
        break;
      }

      if (options.contains(Option.SKIPPING) && (doAllSkipping || random().nextDouble() <= skipChance)) {
        int targetDocID = -1;
        if (expected.upto < stopAt && random().nextBoolean()) {
          // Pick target we know exists:
          final int skipCount = _TestUtil.nextInt(random(), 1, skipInc);
          for(int skip=0;skip<skipCount;skip++) {
            if (expected.nextDoc() == DocsEnum.NO_MORE_DOCS) {
              break;
            }
          }
        } else {
          // Pick random target (might not exist):
          final int skipDocIDs = _TestUtil.nextInt(random(), 1, skipDocInc);
          if (skipDocIDs > 0) {
            targetDocID = expected.docID() + skipDocIDs;
            expected.advance(targetDocID);
          }
        }

        if (expected.upto >= stopAt) {
          int target = random().nextBoolean() ? maxDoc : DocsEnum.NO_MORE_DOCS;
          if (VERBOSE) {
            System.out.println("  now advance to end (target=" + target + ")");
          }
          assertEquals("DocsEnum should have ended but didn't", DocsEnum.NO_MORE_DOCS, docsEnum.advance(target));
          break;
        } else {
          if (VERBOSE) {
            if (targetDocID != -1) {
              System.out.println("  now advance to random target=" + targetDocID + " (" + expected.upto + " of " + stopAt + ") current=" + docsEnum.docID());
            } else {
              System.out.println("  now advance to known-exists target=" + expected.docID() + " (" + expected.upto + " of " + stopAt + ") current=" + docsEnum.docID());
            }
          }
          int docID = docsEnum.advance(targetDocID != -1 ? targetDocID : expected.docID());
          assertEquals("docID is wrong", expected.docID(), docID);
        }
      } else {
        expected.nextDoc();
        if (VERBOSE) {
          System.out.println("  now nextDoc to " + expected.docID() + " (" + expected.upto + " of " + stopAt + ")");
        }
        int docID = docsEnum.nextDoc();
        assertEquals("docID is wrong", expected.docID(), docID);
        if (docID == DocsEnum.NO_MORE_DOCS) {
          break;
        }
      }

      if (doCheckFreqs && random().nextDouble() <= freqAskChance) {
        if (VERBOSE) {
          System.out.println("    now freq()=" + expected.freq());
        }
        int freq = docsEnum.freq();
        assertEquals("freq is wrong", expected.freq(), freq);
      }

      if (doCheckPositions) {
        int freq = docsEnum.freq();
        int numPosToConsume;
        if (!alwaysTestMax && options.contains(Option.PARTIAL_POS_CONSUME) && random().nextInt(5) == 1) {
          numPosToConsume = random().nextInt(freq);
        } else {
          numPosToConsume = freq;
        }

        for(int i=0;i<numPosToConsume;i++) {
          int pos = expected.nextPosition();
          if (VERBOSE) {
            System.out.println("    now nextPosition to " + pos);
          }
          assertEquals("position is wrong", pos, docsAndPositionsEnum.nextPosition());

          if (doCheckPayloads) {
            BytesRef expectedPayload = expected.getPayload();
            if (random().nextDouble() <= payloadCheckChance) {
              if (VERBOSE) {
                System.out.println("      now check expectedPayload length=" + (expectedPayload == null ? 0 : expectedPayload.length));
              }
              if (expectedPayload == null || expectedPayload.length == 0) {
                assertNull("should not have payload", docsAndPositionsEnum.getPayload());
              } else {
                BytesRef payload = docsAndPositionsEnum.getPayload();
                assertNotNull("should have payload but doesn't", payload);

                assertEquals("payload length is wrong", expectedPayload.length, payload.length);
                for(int byteUpto=0;byteUpto<expectedPayload.length;byteUpto++) {
                  assertEquals("payload bytes are wrong",
                               expectedPayload.bytes[expectedPayload.offset + byteUpto],
                               payload.bytes[payload.offset+byteUpto]);
                }
                
                // make a deep copy
                payload = BytesRef.deepCopyOf(payload);
                assertEquals("2nd call to getPayload returns something different!", payload, docsAndPositionsEnum.getPayload());
              }
            } else {
              if (VERBOSE) {
                System.out.println("      skip check payload length=" + (expectedPayload == null ? 0 : expectedPayload.length));
              }
            }
          }

          if (doCheckOffsets) {
            if (random().nextDouble() <= offsetCheckChance) {
              if (VERBOSE) {
                System.out.println("      now check offsets: startOff=" + expected.startOffset() + " endOffset=" + expected.endOffset());
              }
              assertEquals("startOffset is wrong", expected.startOffset(), docsAndPositionsEnum.startOffset());
              assertEquals("endOffset is wrong", expected.endOffset(), docsAndPositionsEnum.endOffset());
            } else {
              if (VERBOSE) {
                System.out.println("      skip check offsets");
              }
            }
          } else if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) < 0) {
            if (VERBOSE) {
              System.out.println("      now check offsets are -1");
            }
            assertEquals("startOffset isn't -1", -1, docsAndPositionsEnum.startOffset());
            assertEquals("endOffset isn't -1", -1, docsAndPositionsEnum.endOffset());
          }
        }
      }
    }
  }

  private static class TestThread extends Thread {
    private Fields fieldsSource;
    private EnumSet<Option> options;
    private IndexOptions maxIndexOptions;
    private IndexOptions maxTestOptions;
    private boolean alwaysTestMax;
    private BasePostingsFormatTestCase testCase;

    public TestThread(BasePostingsFormatTestCase testCase, Fields fieldsSource, EnumSet<Option> options, IndexOptions maxTestOptions,
                      IndexOptions maxIndexOptions, boolean alwaysTestMax) {
      this.fieldsSource = fieldsSource;
      this.options = options;
      this.maxTestOptions = maxTestOptions;
      this.maxIndexOptions = maxIndexOptions;
      this.alwaysTestMax = alwaysTestMax;
      this.testCase = testCase;
    }

    @Override
    public void run() {
      try {
        try {
          testCase.testTermsOneThread(fieldsSource, options, maxTestOptions, maxIndexOptions, alwaysTestMax);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      } finally {
        fieldsSource = null;
        testCase = null;
      }
    }
  }

  private void testTerms(final Fields fieldsSource, final EnumSet<Option> options,
                         final IndexOptions maxTestOptions,
                         final IndexOptions maxIndexOptions,
                         final boolean alwaysTestMax) throws Exception {

    if (options.contains(Option.THREADS)) {
      int numThreads = _TestUtil.nextInt(random(), 2, 5);
      Thread[] threads = new Thread[numThreads];
      for(int threadUpto=0;threadUpto<numThreads;threadUpto++) {
        threads[threadUpto] = new TestThread(this, fieldsSource, options, maxTestOptions, maxIndexOptions, alwaysTestMax);
        threads[threadUpto].start();
      }
      for(int threadUpto=0;threadUpto<numThreads;threadUpto++) {
        threads[threadUpto].join();
      }
    } else {
      testTermsOneThread(fieldsSource, options, maxTestOptions, maxIndexOptions, alwaysTestMax);
    }
  }

  private void testTermsOneThread(Fields fieldsSource, EnumSet<Option> options,
                                  IndexOptions maxTestOptions,
                                  IndexOptions maxIndexOptions, boolean alwaysTestMax) throws IOException {

    ThreadState threadState = new ThreadState();

    // Test random terms/fields:
    List<TermState> termStates = new ArrayList<TermState>();
    List<FieldAndTerm> termStateTerms = new ArrayList<FieldAndTerm>();
    
    Collections.shuffle(allTerms, random());
    int upto = 0;
    while (upto < allTerms.size()) {

      boolean useTermState = termStates.size() != 0 && random().nextInt(5) == 1;
      FieldAndTerm fieldAndTerm;
      TermsEnum termsEnum;

      TermState termState = null;

      if (!useTermState) {
        // Seek by random field+term:
        fieldAndTerm = allTerms.get(upto++);
        if (VERBOSE) {
          System.out.println("\nTEST: seek to term=" + fieldAndTerm.field + ":" + fieldAndTerm.term.utf8ToString() );
        }
      } else {
        // Seek by previous saved TermState
        int idx = random().nextInt(termStates.size());
        fieldAndTerm = termStateTerms.get(idx);
        if (VERBOSE) {
          System.out.println("\nTEST: seek using TermState to term=" + fieldAndTerm.field + ":" + fieldAndTerm.term.utf8ToString());
        }
        termState = termStates.get(idx);
      }

      Terms terms = fieldsSource.terms(fieldAndTerm.field);
      assertNotNull(terms);
      termsEnum = terms.iterator(null);

      if (!useTermState) {
        assertTrue(termsEnum.seekExact(fieldAndTerm.term, true));
      } else {
        termsEnum.seekExact(fieldAndTerm.term, termState);
      }

      boolean savedTermState = false;

      if (options.contains(Option.TERM_STATE) && !useTermState && random().nextInt(5) == 1) {
        // Save away this TermState:
        termStates.add(termsEnum.termState());
        termStateTerms.add(fieldAndTerm);
        savedTermState = true;
      }

      verifyEnum(threadState,
                 fieldAndTerm.field,
                 fieldAndTerm.term,
                 termsEnum,
                 maxTestOptions,
                 maxIndexOptions,
                 options,
                 alwaysTestMax);

      // Sometimes save term state after pulling the enum:
      if (options.contains(Option.TERM_STATE) && !useTermState && !savedTermState && random().nextInt(5) == 1) {
        // Save away this TermState:
        termStates.add(termsEnum.termState());
        termStateTerms.add(fieldAndTerm);
        useTermState = true;
      }

      // 10% of the time make sure you can pull another enum
      // from the same term:
      if (alwaysTestMax || random().nextInt(10) == 7) {
        // Try same term again
        if (VERBOSE) {
          System.out.println("TEST: try enum again on same term");
        }

        verifyEnum(threadState,
                   fieldAndTerm.field,
                   fieldAndTerm.term,
                   termsEnum,
                   maxTestOptions,
                   maxIndexOptions,
                   options,
                   alwaysTestMax);
      }
    }
  }
  
  private void testFields(Fields fields) throws Exception {
    Iterator<String> iterator = fields.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      try {
        iterator.remove();
        fail("Fields.iterator() allows for removal");
      } catch (UnsupportedOperationException expected) {
        // expected;
      }
    }
    assertFalse(iterator.hasNext());
    try {
      iterator.next();
      fail("Fields.iterator() doesn't throw NoSuchElementException when past the end");
    } catch (NoSuchElementException expected) {
      // expected
    }
  }

  /** Indexes all fields/terms at the specified
   *  IndexOptions, and fully tests at that IndexOptions. */
  private void testFull(IndexOptions options, boolean withPayloads) throws Exception {
    File path = _TestUtil.getTempDir("testPostingsFormat.testExact");
    Directory dir = newFSDirectory(path);

    // TODO test thread safety of buildIndex too
    FieldsProducer fieldsProducer = buildIndex(dir, options, withPayloads, true);

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
    _TestUtil.rmDir(path);
  }

  public void testDocsOnly() throws Exception {
    testFull(IndexOptions.DOCS_ONLY, false);
  }

  public void testDocsAndFreqs() throws Exception {
    testFull(IndexOptions.DOCS_AND_FREQS, false);
  }

  public void testDocsAndFreqsAndPositions() throws Exception {
    testFull(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, false);
  }

  public void testDocsAndFreqsAndPositionsAndPayloads() throws Exception {
    testFull(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
  }

  public void testDocsAndFreqsAndPositionsAndOffsets() throws Exception {
    testFull(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, false);
  }

  public void testDocsAndFreqsAndPositionsAndOffsetsAndPayloads() throws Exception {
    testFull(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, true);
  }

  public void testRandom() throws Exception {

    int iters = 5;

    for(int iter=0;iter<iters;iter++) {
      File path = _TestUtil.getTempDir("testPostingsFormat");
      Directory dir = newFSDirectory(path);

      boolean indexPayloads = random().nextBoolean();
      // TODO test thread safety of buildIndex too
      FieldsProducer fieldsProducer = buildIndex(dir, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, indexPayloads, false);

      testFields(fieldsProducer);

      // NOTE: you can also test "weaker" index options than
      // you indexed with:
      testTerms(fieldsProducer, EnumSet.allOf(Option.class), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, false);

      fieldsProducer.close();
      fieldsProducer = null;

      dir.close();
      _TestUtil.rmDir(path);
    }
  }
}
