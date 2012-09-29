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
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
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

// TODO hmm contract says .doc() can return NO_MORE_DOCS
// before nextDoc too...?

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
    THREADS};

  private static class FieldAndTerm {
    String field;
    BytesRef term;

    public FieldAndTerm(String field, BytesRef term) {
      this.field = field;
      this.term = BytesRef.deepCopyOf(term);
    }
  }

  private static class Position {
    int position;
    byte[] payload;
    int startOffset;
    int endOffset;
  }

  private static class Posting implements Comparable<Posting> {
    int docID;
    List<Position> positions;

    public int compareTo(Posting other) {
      return docID - other.docID;
    }
  }

  // Holds all postings:
  private static Map<String,Map<BytesRef,List<Posting>>> fields;

  // Holds only live doc postings:
  private static Map<String,Map<BytesRef,List<Posting>>> fieldsLive;

  private static FieldInfos fieldInfos;

  private static int maxDocID;

  private static FixedBitSet globalLiveDocs;

  private static List<FieldAndTerm> allTerms;

  private static long totalPostings;
  private static long totalPayloadBytes;

  @BeforeClass
  public static void createPostings() throws IOException {
    fields = new TreeMap<String,Map<BytesRef,List<Posting>>>();
    fieldsLive = new TreeMap<String,Map<BytesRef,List<Posting>>>();

    final int numFields = _TestUtil.nextInt(random(), 1, 5);
    if (VERBOSE) {
      System.out.println("TEST: " + numFields + " fields");
    }

    FieldInfo[] fieldInfoArray = new FieldInfo[numFields];
    int fieldUpto = 0;
    int numMediumTerms = 0;
    int numBigTerms = 0;
    int numManyPositions = 0;
    totalPostings = 0;
    totalPayloadBytes = 0;
    while (fieldUpto < numFields) {
      String field = _TestUtil.randomSimpleString(random());
      if (fields.containsKey(field)) {
        continue;
      }

      fieldInfoArray[fieldUpto] = new FieldInfo(field, true, fieldUpto, false, false, true,
                                                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                                                null, DocValues.Type.FIXED_INTS_8, null);
      fieldUpto++;

      Map<BytesRef,List<Posting>> postings = new TreeMap<BytesRef,List<Posting>>();
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

        int numDocs;
        if (numBigTerms == 0 || (random().nextInt(10) == 3 && numBigTerms < 2)) {
          // Make at least 1 big term, then maybe (~10%
          // chance) make another:
          numDocs = RANDOM_MULTIPLIER * _TestUtil.nextInt(random(), 50000, 70000);
          numBigTerms++;
          term = "big_" + term;
        } else if (numMediumTerms == 0 || (random().nextInt(10) == 3 && numMediumTerms < 5)) {
          // Make at least 1 medium term, then maybe (~10%
          // chance) make up to 4 more:
          numDocs = RANDOM_MULTIPLIER * _TestUtil.nextInt(random(), 3000, 6000);
          numMediumTerms++;
          term = "medium_" + term;
        } else if (random().nextBoolean()) {
          // Low freq term:
          numDocs = RANDOM_MULTIPLIER * _TestUtil.nextInt(random(), 1, 40);
          term = "low_" + term;
        } else {
          // Very low freq term (don't multiply by RANDOM_MULTIPLIER):
          numDocs = _TestUtil.nextInt(random(), 1, 3);
          term = "verylow_" + term;
        }

        List<Posting> termPostings = new ArrayList<Posting>();
        postings.put(new BytesRef(term), termPostings);

        int docID = 0;

        // TODO: more realistic to inversely tie this to numDocs:
        int maxDocSpacing = _TestUtil.nextInt(random(), 1, 100);

        int payloadSize;
        if (random().nextInt(10) == 7) {
          // 10% of the time create big payloads:
          payloadSize = 1 + random().nextInt(3);
        } else {
          payloadSize = 1 + random().nextInt(1);
        }

        boolean fixedPayloads = random().nextBoolean();

        for(int docUpto=0;docUpto<numDocs;docUpto++) {
          if (docUpto == 0 && random().nextBoolean()) {
            // Sometimes index docID = 0
          } else if (maxDocSpacing == 1) {
            docID++;
          } else {
            // TODO: sometimes have a biggish gap here!
            docID += _TestUtil.nextInt(random(), 1, maxDocSpacing);
          }

          Posting posting = new Posting();
          posting.docID = docID;
          maxDocID = Math.max(docID, maxDocID);
          posting.positions = new ArrayList<Position>();
          termPostings.add(posting);

          int freq;
          if (random().nextInt(30) == 17 && numManyPositions < 10) {
            freq = _TestUtil.nextInt(random(), 1, 1000);
            numManyPositions++;
          } else {
            freq = _TestUtil.nextInt(random(), 1, 20);
          }
          int pos = 0;
          int offset = 0;
          int posSpacing = _TestUtil.nextInt(random(), 1, 100);
          totalPostings += freq;
          for(int posUpto=0;posUpto<freq;posUpto++) {
            if (posUpto == 0 && random().nextBoolean()) {
              // Sometimes index pos = 0
            } else if (posSpacing == 1) {
              pos++;
            } else {
              pos += _TestUtil.nextInt(random(), 1, posSpacing);
            }

            Position position = new Position();
            posting.positions.add(position);
            position.position = pos;
            if (payloadSize != 0) {
              if (fixedPayloads) {
                position.payload = new byte[payloadSize];
              } else {
                int thisPayloadSize = random().nextInt(payloadSize);
                if (thisPayloadSize != 0) {
                  position.payload = new byte[thisPayloadSize];
                }
              }
            }

            if (position.payload != null) {
              random().nextBytes(position.payload); 
              totalPayloadBytes += position.payload.length;
            }

            position.startOffset = offset + random().nextInt(5);
            position.endOffset = position.startOffset + random().nextInt(10);
            offset = position.endOffset;
          }
        }
      }
    }

    fieldInfos = new FieldInfos(fieldInfoArray);

    globalLiveDocs = new FixedBitSet(1+maxDocID);
    double liveRatio = random().nextDouble();
    for(int i=0;i<1+maxDocID;i++) {
      if (random().nextDouble() <= liveRatio) {
        globalLiveDocs.set(i);
      }
    }

    // Pre-filter postings by globalLiveDocs:
    for(Map.Entry<String,Map<BytesRef,List<Posting>>> fieldEnt : fields.entrySet()) {
      Map<BytesRef,List<Posting>> postingsLive = new TreeMap<BytesRef,List<Posting>>();
      fieldsLive.put(fieldEnt.getKey(), postingsLive);
      for(Map.Entry<BytesRef,List<Posting>> termEnt : fieldEnt.getValue().entrySet()) {
        List<Posting> termPostingsLive = new ArrayList<Posting>();
        postingsLive.put(termEnt.getKey(), termPostingsLive);
        for(Posting posting : termEnt.getValue()) {
          if (globalLiveDocs.get(posting.docID)) {
            termPostingsLive.add(posting);
          }
        }
      }
    }

    allTerms = new ArrayList<FieldAndTerm>();
    for(Map.Entry<String,Map<BytesRef,List<Posting>>> fieldEnt : fields.entrySet()) {
      String field = fieldEnt.getKey();
      for(Map.Entry<BytesRef,List<Posting>> termEnt : fieldEnt.getValue().entrySet()) {
        allTerms.add(new FieldAndTerm(field, termEnt.getKey()));
      }
    }

    if (VERBOSE) {
      System.out.println("TEST: done init postings; maxDocID=" + maxDocID + "; " + allTerms.size() + " total terms, across " + fieldInfos.size() + " fields");
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    allTerms = null;
    fieldInfos = null;
    fields = null;
    fieldsLive = null;
    globalLiveDocs = null;
  }

  // TODO maybe instead of @BeforeClass just make a single test run: build postings & index & test it?

  private FieldInfos currentFieldInfos;

  // maxAllowed = the "highest" we can index, but we will still
  // randomly index at lower IndexOption
  private FieldsProducer buildIndex(Directory dir, IndexOptions maxAllowed, boolean allowPayloads, boolean alwaysTestMax) throws IOException {
    Codec codec = getCodec();
    SegmentInfo segmentInfo = new SegmentInfo(dir, Constants.LUCENE_MAIN_VERSION, "_0", 1+maxDocID, false, codec, null, null);

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
                                                   DocValues.Type.FIXED_INTS_8,
                                                   null);
    }

    FieldInfos newFieldInfos = new FieldInfos(newFieldInfoArray);

    // Estimate that flushed segment size will be 25% of
    // what we use in RAM:
    long bytes =  totalPostings * 8 + totalPayloadBytes;

    SegmentWriteState writeState = new SegmentWriteState(null, dir,
                                                         segmentInfo, newFieldInfos,
                                                         32, null, new IOContext(new FlushInfo(maxDocID, bytes)));
    FieldsConsumer fieldsConsumer = codec.postingsFormat().fieldsConsumer(writeState);

    for(Map.Entry<String,Map<BytesRef,List<Posting>>> fieldEnt : fields.entrySet()) {
      String field = fieldEnt.getKey();
      Map<BytesRef,List<Posting>> terms = fieldEnt.getValue();

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
      FixedBitSet seenDocs = new FixedBitSet(maxDocID+1);
      for(Map.Entry<BytesRef,List<Posting>> termEnt : terms.entrySet()) {
        BytesRef term = termEnt.getKey();
        List<Posting> postings = termEnt.getValue();
        if (VERBOSE) {
          System.out.println("  term=" + field + ":" + term.utf8ToString() + " docFreq=" + postings.size());
        }
        
        PostingsConsumer postingsConsumer = termsConsumer.startTerm(term);
        long totalTF = 0;
        int docCount = 0;
        for(Posting posting : postings) {
          if (VERBOSE) {
            System.out.println("    " + docCount + ": docID=" + posting.docID + " freq=" + posting.positions.size());
          }
          postingsConsumer.startDoc(posting.docID, doFreq ? posting.positions.size() : -1);
          seenDocs.set(posting.docID);
          if (doPos) {
            totalTF += posting.positions.size();
            for(Position pos : posting.positions) {
              if (VERBOSE) {
                if (doPayloads) {
                  System.out.println("      pos=" + pos.position + " payload=" + (pos.payload == null ? "null" : pos.payload.length + " bytes"));
                } else {
                  System.out.println("      pos=" + pos.position);
                }
              }
              postingsConsumer.addPosition(pos.position, (doPayloads && pos.payload != null) ? new BytesRef(pos.payload) : null, doOffsets ? pos.startOffset : -1, doOffsets ? pos.endOffset : -1);
            }
          } else if (doFreq) {
            totalTF += posting.positions.size();
          } else {
            totalTF++;
          }
          postingsConsumer.finishDoc();
          docCount++;
        }
        termsConsumer.finishTerm(term, new TermStats(postings.size(), doFreq ? totalTF : -1));
        sumTotalTF += totalTF;
        sumDF += postings.size();
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
                          IndexOptions maxIndexOptions,

                          EnumSet<Option> options,
                          boolean alwaysTestMax) throws IOException {
        
    if (VERBOSE) {
      System.out.println("  verifyEnum: options=" + options + " maxIndexOptions=" + maxIndexOptions);
    }

    // 50% of the time time pass liveDocs:
    Bits liveDocs;
    Map<String,Map<BytesRef,List<Posting>>> fieldsToUse;
    if (options.contains(Option.LIVE_DOCS) && random().nextBoolean()) {
      liveDocs = globalLiveDocs;
      fieldsToUse = fieldsLive;
      if (VERBOSE) {
        System.out.println("  use liveDocs");
      }
    } else {
      liveDocs = null;
      fieldsToUse = fields;
      if (VERBOSE) {
        System.out.println("  no liveDocs");
      }
    }

    FieldInfo fieldInfo = currentFieldInfos.fieldInfo(field);

    assertEquals(fields.get(field).get(term).size(), termsEnum.docFreq());

    // NOTE: can be empty list if we are using liveDocs:
    List<Posting> expected = fieldsToUse.get(field).get(term);
    
    boolean allowFreqs = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0 &&
      maxIndexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean doCheckFreqs = allowFreqs && (alwaysTestMax || random().nextInt(3) <= 2);

    boolean allowPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 &&
      maxIndexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean doCheckPositions = allowPositions && (alwaysTestMax || random().nextInt(3) <= 2);

    boolean allowOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0 &&
      maxIndexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
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
        threadState.reuseDocsEnum = termsEnum.docs(liveDocs, prevDocsEnum, doCheckFreqs ? DocsEnum.FLAG_FREQS : 0);
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
    assertTrue("inital docID should be -1 or NO_MORE_DOCS: " + docsEnum, initialDocID == -1 || initialDocID == DocsEnum.NO_MORE_DOCS);

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
    if (!alwaysTestMax && options.contains(Option.PARTIAL_DOC_CONSUME) && expected.size() > 1 && random().nextInt(10) == 7) {
      stopAt = random().nextInt(expected.size()-1);
      if (VERBOSE) {
        System.out.println("  will not consume all docs (" + stopAt + " vs " + expected.size() + ")");
      }
    } else {
      stopAt = expected.size();
      if (VERBOSE) {
        System.out.println("  consume all docs");
      }
    }

    double skipChance = alwaysTestMax ? 0.5 : random().nextDouble();
    int numSkips = expected.size() < 3 ? 1 : _TestUtil.nextInt(random(), 1, Math.min(20, expected.size()/3));
    int skipInc = expected.size()/numSkips;
    int skipDocInc = (1+maxDocID)/numSkips;

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

    int nextPosting = 0;
    while (nextPosting <= stopAt) {
      if (nextPosting == stopAt) {
        if (stopAt == expected.size()) {
          assertEquals("DocsEnum should have ended but didn't", DocsEnum.NO_MORE_DOCS, docsEnum.nextDoc());

          // Common bug is to forget to set this.doc=NO_MORE_DOCS in the enum!:
          assertEquals("DocsEnum should have ended but didn't", DocsEnum.NO_MORE_DOCS, docsEnum.docID());
        }
        break;
      }

      Posting posting;
      if (options.contains(Option.SKIPPING) && (doAllSkipping || random().nextDouble() <= skipChance)) {
        int targetDocID = -1;
        if (nextPosting < stopAt && random().nextBoolean()) {
          // Pick target we know exists:
          nextPosting = _TestUtil.nextInt(random(), nextPosting, nextPosting+skipInc);
        } else {
          // Pick random target (might not exist):
          Posting target = new Posting();
          target.docID = _TestUtil.nextInt(random(), expected.get(nextPosting).docID, expected.get(nextPosting).docID+skipDocInc);
          targetDocID = target.docID;
          int loc = Collections.binarySearch(expected.subList(nextPosting, expected.size()), target);
          if (loc < 0) {
            loc = -loc-1;
          }
          nextPosting = nextPosting + loc;
        }

        if (nextPosting >= stopAt) {
          int target = random().nextBoolean() ? (maxDocID+1) : DocsEnum.NO_MORE_DOCS;
          if (VERBOSE) {
            System.out.println("  now advance to end (target=" + target + ")");
          }
          assertEquals("DocsEnum should have ended but didn't", DocsEnum.NO_MORE_DOCS, docsEnum.advance(target));
          break;
        } else {
          posting = expected.get(nextPosting++);
          if (VERBOSE) {
            if (targetDocID != -1) {
              System.out.println("  now advance to random target=" + targetDocID + " (" + nextPosting + " of " + stopAt + ")");
            } else {
              System.out.println("  now advance to known-exists target=" + posting.docID + " (" + nextPosting + " of " + stopAt + ")");
            }
          }
          int docID = docsEnum.advance(targetDocID != -1 ? targetDocID : posting.docID);
          assertEquals("docID is wrong", posting.docID, docID);
        }
      } else {
        posting = expected.get(nextPosting++);
        if (VERBOSE) {
          System.out.println("  now nextDoc to " + posting.docID + " (" + nextPosting + " of " + stopAt + ")");
        }
        int docID = docsEnum.nextDoc();
        assertEquals("docID is wrong", posting.docID, docID);
      }

      if (doCheckFreqs && random().nextDouble() <= freqAskChance) {
        if (VERBOSE) {
          System.out.println("    now freq()=" + posting.positions.size());
        }
        int freq = docsEnum.freq();
        assertEquals("freq is wrong", posting.positions.size(), freq);
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
          Position position = posting.positions.get(i);
          if (VERBOSE) {
            System.out.println("    now nextPosition to " + position.position);
          }
          assertEquals("position is wrong", position.position, docsAndPositionsEnum.nextPosition());

          // TODO sometimes don't pull the payload even
          // though we pulled the position

          if (doCheckPayloads) {
            if (random().nextDouble() <= payloadCheckChance) {
              if (VERBOSE) {
                System.out.println("      now check payload length=" + (position.payload == null ? 0 : position.payload.length));
              }
              if (position.payload == null || position.payload.length == 0) {
                assertNull("should not have payload", docsAndPositionsEnum.getPayload());
              } else {
                BytesRef payload = docsAndPositionsEnum.getPayload();
                assertNotNull("should have payload but doesn't", payload);

                assertEquals("payload length is wrong", position.payload.length, payload.length);
                for(int byteUpto=0;byteUpto<position.payload.length;byteUpto++) {
                  assertEquals("payload bytes are wrong",
                               position.payload[byteUpto],
                               payload.bytes[payload.offset+byteUpto]);
                }
                
                // make a deep copy
                payload = BytesRef.deepCopyOf(payload);
                assertEquals("2nd call to getPayload returns something different!", payload, docsAndPositionsEnum.getPayload());
              }
            } else {
              if (VERBOSE) {
                System.out.println("      skip check payload length=" + (position.payload == null ? 0 : position.payload.length));
              }
            }
          }

          if (doCheckOffsets) {
            if (random().nextDouble() <= offsetCheckChance) {
              if (VERBOSE) {
                System.out.println("      now check offsets: startOff=" + position.startOffset + " endOffset=" + position.endOffset);
              }
              assertEquals("startOffset is wrong", position.startOffset, docsAndPositionsEnum.startOffset());
              assertEquals("endOffset is wrong", position.endOffset, docsAndPositionsEnum.endOffset());
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

  private void testTerms(final Fields fieldsSource, final EnumSet<Option> options,
                         final IndexOptions maxIndexOptions,
                         final boolean alwaysTestMax) throws Exception {

    if (options.contains(Option.THREADS)) {
      int numThreads = _TestUtil.nextInt(random(), 2, 5);
      Thread[] threads = new Thread[numThreads];
      for(int threadUpto=0;threadUpto<numThreads;threadUpto++) {
        threads[threadUpto] = new Thread() {
            @Override
            public void run() {
              try {
                testTermsOneThread(fieldsSource, options, maxIndexOptions, alwaysTestMax);
              } catch (Throwable t) {
                throw new RuntimeException(t);
              }
            }
          };
        threads[threadUpto].start();
      }
      for(int threadUpto=0;threadUpto<numThreads;threadUpto++) {
        threads[threadUpto].join();
      }
    } else {
      testTermsOneThread(fieldsSource, options, maxIndexOptions, alwaysTestMax);
    }
  }

  private void testTermsOneThread(Fields fieldsSource, EnumSet<Option> options, IndexOptions maxIndexOptions, boolean alwaysTestMax) throws IOException {

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
                   maxIndexOptions,
                   options,
                   alwaysTestMax);
      }
    }
  }
  
  private void testFields(Fields fields) throws Exception {
    Iterator<String> iterator = fields.iterator();
    while (iterator.hasNext()) {
      String field = iterator.next();
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
      testTerms(fieldsProducer, EnumSet.allOf(Option.class), allOptions[i], true);
      if (withPayloads) {
        // If we indexed w/ payloads, also test enums w/o accessing payloads:
        testTerms(fieldsProducer, EnumSet.complementOf(EnumSet.of(Option.PAYLOADS)), allOptions[i], true);
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

    int iters = atLeast(10);

    for(int iter=0;iter<iters;iter++) {
      File path = _TestUtil.getTempDir("testPostingsFormat");
      Directory dir = newFSDirectory(path);

      boolean indexPayloads = random().nextBoolean();
      // TODO test thread safety of buildIndex too
      FieldsProducer fieldsProducer = buildIndex(dir, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, indexPayloads, false);

      testFields(fieldsProducer);

      // NOTE: you can also test "weaker" index options than
      // you indexed with:
      testTerms(fieldsProducer, EnumSet.allOf(Option.class), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, false);

      fieldsProducer.close();
      dir.close();
      _TestUtil.rmDir(path);
    }
  }
}
