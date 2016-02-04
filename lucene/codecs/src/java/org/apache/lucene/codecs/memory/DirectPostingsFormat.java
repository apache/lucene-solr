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
package org.apache.lucene.codecs.memory;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RunAutomaton;
import org.apache.lucene.util.automaton.Transition;

// TODO:
//   - build depth-N prefix hash?
//   - or: longer dense skip lists than just next byte?

/** Wraps {@link Lucene50PostingsFormat} format for on-disk
 *  storage, but then at read time loads and stores all
 *  terms and postings directly in RAM as byte[], int[].
 *
 *  <p><b>WARNING</b>: This is
 *  exceptionally RAM intensive: it makes no effort to
 *  compress the postings data, storing terms as separate
 *  byte[] and postings as separate int[], but as a result it
 *  gives substantial increase in search performance.
 *
 *  <p>This postings format supports {@link TermsEnum#ord}
 *  and {@link TermsEnum#seekExact(long)}.

 *  <p>Because this holds all term bytes as a single
 *  byte[], you cannot have more than 2.1GB worth of term
 *  bytes in a single segment.
 *
 * @lucene.experimental */

public final class DirectPostingsFormat extends PostingsFormat {

  private final int minSkipCount;
  private final int lowFreqCutoff;

  private final static int DEFAULT_MIN_SKIP_COUNT = 8;
  private final static int DEFAULT_LOW_FREQ_CUTOFF = 32;

  //private static final boolean DEBUG = true;

  // TODO: allow passing/wrapping arbitrary postings format?

  public DirectPostingsFormat() {
    this(DEFAULT_MIN_SKIP_COUNT, DEFAULT_LOW_FREQ_CUTOFF);
  }

  /** minSkipCount is how many terms in a row must have the
   *  same prefix before we put a skip pointer down.  Terms
   *  with docFreq &lt;= lowFreqCutoff will use a single int[]
   *  to hold all docs, freqs, position and offsets; terms
   *  with higher docFreq will use separate arrays. */
  public DirectPostingsFormat(int minSkipCount, int lowFreqCutoff) {
    super("Direct");
    this.minSkipCount = minSkipCount;
    this.lowFreqCutoff = lowFreqCutoff;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return PostingsFormat.forName("Lucene50").fieldsConsumer(state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    FieldsProducer postings = PostingsFormat.forName("Lucene50").fieldsProducer(state);
    if (state.context.context != IOContext.Context.MERGE) {
      FieldsProducer loadedPostings;
      try {
        postings.checkIntegrity();
        loadedPostings = new DirectFields(state, postings, minSkipCount, lowFreqCutoff);
      } finally {
        postings.close();
      }
      return loadedPostings;
    } else {
      // Don't load postings for merge:
      return postings;
    }
  }

  private static final class DirectFields extends FieldsProducer {
    private final Map<String,DirectField> fields = new TreeMap<>();

    public DirectFields(SegmentReadState state, Fields fields, int minSkipCount, int lowFreqCutoff) throws IOException {
      for (String field : fields) {
        this.fields.put(field, new DirectField(state, field, fields.terms(field), minSkipCount, lowFreqCutoff));
      }
    }

    @Override
    public Iterator<String> iterator() {
      return Collections.unmodifiableSet(fields.keySet()).iterator();
    }

    @Override
    public Terms terms(String field) {
      return fields.get(field);
    }

    @Override
    public int size() {
      return fields.size();
    }

    @Override
    public void close() {
    }

    @Override
    public long ramBytesUsed() {
      long sizeInBytes = 0;
      for(Map.Entry<String,DirectField> entry: fields.entrySet()) {
        sizeInBytes += entry.getKey().length() * RamUsageEstimator.NUM_BYTES_CHAR;
        sizeInBytes += entry.getValue().ramBytesUsed();
      }
      return sizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      return Accountables.namedAccountables("field", fields);
    }

    @Override
    public void checkIntegrity() throws IOException {
      // if we read entirely into ram, we already validated.
      // otherwise returned the raw postings reader
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(fields=" + fields.size() + ")";
    }
  }

  private final static class DirectField extends Terms implements Accountable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DirectField.class);

    private static abstract class TermAndSkip implements Accountable {
      public int[] skips;
    }

    private static final class LowFreqTerm extends TermAndSkip {

      private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HighFreqTerm.class);

      public final int[] postings;
      public final byte[] payloads;
      public final int docFreq;
      public final int totalTermFreq;

      public LowFreqTerm(int[] postings, byte[] payloads, int docFreq, int totalTermFreq) {
        this.postings = postings;
        this.payloads = payloads;
        this.docFreq = docFreq;
        this.totalTermFreq = totalTermFreq;
      }

      @Override
      public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED +
            ((postings!=null) ? RamUsageEstimator.sizeOf(postings) : 0) +
            ((payloads!=null) ? RamUsageEstimator.sizeOf(payloads) : 0);
      }

      @Override
      public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
      }

    }

    // TODO: maybe specialize into prx/no-prx/no-frq cases?
    private static final class HighFreqTerm extends TermAndSkip {

      private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HighFreqTerm.class);

      public final long totalTermFreq;
      public final int[] docIDs;
      public final int[] freqs;
      public final int[][] positions;
      public final byte[][][] payloads;

      public HighFreqTerm(int[] docIDs, int[] freqs, int[][] positions, byte[][][] payloads, long totalTermFreq) {
        this.docIDs = docIDs;
        this.freqs = freqs;
        this.positions = positions;
        this.payloads = payloads;
        this.totalTermFreq = totalTermFreq;
      }

      @Override
      public long ramBytesUsed() {
        long sizeInBytes = BASE_RAM_BYTES_USED;
        sizeInBytes += (docIDs!=null)? RamUsageEstimator.sizeOf(docIDs) : 0;
        sizeInBytes += (freqs!=null)? RamUsageEstimator.sizeOf(freqs) : 0;

        if(positions != null) {
          sizeInBytes += RamUsageEstimator.shallowSizeOf(positions);
          for(int[] position : positions) {
            sizeInBytes += (position!=null) ? RamUsageEstimator.sizeOf(position) : 0;
          }
        }

        if (payloads != null) {
          sizeInBytes += RamUsageEstimator.shallowSizeOf(payloads);
          for(byte[][] payload : payloads) {
            if(payload != null) {
              sizeInBytes += RamUsageEstimator.shallowSizeOf(payload);
              for(byte[] pload : payload) {
                sizeInBytes += (pload!=null) ? RamUsageEstimator.sizeOf(pload) : 0;
              }
            }
          }
        }

        return sizeInBytes;
      }
      
      @Override
      public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
      }

    }

    private final byte[] termBytes;
    private final int[] termOffsets;

    private final int[] skips;
    private final int[] skipOffsets;

    private final TermAndSkip[] terms;
    private final boolean hasFreq;
    private final boolean hasPos;
    private final boolean hasOffsets;
    private final boolean hasPayloads;
    private final long sumTotalTermFreq;
    private final int docCount;
    private final long sumDocFreq;
    private int skipCount;

    // TODO: maybe make a separate builder?  These are only
    // used during load:
    private int count;
    private int[] sameCounts = new int[10];
    private final int minSkipCount;

    private final static class IntArrayWriter {
      private int[] ints = new int[10];
      private int upto;

      public void add(int value) {
        if (ints.length == upto) {
          ints = ArrayUtil.grow(ints);
        }
        ints[upto++] = value;
      }

      public int[] get() {
        final int[] arr = new int[upto];
        System.arraycopy(ints, 0, arr, 0, upto);
        upto = 0;
        return arr;
      }
    }

    public DirectField(SegmentReadState state, String field, Terms termsIn, int minSkipCount, int lowFreqCutoff) throws IOException {
      final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);

      sumTotalTermFreq = termsIn.getSumTotalTermFreq();
      sumDocFreq = termsIn.getSumDocFreq();
      docCount = termsIn.getDocCount();

      final int numTerms = (int) termsIn.size();
      if (numTerms == -1) {
        throw new IllegalArgumentException("codec does not provide Terms.size()");
      }
      terms = new TermAndSkip[numTerms];
      termOffsets = new int[1+numTerms];

      byte[] termBytes = new byte[1024];

      this.minSkipCount = minSkipCount;

      hasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS) > 0;
      hasPos = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) > 0;
      hasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) > 0;
      hasPayloads = fieldInfo.hasPayloads();

      BytesRef term;
      PostingsEnum postingsEnum = null;
      PostingsEnum docsAndPositionsEnum = null;
      final TermsEnum termsEnum = termsIn.iterator();
      int termOffset = 0;

      final IntArrayWriter scratch = new IntArrayWriter();

      // Used for payloads, if any:
      final RAMOutputStream ros = new RAMOutputStream();

      // if (DEBUG) {
      //   System.out.println("\nLOAD terms seg=" + state.segmentInfo.name + " field=" + field + " hasOffsets=" + hasOffsets + " hasFreq=" + hasFreq + " hasPos=" + hasPos + " hasPayloads=" + hasPayloads);
      // }

      while ((term = termsEnum.next()) != null) {
        final int docFreq = termsEnum.docFreq();
        final long totalTermFreq = termsEnum.totalTermFreq();

        // if (DEBUG) {
        //   System.out.println("  term=" + term.utf8ToString());
        // }

        termOffsets[count] = termOffset;

        if (termBytes.length < (termOffset + term.length)) {
          termBytes = ArrayUtil.grow(termBytes, termOffset + term.length);
        }
        System.arraycopy(term.bytes, term.offset, termBytes, termOffset, term.length);
        termOffset += term.length;
        termOffsets[count+1] = termOffset;

        if (hasPos) {
          docsAndPositionsEnum = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.ALL);
        } else {
          postingsEnum = termsEnum.postings(postingsEnum);
        }

        final TermAndSkip ent;

        final PostingsEnum postingsEnum2;
        if (hasPos) {
          postingsEnum2 = docsAndPositionsEnum;
        } else {
          postingsEnum2 = postingsEnum;
        }

        int docID;

        if (docFreq <= lowFreqCutoff) {

          ros.reset();

          // Pack postings for low-freq terms into a single int[]:
          while ((docID = postingsEnum2.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
            scratch.add(docID);
            if (hasFreq) {
              final int freq = postingsEnum2.freq();
              scratch.add(freq);
              if (hasPos) {
                for(int pos=0;pos<freq;pos++) {
                  scratch.add(docsAndPositionsEnum.nextPosition());
                  if (hasOffsets) {
                    scratch.add(docsAndPositionsEnum.startOffset());
                    scratch.add(docsAndPositionsEnum.endOffset());
                  }
                  if (hasPayloads) {
                    final BytesRef payload = docsAndPositionsEnum.getPayload();
                    if (payload != null) {
                      scratch.add(payload.length);
                      ros.writeBytes(payload.bytes, payload.offset, payload.length);
                    } else {
                      scratch.add(0);
                    }
                  }
                }
              }
            }
          }

          final byte[] payloads;
          if (hasPayloads) {
            payloads = new byte[(int) ros.getFilePointer()];
            ros.writeTo(payloads, 0);
          } else {
            payloads = null;
          }

          final int[] postings = scratch.get();

          ent = new LowFreqTerm(postings, payloads, docFreq, (int) totalTermFreq);
        } else {
          final int[] docs = new int[docFreq];
          final int[] freqs;
          final int[][] positions;
          final byte[][][] payloads;
          if (hasFreq) {
            freqs = new int[docFreq];
            if (hasPos) {
              positions = new int[docFreq][];
              if (hasPayloads) {
                payloads = new byte[docFreq][][];
              } else {
                payloads = null;
              }
            } else {
              positions = null;
              payloads = null;
            }
          } else {
            freqs = null;
            positions = null;
            payloads = null;
          }

          // Use separate int[] for the postings for high-freq
          // terms:
          int upto = 0;
          while ((docID = postingsEnum2.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
            docs[upto] = docID;
            if (hasFreq) {
              final int freq = postingsEnum2.freq();
              freqs[upto] = freq;
              if (hasPos) {
                final int mult;
                if (hasOffsets) {
                  mult = 3;
                } else {
                  mult = 1;
                }
                if (hasPayloads) {
                  payloads[upto] = new byte[freq][];
                }
                positions[upto] = new int[mult*freq];
                int posUpto = 0;
                for(int pos=0;pos<freq;pos++) {
                  positions[upto][posUpto] = docsAndPositionsEnum.nextPosition();
                  if (hasPayloads) {
                    BytesRef payload = docsAndPositionsEnum.getPayload();
                    if (payload != null) {
                      byte[] payloadBytes = new byte[payload.length];
                      System.arraycopy(payload.bytes, payload.offset, payloadBytes, 0, payload.length);
                      payloads[upto][pos] = payloadBytes;
                    }
                  }
                  posUpto++;
                  if (hasOffsets) {
                    positions[upto][posUpto++] = docsAndPositionsEnum.startOffset();
                    positions[upto][posUpto++] = docsAndPositionsEnum.endOffset();
                  }
                }
              }
            }

            upto++;
          }
          assert upto == docFreq;
          ent = new HighFreqTerm(docs, freqs, positions, payloads, totalTermFreq);
        }

        terms[count] = ent;
        setSkips(count, termBytes);
        count++;
      }

      // End sentinel:
      termOffsets[count] = termOffset;

      finishSkips();

      //System.out.println(skipCount + " skips: " + field);

      this.termBytes = new byte[termOffset];
      System.arraycopy(termBytes, 0, this.termBytes, 0, termOffset);

      // Pack skips:
      this.skips = new int[skipCount];
      this.skipOffsets = new int[1+numTerms];

      int skipOffset = 0;
      for(int i=0;i<numTerms;i++) {
        final int[] termSkips = terms[i].skips;
        skipOffsets[i] = skipOffset;
        if (termSkips != null) {
          System.arraycopy(termSkips, 0, skips, skipOffset, termSkips.length);
          skipOffset += termSkips.length;
          terms[i].skips = null;
        }
      }
      this.skipOffsets[numTerms] = skipOffset;
      assert skipOffset == skipCount;
    }

    @Override
    public long ramBytesUsed() {
      long sizeInBytes = BASE_RAM_BYTES_USED;
      sizeInBytes += ((termBytes!=null) ? RamUsageEstimator.sizeOf(termBytes) : 0);
      sizeInBytes += ((termOffsets!=null) ? RamUsageEstimator.sizeOf(termOffsets) : 0);
      sizeInBytes += ((skips!=null) ? RamUsageEstimator.sizeOf(skips) : 0);
      sizeInBytes += ((skipOffsets!=null) ? RamUsageEstimator.sizeOf(skipOffsets) : 0);
      sizeInBytes += ((sameCounts!=null) ? RamUsageEstimator.sizeOf(sameCounts) : 0);

      if(terms!=null) {
        sizeInBytes += RamUsageEstimator.shallowSizeOf(terms);
        for(TermAndSkip termAndSkip : terms) {
          sizeInBytes += (termAndSkip!=null) ? termAndSkip.ramBytesUsed() : 0;
        }
      }

      return sizeInBytes;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      return Collections.emptyList();
    }

    @Override
    public String toString() {
      return "DirectTerms(terms=" + terms.length + ",postings=" + sumDocFreq + ",positions=" + sumTotalTermFreq + ",docs=" + docCount + ")";
    }

    // Compares in unicode (UTF8) order:
    int compare(int ord, BytesRef other) {
      final byte[] otherBytes = other.bytes;

      int upto = termOffsets[ord];
      final int termLen = termOffsets[1+ord] - upto;
      int otherUpto = other.offset;

      final int stop = upto + Math.min(termLen, other.length);
      while (upto < stop) {
        int diff = (termBytes[upto++] & 0xFF) - (otherBytes[otherUpto++] & 0xFF);
        if (diff != 0) {
          return diff;
        }
      }

      // One is a prefix of the other, or, they are equal:
      return termLen - other.length;
    }

    private void setSkips(int termOrd, byte[] termBytes) {

      final int termLength = termOffsets[termOrd+1] - termOffsets[termOrd];

      if (sameCounts.length < termLength) {
        sameCounts = ArrayUtil.grow(sameCounts, termLength);
      }

      // Update skip pointers:
      if (termOrd > 0) {
        final int lastTermLength = termOffsets[termOrd] - termOffsets[termOrd-1];
        final int limit = Math.min(termLength, lastTermLength);

        int lastTermOffset = termOffsets[termOrd-1];
        int termOffset = termOffsets[termOrd];

        int i = 0;
        for(;i<limit;i++) {
          if (termBytes[lastTermOffset++] == termBytes[termOffset++]) {
            sameCounts[i]++;
          } else {
            for(;i<limit;i++) {
              if (sameCounts[i] >= minSkipCount) {
                // Go back and add a skip pointer:
                saveSkip(termOrd, sameCounts[i]);
              }
              sameCounts[i] = 1;
            }
            break;
          }
        }

        for(;i<lastTermLength;i++) {
          if (sameCounts[i] >= minSkipCount) {
            // Go back and add a skip pointer:
            saveSkip(termOrd, sameCounts[i]);
          }
          sameCounts[i] = 0;
        }
        for(int j=limit;j<termLength;j++) {
          sameCounts[j] = 1;
        }
      } else {
        for(int i=0;i<termLength;i++) {
          sameCounts[i]++;
        }
      }
    }

    private void finishSkips() {
      assert count == terms.length;
      int lastTermOffset = termOffsets[count-1];
      int lastTermLength = termOffsets[count] - lastTermOffset;

      for(int i=0;i<lastTermLength;i++) {
        if (sameCounts[i] >= minSkipCount) {
          // Go back and add a skip pointer:
          saveSkip(count, sameCounts[i]);
        }
      }

      // Reverse the skip pointers so they are "nested":
      for(int termID=0;termID<terms.length;termID++) {
        TermAndSkip term = terms[termID];
        if (term.skips != null && term.skips.length > 1) {
          for(int pos=0;pos<term.skips.length/2;pos++) {
            final int otherPos = term.skips.length-pos-1;

            final int temp = term.skips[pos];
            term.skips[pos] = term.skips[otherPos];
            term.skips[otherPos] = temp;
          }
        }
      }
    }

    private void saveSkip(int ord, int backCount) {
      final TermAndSkip term = terms[ord - backCount];
      skipCount++;
      if (term.skips == null) {
        term.skips = new int[] {ord};
      } else {
        // Normally we'd grow at a slight exponential... but
        // given that the skips themselves are already log(N)
        // we can grow by only 1 and still have amortized
        // linear time:
        final int[] newSkips = new int[term.skips.length+1];
        System.arraycopy(term.skips, 0, newSkips, 0, term.skips.length);
        term.skips = newSkips;
        term.skips[term.skips.length-1] = ord;
      }
    }

    @Override
    public TermsEnum iterator() {
      return new DirectTermsEnum();
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) {
      return new DirectIntersectTermsEnum(compiled, startTerm);
    }

    @Override
    public long size() {
      return terms.length;
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }

    @Override
    public long getSumDocFreq() {
      return sumDocFreq;
    }

    @Override
    public int getDocCount() {
      return docCount;
    }

    @Override
    public boolean hasFreqs() {
      return hasFreq;
    }

    @Override
    public boolean hasOffsets() {
      return hasOffsets;
    }

    @Override
    public boolean hasPositions() {
      return hasPos;
    }

    @Override
    public boolean hasPayloads() {
      return hasPayloads;
    }

    private final class DirectTermsEnum extends TermsEnum {

      private final BytesRef scratch = new BytesRef();
      private int termOrd;

      private DirectTermsEnum() {
        termOrd = -1;
      }

      private BytesRef setTerm() {
        scratch.bytes = termBytes;
        scratch.offset = termOffsets[termOrd];
        scratch.length = termOffsets[termOrd+1] - termOffsets[termOrd];
        return scratch;
      }

      @Override
      public BytesRef next() {
        termOrd++;
        if (termOrd < terms.length) {
          return setTerm();
        } else {
          return null;
        }
      }

      @Override
      public TermState termState() {
        OrdTermState state = new OrdTermState();
        state.ord = termOrd;
        return state;
      }

      // If non-negative, exact match; else, -ord-1, where ord
      // is where you would insert the term.
      private int findTerm(BytesRef term) {

        // Just do binary search: should be (constant factor)
        // faster than using the skip list:
        int low = 0;
        int high = terms.length-1;

        while (low <= high) {
          int mid = (low + high) >>> 1;
          int cmp = compare(mid, term);
          if (cmp < 0) {
            low = mid + 1;
          } else if (cmp > 0) {
            high = mid - 1;
          } else {
            return mid; // key found
          }
        }

        return -(low + 1);  // key not found.
      }

      @Override
      public SeekStatus seekCeil(BytesRef term) {
        // TODO: we should use the skip pointers; should be
        // faster than bin search; we should also hold
        // & reuse current state so seeking forwards is
        // faster
        final int ord = findTerm(term);
        // if (DEBUG) {
        //   System.out.println("  find term=" + term.utf8ToString() + " ord=" + ord);
        // }
        if (ord >= 0) {
          termOrd = ord;
          setTerm();
          return SeekStatus.FOUND;
        } else if (ord == -terms.length-1) {
          return SeekStatus.END;
        } else {
          termOrd = -ord - 1;
          setTerm();
          return SeekStatus.NOT_FOUND;
        }
      }

      @Override
      public boolean seekExact(BytesRef term) {
        // TODO: we should use the skip pointers; should be
        // faster than bin search; we should also hold
        // & reuse current state so seeking forwards is
        // faster
        final int ord = findTerm(term);
        if (ord >= 0) {
          termOrd = ord;
          setTerm();
          return true;
        } else {
          return false;
        }
      }

      @Override
      public void seekExact(long ord) {
        termOrd = (int) ord;
        setTerm();
      }

      @Override
      public void seekExact(BytesRef term, TermState state) throws IOException {
        termOrd = (int) ((OrdTermState) state).ord;
        setTerm();
        assert term.equals(scratch);
      }

      @Override
      public BytesRef term() {
        return scratch;
      }

      @Override
      public long ord() {
        return termOrd;
      }

      @Override
      public int docFreq() {
        if (terms[termOrd] instanceof LowFreqTerm) {
          return ((LowFreqTerm) terms[termOrd]).docFreq;
        } else {
          return ((HighFreqTerm) terms[termOrd]).docIDs.length;
        }
      }

      @Override
      public long totalTermFreq() {
        if (terms[termOrd] instanceof LowFreqTerm) {
          return ((LowFreqTerm) terms[termOrd]).totalTermFreq;
        } else {
          return ((HighFreqTerm) terms[termOrd]).totalTermFreq;
        }
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        
        if (PostingsEnum.featureRequested(flags, DocsAndPositionsEnum.OLD_NULL_SEMANTICS)) {
          if (!hasPos) {
            // Positions were not indexed:
            return null;
          }
        }

        // TODO: implement reuse
        // it's hairy!

        // TODO: the logic of which enum impl to choose should be refactored to be simpler...
        if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {

          if (terms[termOrd] instanceof LowFreqTerm) {
            final LowFreqTerm term = ((LowFreqTerm) terms[termOrd]);
            final int[] postings = term.postings;
            if (hasFreq == false) {
              LowFreqDocsEnumNoTF docsEnum;
              if (reuse instanceof LowFreqDocsEnumNoTF) {
                docsEnum = (LowFreqDocsEnumNoTF) reuse;
              } else {
                docsEnum = new LowFreqDocsEnumNoTF();
              }

              return docsEnum.reset(postings);
              
            } else if (hasPos == false) {
              LowFreqDocsEnumNoPos docsEnum;
              if (reuse instanceof LowFreqDocsEnumNoPos) {
                docsEnum = (LowFreqDocsEnumNoPos) reuse;
              } else {
                docsEnum = new LowFreqDocsEnumNoPos();
              }

              return docsEnum.reset(postings);
            }
            final byte[] payloads = term.payloads;
            return new LowFreqPostingsEnum(hasOffsets, hasPayloads).reset(postings, payloads);
          } else {
            final HighFreqTerm term = (HighFreqTerm) terms[termOrd];
            if (hasPos == false) {
              return new HighFreqDocsEnum().reset(term.docIDs, term.freqs);
            } else {
              return new HighFreqPostingsEnum(hasOffsets).reset(term.docIDs, term.freqs, term.positions, term.payloads);
            }
          }
        }

        if (terms[termOrd] instanceof LowFreqTerm) {
          final int[] postings = ((LowFreqTerm) terms[termOrd]).postings;
          if (hasFreq) {
            if (hasPos) {
              int posLen;
              if (hasOffsets) {
                posLen = 3;
              } else {
                posLen = 1;
              }
              if (hasPayloads) {
                posLen++;
              }
              LowFreqDocsEnum docsEnum;
              if (reuse instanceof LowFreqDocsEnum) {
                docsEnum = (LowFreqDocsEnum) reuse;
                if (!docsEnum.canReuse(posLen)) {
                  docsEnum = new LowFreqDocsEnum(posLen);
                }
              } else {
                docsEnum = new LowFreqDocsEnum(posLen);
              }

              return docsEnum.reset(postings);
            } else {
              LowFreqDocsEnumNoPos docsEnum;
              if (reuse instanceof LowFreqDocsEnumNoPos) {
                docsEnum = (LowFreqDocsEnumNoPos) reuse;
              } else {
                docsEnum = new LowFreqDocsEnumNoPos();
              }

              return docsEnum.reset(postings);
            }
          } else {
            LowFreqDocsEnumNoTF docsEnum;
            if (reuse instanceof LowFreqDocsEnumNoTF) {
              docsEnum = (LowFreqDocsEnumNoTF) reuse;
            } else {
              docsEnum = new LowFreqDocsEnumNoTF();
            }

            return docsEnum.reset(postings);
          }
        } else {
          final HighFreqTerm term = (HighFreqTerm) terms[termOrd];

          HighFreqDocsEnum docsEnum;
          if (reuse instanceof HighFreqDocsEnum) {
            docsEnum = (HighFreqDocsEnum) reuse;
          } else {
            docsEnum = new HighFreqDocsEnum();
          }

          //System.out.println("  DE for term=" + new BytesRef(terms[termOrd].term).utf8ToString() + ": " + term.docIDs.length + " docs");
          return docsEnum.reset(term.docIDs, term.freqs);
        }
      }

    }

    private final class DirectIntersectTermsEnum extends TermsEnum {
      private final RunAutomaton runAutomaton;
      private final CompiledAutomaton compiledAutomaton;
      private int termOrd;
      private final BytesRef scratch = new BytesRef();

      private final class State {
        int changeOrd;
        int state;
        int transitionUpto;
        int transitionCount;
        int transitionMax;
        int transitionMin;
        final Transition transition = new Transition();
      }

      private State[] states;
      private int stateUpto;

      public DirectIntersectTermsEnum(CompiledAutomaton compiled, BytesRef startTerm) {
        runAutomaton = compiled.runAutomaton;
        compiledAutomaton = compiled;
        termOrd = -1;
        states = new State[1];
        states[0] = new State();
        states[0].changeOrd = terms.length;
        states[0].state = runAutomaton.getInitialState();
        states[0].transitionCount = compiledAutomaton.automaton.getNumTransitions(states[0].state);
        compiledAutomaton.automaton.initTransition(states[0].state, states[0].transition);
        states[0].transitionUpto = -1;
        states[0].transitionMax = -1;

        //System.out.println("IE.init startTerm=" + startTerm);

        if (startTerm != null) {
          int skipUpto = 0;
          if (startTerm.length == 0) {
            if (terms.length > 0 && termOffsets[1] == 0) {
              termOrd = 0;
            }
          } else {
            termOrd++;

            nextLabel:
            for(int i=0;i<startTerm.length;i++) {
              final int label = startTerm.bytes[startTerm.offset+i] & 0xFF;

              while (label > states[i].transitionMax) {
                states[i].transitionUpto++;
                assert states[i].transitionUpto < states[i].transitionCount;
                compiledAutomaton.automaton.getNextTransition(states[i].transition);
                states[i].transitionMin = states[i].transition.min;
                states[i].transitionMax = states[i].transition.max;
                assert states[i].transitionMin >= 0;
                assert states[i].transitionMin <= 255;
                assert states[i].transitionMax >= 0;
                assert states[i].transitionMax <= 255;
              }

              // Skip forwards until we find a term matching
              // the label at this position:
              while (termOrd < terms.length) {
                final int skipOffset = skipOffsets[termOrd];
                final int numSkips = skipOffsets[termOrd+1] - skipOffset;
                final int termOffset = termOffsets[termOrd];
                final int termLength = termOffsets[1+termOrd] - termOffset;

                // if (DEBUG) {
                //   System.out.println("  check termOrd=" + termOrd + " term=" + new BytesRef(termBytes, termOffset, termLength).utf8ToString() + " skips=" + Arrays.toString(skips) + " i=" + i);
                // }

                if (termOrd == states[stateUpto].changeOrd) {
                  // if (DEBUG) {
                  //   System.out.println("  end push return");
                  // }
                  stateUpto--;
                  termOrd--;
                  return;
                }

                if (termLength == i) {
                  termOrd++;
                  skipUpto = 0;
                  // if (DEBUG) {
                  //   System.out.println("    term too short; next term");
                  // }
                } else if (label < (termBytes[termOffset+i] & 0xFF)) {
                  termOrd--;
                  // if (DEBUG) {
                  //   System.out.println("  no match; already beyond; return termOrd=" + termOrd);
                  // }
                  stateUpto -= skipUpto;
                  assert stateUpto >= 0;
                  return;
                } else if (label == (termBytes[termOffset+i] & 0xFF)) {
                  // if (DEBUG) {
                  //   System.out.println("    label[" + i + "] matches");
                  // }
                  if (skipUpto < numSkips) {
                    grow();

                    final int nextState = runAutomaton.step(states[stateUpto].state, label);

                    // Automaton is required to accept startTerm:
                    assert nextState != -1;

                    stateUpto++;
                    states[stateUpto].changeOrd = skips[skipOffset + skipUpto++];
                    states[stateUpto].state = nextState;
                    states[stateUpto].transitionCount = compiledAutomaton.automaton.getNumTransitions(nextState);
                    compiledAutomaton.automaton.initTransition(states[stateUpto].state, states[stateUpto].transition);
                    states[stateUpto].transitionUpto = -1;
                    states[stateUpto].transitionMax = -1;
                    //System.out.println("  push " + states[stateUpto].transitions.length + " trans");

                    // if (DEBUG) {
                    //   System.out.println("    push skip; changeOrd=" + states[stateUpto].changeOrd);
                    // }

                    // Match next label at this same term:
                    continue nextLabel;
                  } else {
                    // if (DEBUG) {
                    //   System.out.println("    linear scan");
                    // }
                    // Index exhausted: just scan now (the
                    // number of scans required will be less
                    // than the minSkipCount):
                    final int startTermOrd = termOrd;
                    while (termOrd < terms.length && compare(termOrd, startTerm) <= 0) {
                      assert termOrd == startTermOrd || skipOffsets[termOrd] == skipOffsets[termOrd+1];
                      termOrd++;
                    }
                    assert termOrd - startTermOrd < minSkipCount;
                    termOrd--;
                    stateUpto -= skipUpto;
                    // if (DEBUG) {
                    //   System.out.println("  end termOrd=" + termOrd);
                    // }
                    return;
                  }
                } else {
                  if (skipUpto < numSkips) {
                    termOrd = skips[skipOffset + skipUpto];
                    // if (DEBUG) {
                    //   System.out.println("  no match; skip to termOrd=" + termOrd);
                    // }
                  } else {
                    // if (DEBUG) {
                    //   System.out.println("  no match; next term");
                    // }
                    termOrd++;
                  }
                  skipUpto = 0;
                }
              }

              // startTerm is >= last term so enum will not
              // return any terms:
              termOrd--;
              // if (DEBUG) {
              //   System.out.println("  beyond end; no terms will match");
              // }
              return;
            }
          }

          final int termOffset = termOffsets[termOrd];
          final int termLen = termOffsets[1+termOrd] - termOffset;

          if (termOrd >= 0 && !startTerm.equals(new BytesRef(termBytes, termOffset, termLen))) {
            stateUpto -= skipUpto;
            termOrd--;
          }
          // if (DEBUG) {
          //   System.out.println("  loop end; return termOrd=" + termOrd + " stateUpto=" + stateUpto);
          // }
        }
      }

      private void grow() {
        if (states.length == 1+stateUpto) {
          final State[] newStates = new State[states.length+1];
          System.arraycopy(states, 0, newStates, 0, states.length);
          newStates[states.length] = new State();
          states = newStates;
        }
      }

      @Override
      public BytesRef next() {
        // if (DEBUG) {
        //   System.out.println("\nIE.next");
        // }

        termOrd++;
        int skipUpto = 0;

        if (termOrd == 0 && termOffsets[1] == 0) {
          // Special-case empty string:
          assert stateUpto == 0;
          // if (DEBUG) {
          //   System.out.println("  visit empty string");
          // }
          if (runAutomaton.isAccept(states[0].state)) {
            scratch.bytes = termBytes;
            scratch.offset = 0;
            scratch.length = 0;
            return scratch;
          }
          termOrd++;
        }

        nextTerm:

        while (true) {
          // if (DEBUG) {
          //   System.out.println("  cycle termOrd=" + termOrd + " stateUpto=" + stateUpto + " skipUpto=" + skipUpto);
          // }
          if (termOrd == terms.length) {
            // if (DEBUG) {
            //   System.out.println("  return END");
            // }
            return null;
          }

          final State state = states[stateUpto];
          if (termOrd == state.changeOrd) {
            // Pop:
            // if (DEBUG) {
            //   System.out.println("  pop stateUpto=" + stateUpto);
            // }
            stateUpto--;
            /*
            if (DEBUG) {
              try {
                //System.out.println("    prefix pop " + new BytesRef(terms[termOrd].term, 0, Math.min(stateUpto, terms[termOrd].term.length)).utf8ToString());
                System.out.println("    prefix pop " + new BytesRef(terms[termOrd].term, 0, Math.min(stateUpto, terms[termOrd].term.length)));
              } catch (ArrayIndexOutOfBoundsException aioobe) {
                System.out.println("    prefix pop " + new BytesRef(terms[termOrd].term, 0, Math.min(stateUpto, terms[termOrd].term.length)));
              }
            }
            */

            continue;
          }

          final int termOffset = termOffsets[termOrd];
          final int termLength = termOffsets[termOrd+1] - termOffset;
          final int skipOffset = skipOffsets[termOrd];
          final int numSkips = skipOffsets[termOrd+1] - skipOffset;

          // if (DEBUG) {
          //   System.out.println("  term=" + new BytesRef(termBytes, termOffset, termLength).utf8ToString() + " skips=" + Arrays.toString(skips));
          // }

          assert termOrd < state.changeOrd;

          assert stateUpto <= termLength: "term.length=" + termLength + "; stateUpto=" + stateUpto;
          final int label = termBytes[termOffset+stateUpto] & 0xFF;

          while (label > state.transitionMax) {
            //System.out.println("  label=" + label + " vs max=" + state.transitionMax + " transUpto=" + state.transitionUpto + " vs " + state.transitions.length);
            state.transitionUpto++;
            if (state.transitionUpto == state.transitionCount) {
              // We've exhausted transitions leaving this
              // state; force pop+next/skip now:
              //System.out.println("forcepop: stateUpto=" + stateUpto);
              if (stateUpto == 0) {
                termOrd = terms.length;
                return null;
              } else {
                assert state.changeOrd > termOrd;
                // if (DEBUG) {
                //   System.out.println("  jumpend " + (state.changeOrd - termOrd));
                // }
                //System.out.println("  jump to termOrd=" + states[stateUpto].changeOrd + " vs " + termOrd);
                termOrd = states[stateUpto].changeOrd;
                skipUpto = 0;
                stateUpto--;
              }
              continue nextTerm;
            }
            compiledAutomaton.automaton.getNextTransition(state.transition);
            assert state.transitionUpto < state.transitionCount: " state.transitionUpto=" + state.transitionUpto + " vs " + state.transitionCount;
            state.transitionMin = state.transition.min;
            state.transitionMax = state.transition.max;
            assert state.transitionMin >= 0;
            assert state.transitionMin <= 255;
            assert state.transitionMax >= 0;
            assert state.transitionMax <= 255;
          }

          /*
          if (DEBUG) {
            System.out.println("    check ord=" + termOrd + " term[" + stateUpto + "]=" + (char) label + "(" + label + ") term=" + new BytesRef(terms[termOrd].term).utf8ToString() + " trans " +
                               (char) state.transitionMin + "(" + state.transitionMin + ")" + "-" + (char) state.transitionMax + "(" + state.transitionMax + ") nextChange=+" + (state.changeOrd - termOrd) + " skips=" + (skips == null ? "null" : Arrays.toString(skips)));
            System.out.println("    check ord=" + termOrd + " term[" + stateUpto + "]=" + Integer.toHexString(label) + "(" + label + ") term=" + new BytesRef(termBytes, termOffset, termLength) + " trans " +
                               Integer.toHexString(state.transitionMin) + "(" + state.transitionMin + ")" + "-" + Integer.toHexString(state.transitionMax) + "(" + state.transitionMax + ") nextChange=+" + (state.changeOrd - termOrd) + " skips=" + (skips == null ? "null" : Arrays.toString(skips)));
          }
          */

          final int targetLabel = state.transitionMin;

          if ((termBytes[termOffset+stateUpto] & 0xFF) < targetLabel) {
            // if (DEBUG) {
            //   System.out.println("    do bin search");
            // }
            //int startTermOrd = termOrd;
            int low = termOrd+1;
            int high = state.changeOrd-1;
            while (true) {
              if (low > high) {
                // Label not found
                termOrd = low;
                // if (DEBUG) {
                //   System.out.println("      advanced by " + (termOrd - startTermOrd));
                // }
                //System.out.println("  jump " + (termOrd - startTermOrd));
                skipUpto = 0;
                continue nextTerm;
              }
              int mid = (low + high) >>> 1;
              int cmp = (termBytes[termOffsets[mid] + stateUpto] & 0xFF) - targetLabel;
              // if (DEBUG) {
              //   System.out.println("      bin: check label=" + (char) (termBytes[termOffsets[low] + stateUpto] & 0xFF) + " ord=" + mid);
              // }
              if (cmp < 0) {
                low = mid+1;
              } else if (cmp > 0) {
                high = mid - 1;
              } else {
                // Label found; walk backwards to first
                // occurrence:
                while (mid > termOrd && (termBytes[termOffsets[mid-1] + stateUpto] & 0xFF) == targetLabel) {
                  mid--;
                }
                termOrd = mid;
                // if (DEBUG) {
                //   System.out.println("      advanced by " + (termOrd - startTermOrd));
                // }
                //System.out.println("  jump " + (termOrd - startTermOrd));
                skipUpto = 0;
                continue nextTerm;
              }
            }
          }

          int nextState = runAutomaton.step(states[stateUpto].state, label);

          if (nextState == -1) {
            // Skip
            // if (DEBUG) {
            //   System.out.println("  automaton doesn't accept; skip");
            // }
            if (skipUpto < numSkips) {
              // if (DEBUG) {
              //   System.out.println("  jump " + (skips[skipOffset+skipUpto]-1 - termOrd));
              // }
              termOrd = skips[skipOffset+skipUpto];
            } else {
              termOrd++;
            }
            skipUpto = 0;
          } else if (skipUpto < numSkips) {
            // Push:
            // if (DEBUG) {
            //   System.out.println("  push");
            // }
            /*
            if (DEBUG) {
              try {
                //System.out.println("    prefix push " + new BytesRef(term, 0, stateUpto+1).utf8ToString());
                System.out.println("    prefix push " + new BytesRef(term, 0, stateUpto+1));
              } catch (ArrayIndexOutOfBoundsException aioobe) {
                System.out.println("    prefix push " + new BytesRef(term, 0, stateUpto+1));
              }
            }
            */

            grow();
            stateUpto++;
            states[stateUpto].state = nextState;
            states[stateUpto].changeOrd = skips[skipOffset + skipUpto++];
            states[stateUpto].transitionCount = compiledAutomaton.automaton.getNumTransitions(nextState);
            compiledAutomaton.automaton.initTransition(nextState, states[stateUpto].transition);
            states[stateUpto].transitionUpto = -1;
            states[stateUpto].transitionMax = -1;

            if (stateUpto == termLength) {
              // if (DEBUG) {
              //   System.out.println("  term ends after push");
              // }
              if (runAutomaton.isAccept(nextState)) {
                // if (DEBUG) {
                //   System.out.println("  automaton accepts: return");
                // }
                scratch.bytes = termBytes;
                scratch.offset = termOffsets[termOrd];
                scratch.length = termOffsets[1+termOrd] - scratch.offset;
                // if (DEBUG) {
                //   System.out.println("  ret " + scratch.utf8ToString());
                // }
                return scratch;
              } else {
                // if (DEBUG) {
                //   System.out.println("  automaton rejects: nextTerm");
                // }
                termOrd++;
                skipUpto = 0;
              }
            }
          } else {
            // Run the non-indexed tail of this term:

            // TODO: add assert that we don't inc too many times

            if (compiledAutomaton.commonSuffixRef != null) {
              //System.out.println("suffix " + compiledAutomaton.commonSuffixRef.utf8ToString());
              assert compiledAutomaton.commonSuffixRef.offset == 0;
              if (termLength < compiledAutomaton.commonSuffixRef.length) {
                termOrd++;
                skipUpto = 0;
                continue nextTerm;
              }
              int offset = termOffset + termLength - compiledAutomaton.commonSuffixRef.length;
              for(int suffix=0;suffix<compiledAutomaton.commonSuffixRef.length;suffix++) {
                if (termBytes[offset + suffix] != compiledAutomaton.commonSuffixRef.bytes[suffix]) {
                  termOrd++;
                  skipUpto = 0;
                  continue nextTerm;
                }
              }
            }

            int upto = stateUpto+1;
            while (upto < termLength) {
              nextState = runAutomaton.step(nextState, termBytes[termOffset+upto] & 0xFF);
              if (nextState == -1) {
                termOrd++;
                skipUpto = 0;
                // if (DEBUG) {
                //   System.out.println("  nomatch tail; next term");
                // }
                continue nextTerm;
              }
              upto++;
            }

            if (runAutomaton.isAccept(nextState)) {
              scratch.bytes = termBytes;
              scratch.offset = termOffsets[termOrd];
              scratch.length = termOffsets[1+termOrd] - scratch.offset;
              // if (DEBUG) {
              //   System.out.println("  match tail; return " + scratch.utf8ToString());
              //   System.out.println("  ret2 " + scratch.utf8ToString());
              // }
              return scratch;
            } else {
              termOrd++;
              skipUpto = 0;
              // if (DEBUG) {
              //   System.out.println("  nomatch tail; next term");
              // }
            }
          }
        }
      }

      @Override
      public TermState termState() {
        OrdTermState state = new OrdTermState();
        state.ord = termOrd;
        return state;
      }

      @Override
      public BytesRef term() {
        return scratch;
      }

      @Override
      public long ord() {
        return termOrd;
      }

      @Override
      public int docFreq() {
        if (terms[termOrd] instanceof LowFreqTerm) {
          return ((LowFreqTerm) terms[termOrd]).docFreq;
        } else {
          return ((HighFreqTerm) terms[termOrd]).docIDs.length;
        }
      }

      @Override
      public long totalTermFreq() {
        if (terms[termOrd] instanceof LowFreqTerm) {
          return ((LowFreqTerm) terms[termOrd]).totalTermFreq;
        } else {
          return ((HighFreqTerm) terms[termOrd]).totalTermFreq;
        }
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) {
        
        if (PostingsEnum.featureRequested(flags, DocsAndPositionsEnum.OLD_NULL_SEMANTICS)) {
          if (!hasPos) {
            // Positions were not indexed:
            return null;
          }
        }
        
        // TODO: implement reuse
        // it's hairy!

        // TODO: the logic of which enum impl to choose should be refactored to be simpler...
        if (hasPos && PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
          if (terms[termOrd] instanceof LowFreqTerm) {
            final LowFreqTerm term = ((LowFreqTerm) terms[termOrd]);
            final int[] postings = term.postings;
            final byte[] payloads = term.payloads;
            return new LowFreqPostingsEnum(hasOffsets, hasPayloads).reset(postings, payloads);
          } else {
            final HighFreqTerm term = (HighFreqTerm) terms[termOrd];
            return new HighFreqPostingsEnum(hasOffsets).reset(term.docIDs, term.freqs, term.positions, term.payloads);
          }
        }

        if (terms[termOrd] instanceof LowFreqTerm) {
          final int[] postings = ((LowFreqTerm) terms[termOrd]).postings;
          if (hasFreq) {
            if (hasPos) {
              int posLen;
              if (hasOffsets) {
                posLen = 3;
              } else {
                posLen = 1;
              }
              if (hasPayloads) {
                posLen++;
              }
              return new LowFreqDocsEnum(posLen).reset(postings);
            } else {
              return new LowFreqDocsEnumNoPos().reset(postings);
            }
          } else {
            return new LowFreqDocsEnumNoTF().reset(postings);
          }
        } else {
          final HighFreqTerm term = (HighFreqTerm) terms[termOrd];
          //  System.out.println("DE for term=" + new BytesRef(terms[termOrd].term).utf8ToString() + ": " + term.docIDs.length + " docs");
          return new HighFreqDocsEnum().reset(term.docIDs, term.freqs);
        }
      }

      @Override
      public SeekStatus seekCeil(BytesRef term) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void seekExact(long ord) {
        throw new UnsupportedOperationException();
      }
    }
  }

  // Docs only:
  private final static class LowFreqDocsEnumNoTF extends PostingsEnum {
    private int[] postings;
    private int upto;

    public PostingsEnum reset(int[] postings) {
      this.postings = postings;
      upto = -1;
      return this;
    }

    // TODO: can do this w/o setting members?

    @Override
    public int nextDoc() {
      upto++;
      if (upto < postings.length) {
        return postings[upto];
      }
      return NO_MORE_DOCS;
    }

    @Override
    public int docID() {
      if (upto < 0) {
        return -1;
      } else if (upto < postings.length) {
        return postings[upto];
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return 1;
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int advance(int target) throws IOException {
      // Linear scan, but this is low-freq term so it won't
      // be costly:
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return postings.length;
    }
  }

  // Docs + freqs:
  private final static class LowFreqDocsEnumNoPos extends PostingsEnum {
    private int[] postings;
    private int upto;

    public LowFreqDocsEnumNoPos() {}

    public PostingsEnum reset(int[] postings) {
      this.postings = postings;
      upto = -2;
      return this;
    }

    // TODO: can do this w/o setting members?
    @Override
    public int nextDoc() {
      upto += 2;
      if (upto < postings.length) {
        return postings[upto];
      }
      return NO_MORE_DOCS;
    }

    @Override
    public int docID() {
      if (upto < 0) {
        return -1;
      } else if (upto < postings.length) {
        return postings[upto];
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return postings[upto+1];
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int advance(int target) throws IOException {
      // Linear scan, but this is low-freq term so it won't
      // be costly:
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return postings.length / 2;
    }
  }

  // Docs + freqs + positions/offets:
  private final static class LowFreqDocsEnum extends PostingsEnum {
    private int[] postings;
    private final int posMult;
    private int upto;
    private int freq;

    public LowFreqDocsEnum(int posMult) {
      this.posMult = posMult;
      // if (DEBUG) {
      //   System.out.println("LowFreqDE: posMult=" + posMult);
      // }
    }

    public boolean canReuse(int posMult) {
      return this.posMult == posMult;
    }

    public PostingsEnum reset(int[] postings) {
      this.postings = postings;
      upto = -2;
      freq = 0;
      return this;
    }

    // TODO: can do this w/o setting members?
    @Override
    public int nextDoc() {
      upto += 2 + freq*posMult;
      // if (DEBUG) {
      //   System.out.println("  nextDoc freq=" + freq + " upto=" + upto + " vs " + postings.length);
      // }
      if (upto < postings.length) {
        freq = postings[upto+1];
        assert freq > 0;
        return postings[upto];
      }
      return NO_MORE_DOCS;
    }

    @Override
    public int docID() {
      // TODO: store docID member?
      if (upto < 0) {
        return -1;
      } else if (upto < postings.length) {
        return postings[upto];
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      // TODO: can I do postings[upto+1]?
      return freq;
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int advance(int target) throws IOException {
      // Linear scan, but this is low-freq term so it won't
      // be costly:
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      // TODO: could do a better estimate
      return postings.length / 2;
    }
  }

  private final static class LowFreqPostingsEnum extends PostingsEnum {
    private int[] postings;
    private final int posMult;
    private final boolean hasOffsets;
    private final boolean hasPayloads;
    private final BytesRef payload = new BytesRef();
    private int upto;
    private int docID;
    private int freq;
    private int skipPositions;
    private int pos;
    private int startOffset;
    private int endOffset;
    private int lastPayloadOffset;
    private int payloadOffset;
    private int payloadLength;
    private byte[] payloadBytes;

    public LowFreqPostingsEnum(boolean hasOffsets, boolean hasPayloads) {
      this.hasOffsets = hasOffsets;
      this.hasPayloads = hasPayloads;
      if (hasOffsets) {
        if (hasPayloads) {
          posMult = 4;
        } else {
          posMult = 3;
        }
      } else if (hasPayloads) {
        posMult = 2;
      } else {
        posMult = 1;
      }
    }

    public PostingsEnum reset(int[] postings, byte[] payloadBytes) {
      this.postings = postings;
      upto = 0;
      skipPositions = 0;
      pos = -1;
      startOffset = -1;
      endOffset = -1;
      docID = -1;
      payloadLength = 0;
      this.payloadBytes = payloadBytes;
      return this;
    }

    @Override
    public int nextDoc() {
      pos = -1;
      if (hasPayloads) {
        for(int i=0;i<skipPositions;i++) {
          upto++;
          if (hasOffsets) {
            upto += 2;
          }
          payloadOffset += postings[upto++];
        }
      } else {
        upto += posMult * skipPositions;
      }

      if (upto < postings.length) {
        docID = postings[upto++];
        freq = postings[upto++];
        skipPositions = freq;
        return docID;
      }

      return docID = NO_MORE_DOCS;
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
      assert skipPositions > 0;
      skipPositions--;
      pos = postings[upto++];
      if (hasOffsets) {
        startOffset = postings[upto++];
        endOffset = postings[upto++];
      }
      if (hasPayloads) {
        payloadLength = postings[upto++];
        lastPayloadOffset = payloadOffset;
        payloadOffset += payloadLength;
      }
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
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public BytesRef getPayload() {
      if (payloadLength > 0) {
        payload.bytes = payloadBytes;
        payload.offset = lastPayloadOffset;
        payload.length = payloadLength;
        return payload;
      } else {
        return null;
      }
    }

    @Override
    public long cost() {
      // TODO: could do a better estimate
      return postings.length / 2;
    }
  }

  // Docs + freqs:
  private final static class HighFreqDocsEnum extends PostingsEnum {
    private int[] docIDs;
    private int[] freqs;
    private int upto;
    private int docID = -1;

    public HighFreqDocsEnum() {}

    public int[] getDocIDs() {
      return docIDs;
    }

    public int[] getFreqs() {
      return freqs;
    }

    public PostingsEnum reset(int[] docIDs, int[] freqs) {
      this.docIDs = docIDs;
      this.freqs = freqs;
      docID = upto = -1;
      return this;
    }

    @Override
    public int nextDoc() {
      upto++;
      try {
        return docID = docIDs[upto];
      } catch (ArrayIndexOutOfBoundsException e) {
      }
      return docID = NO_MORE_DOCS;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      if (freqs == null) {
        return 1;
      } else {
        return freqs[upto];
      }
    }

    @Override
    public int advance(int target) {
      /*
      upto++;
      if (upto == docIDs.length) {
        return docID = NO_MORE_DOCS;
      }
      final int index = Arrays.binarySearch(docIDs, upto, docIDs.length, target);
      if (index < 0) {
        upto = -index - 1;
      } else {
        upto = index;
      }
      if (liveDocs != null) {
        while (upto < docIDs.length) {
          if (liveDocs.get(docIDs[upto])) {
            break;
          }
          upto++;
        }
      }
      if (upto == docIDs.length) {
        return NO_MORE_DOCS;
      } else {
        return docID = docIDs[upto];
      }
      */

      //System.out.println("  advance target=" + target + " cur=" + docID() + " upto=" + upto + " of " + docIDs.length);
      // if (DEBUG) {
      //   System.out.println("advance target=" + target + " len=" + docIDs.length);
      // }
      upto++;
      if (upto == docIDs.length) {
        return docID = NO_MORE_DOCS;
      }

      // First "grow" outwards, since most advances are to
      // nearby docs:
      int inc = 10;
      int nextUpto = upto+10;
      int low;
      int high;
      while (true) {
        //System.out.println("  grow nextUpto=" + nextUpto + " inc=" + inc);
        if (nextUpto >= docIDs.length) {
          low = nextUpto-inc;
          high = docIDs.length-1;
          break;
        }
        //System.out.println("    docID=" + docIDs[nextUpto]);

        if (target <= docIDs[nextUpto]) {
          low = nextUpto-inc;
          high = nextUpto;
          break;
        }
        inc *= 2;
        nextUpto += inc;
      }

      // Now do normal binary search
      //System.out.println("    after fwd: low=" + low + " high=" + high);

      while (true) {

        if (low > high) {
          // Not exactly found
          //System.out.println("    break: no match");
          upto = low;
          break;
        }

        int mid = (low + high) >>> 1;
        int cmp = docIDs[mid] - target;
        //System.out.println("    bsearch low=" + low + " high=" + high+ ": docIDs[" + mid + "]=" + docIDs[mid]);

        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          // Found target
          upto = mid;
          //System.out.println("    break: match");
          break;
        }
      }

      //System.out.println("    end upto=" + upto + " docID=" + (upto >= docIDs.length ? NO_MORE_DOCS : docIDs[upto]));

      if (upto == docIDs.length) {
        //System.out.println("    return END");
        return docID = NO_MORE_DOCS;
      } else {
        //System.out.println("    return docID=" + docIDs[upto] + " upto=" + upto);
        return docID = docIDs[upto];
      }
    }

    @Override
    public long cost() {
      return docIDs.length;
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }
  }

  // TODO: specialize offsets and not
  private final static class HighFreqPostingsEnum extends PostingsEnum {
    private int[] docIDs;
    private int[] freqs;
    private int[][] positions;
    private byte[][][] payloads;
    private final boolean hasOffsets;
    private final int posJump;
    private int upto;
    private int docID = -1;
    private int posUpto;
    private int[] curPositions;

    public HighFreqPostingsEnum(boolean hasOffsets) {
      this.hasOffsets = hasOffsets;
      posJump = hasOffsets ? 3 : 1;
    }

    public int[] getDocIDs() {
      return docIDs;
    }

    public int[][] getPositions() {
      return positions;
    }

    public int getPosJump() {
      return posJump;
    }

    public PostingsEnum reset(int[] docIDs, int[] freqs, int[][] positions, byte[][][] payloads) {
      this.docIDs = docIDs;
      this.freqs = freqs;
      this.positions = positions;
      this.payloads = payloads;
      upto = -1;
      return this;
    }

    @Override
    public int nextDoc() {
      upto++;
      if (upto < docIDs.length) {
        posUpto = -posJump;
        curPositions = positions[upto];
        return docID = docIDs[upto];
      }

      return docID = NO_MORE_DOCS;
    }

    @Override
    public int freq() {
      return freqs[upto];
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextPosition() {
      posUpto += posJump;
      assert posUpto < curPositions.length;
      return curPositions[posUpto];
    }

    @Override
    public int startOffset() {
      if (hasOffsets) {
        return curPositions[posUpto+1];
      } else {
        return -1;
      }
    }

    @Override
    public int endOffset() {
      if (hasOffsets) {
        return curPositions[posUpto+2];
      } else {
        return -1;
      }
    }

    @Override
    public int advance(int target) {

      /*
      upto++;
      if (upto == docIDs.length) {
        return NO_MORE_DOCS;
      }
      final int index = Arrays.binarySearch(docIDs, upto, docIDs.length, target);
      if (index < 0) {
        upto = -index - 1;
      } else {
        upto = index;
      }
      if (liveDocs != null) {
        while (upto < docIDs.length) {
          if (liveDocs.get(docIDs[upto])) {
            break;
          }
          upto++;
        }
      }
      posUpto = hasOffsets ? -3 : -1;
      if (upto == docIDs.length) {
        return NO_MORE_DOCS;
      } else {
        return docID();
      }
      */

      //System.out.println("  advance target=" + target + " cur=" + docID() + " upto=" + upto + " of " + docIDs.length);
      // if (DEBUG) {
      //   System.out.println("advance target=" + target + " len=" + docIDs.length);
      // }
      upto++;
      if (upto == docIDs.length) {
        return docID = NO_MORE_DOCS;
      }

      // First "grow" outwards, since most advances are to
      // nearby docs:
      int inc = 10;
      int nextUpto = upto+10;
      int low;
      int high;
      while (true) {
        //System.out.println("  grow nextUpto=" + nextUpto + " inc=" + inc);
        if (nextUpto >= docIDs.length) {
          low = nextUpto-inc;
          high = docIDs.length-1;
          break;
        }
        //System.out.println("    docID=" + docIDs[nextUpto]);

        if (target <= docIDs[nextUpto]) {
          low = nextUpto-inc;
          high = nextUpto;
          break;
        }
        inc *= 2;
        nextUpto += inc;
      }

      // Now do normal binary search
      //System.out.println("    after fwd: low=" + low + " high=" + high);

      while (true) {

        if (low > high) {
          // Not exactly found
          //System.out.println("    break: no match");
          upto = low;
          break;
        }

        int mid = (low + high) >>> 1;
        int cmp = docIDs[mid] - target;
        //System.out.println("    bsearch low=" + low + " high=" + high+ ": docIDs[" + mid + "]=" + docIDs[mid]);

        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          // Found target
          upto = mid;
          //System.out.println("    break: match");
          break;
        }
      }

      //System.out.println("    end upto=" + upto + " docID=" + (upto >= docIDs.length ? NO_MORE_DOCS : docIDs[upto]));

      if (upto == docIDs.length) {
        //System.out.println("    return END");
        return docID = NO_MORE_DOCS;
      } else {
        //System.out.println("    return docID=" + docIDs[upto] + " upto=" + upto);
        posUpto = -posJump;
        curPositions = positions[upto];
        return docID = docIDs[upto];
      }
    }

    private final BytesRef payload = new BytesRef();

    @Override
    public BytesRef getPayload() {
      if (payloads == null) {
        return null;
      } else {
        final byte[] payloadBytes = payloads[upto][posUpto/(hasOffsets ? 3:1)];
        if (payloadBytes == null) {
          return null;
        }
        payload.bytes = payloadBytes;
        payload.length = payloadBytes.length;
        payload.offset = 0;
        return payload;
      }
    }

    @Override
    public long cost() {
      return docIDs.length;
    }
  }
}
