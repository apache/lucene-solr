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
package org.apache.lucene.codecs.blockterms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Handles a terms dict, but decouples all details of doc/freqs/positions reading to an instance of
 * {@link PostingsReaderBase}. This class is reusable for codecs that use a different format for
 * docs/freqs/positions (though codecs are also free to make their own terms dict impl).
 *
 * <p>This class also interacts with an instance of {@link TermsIndexReaderBase}, to abstract away
 * the specific implementation of the terms dict index.
 *
 * @lucene.experimental
 */
public class BlockTermsReader extends FieldsProducer {
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(BlockTermsReader.class);
  // Open input to the main terms dict file (_X.tis)
  private final IndexInput in;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  private final PostingsReaderBase postingsReader;

  private final TreeMap<String, FieldReader> fields = new TreeMap<>();

  // Reads the terms index
  private TermsIndexReaderBase indexReader;

  // Used as key for the terms cache
  private static class FieldAndTerm implements Cloneable {
    String field;
    BytesRef term;

    public FieldAndTerm() {}

    public FieldAndTerm(FieldAndTerm other) {
      field = other.field;
      term = BytesRef.deepCopyOf(other.term);
    }

    @Override
    public boolean equals(Object _other) {
      FieldAndTerm other = (FieldAndTerm) _other;
      return other.field.equals(field) && term.bytesEquals(other.term);
    }

    @Override
    public FieldAndTerm clone() {
      return new FieldAndTerm(this);
    }

    @Override
    public int hashCode() {
      return field.hashCode() * 31 + term.hashCode();
    }
  }

  public BlockTermsReader(
      TermsIndexReaderBase indexReader, PostingsReaderBase postingsReader, SegmentReadState state)
      throws IOException {

    this.postingsReader = postingsReader;

    String filename =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, BlockTermsWriter.TERMS_EXTENSION);
    in = state.directory.openInput(filename, state.context);

    boolean success = false;
    try {
      CodecUtil.checkIndexHeader(
          in,
          BlockTermsWriter.CODEC_NAME,
          BlockTermsWriter.VERSION_START,
          BlockTermsWriter.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);

      // Have PostingsReader init itself
      postingsReader.init(in, state);

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(in);

      // Read per-field details
      seekDir(in);

      final int numFields = in.readVInt();
      if (numFields < 0) {
        throw new CorruptIndexException("invalid number of fields: " + numFields, in);
      }
      for (int i = 0; i < numFields; i++) {
        final int field = in.readVInt();
        final long numTerms = in.readVLong();
        assert numTerms >= 0;
        final long termsStartPointer = in.readVLong();
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        final long sumTotalTermFreq = in.readVLong();
        // when frequencies are omitted, sumDocFreq=totalTermFreq and we only write one value
        final long sumDocFreq =
            fieldInfo.getIndexOptions() == IndexOptions.DOCS ? sumTotalTermFreq : in.readVLong();
        final int docCount = in.readVInt();
        if (docCount < 0
            || docCount > state.segmentInfo.maxDoc()) { // #docs with field must be <= #docs
          throw new CorruptIndexException(
              "invalid docCount: " + docCount + " maxDoc: " + state.segmentInfo.maxDoc(), in);
        }
        if (sumDocFreq < docCount) { // #postings must be >= #docs with field
          throw new CorruptIndexException(
              "invalid sumDocFreq: " + sumDocFreq + " docCount: " + docCount, in);
        }
        if (sumTotalTermFreq < sumDocFreq) { // #positions must be >= #postings
          throw new CorruptIndexException(
              "invalid sumTotalTermFreq: " + sumTotalTermFreq + " sumDocFreq: " + sumDocFreq, in);
        }
        FieldReader previous =
            fields.put(
                fieldInfo.name,
                new FieldReader(
                    fieldInfo,
                    numTerms,
                    termsStartPointer,
                    sumTotalTermFreq,
                    sumDocFreq,
                    docCount));
        if (previous != null) {
          throw new CorruptIndexException("duplicate fields: " + fieldInfo.name, in);
        }
      }
      success = true;
    } finally {
      if (!success) {
        in.close();
      }
    }

    this.indexReader = indexReader;
  }

  private void seekDir(IndexInput input) throws IOException {
    input.seek(input.length() - CodecUtil.footerLength() - 8);
    long dirOffset = input.readLong();
    input.seek(dirOffset);
  }

  @Override
  public void close() throws IOException {
    try {
      try {
        if (indexReader != null) {
          indexReader.close();
        }
      } finally {
        // null so if an app hangs on to us (ie, we are not
        // GCable, despite being closed) we still free most
        // ram
        indexReader = null;
        if (in != null) {
          in.close();
        }
      }
    } finally {
      if (postingsReader != null) {
        postingsReader.close();
      }
    }
  }

  @Override
  public Iterator<String> iterator() {
    return Collections.unmodifiableSet(fields.keySet()).iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    assert field != null;
    return fields.get(field);
  }

  @Override
  public int size() {
    return fields.size();
  }

  private static final long FIELD_READER_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(FieldReader.class);

  private class FieldReader extends Terms implements Accountable {
    final long numTerms;
    final FieldInfo fieldInfo;
    final long termsStartPointer;
    final long sumTotalTermFreq;
    final long sumDocFreq;
    final int docCount;

    FieldReader(
        FieldInfo fieldInfo,
        long numTerms,
        long termsStartPointer,
        long sumTotalTermFreq,
        long sumDocFreq,
        int docCount) {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.termsStartPointer = termsStartPointer;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
    }

    @Override
    public long ramBytesUsed() {
      return FIELD_READER_RAM_BYTES_USED;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public boolean hasFreqs() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
      return fieldInfo
              .getIndexOptions()
              .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
          >= 0;
    }

    @Override
    public boolean hasPositions() {
      return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
      return fieldInfo.hasPayloads();
    }

    @Override
    public long size() {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return sumDocFreq;
    }

    @Override
    public int getDocCount() throws IOException {
      return docCount;
    }

    // Iterates through terms in this field
    private final class SegmentTermsEnum extends BaseTermsEnum {
      private final IndexInput in;
      private final BlockTermState state;
      private final boolean doOrd;
      private final FieldAndTerm fieldTerm = new FieldAndTerm();
      private final TermsIndexReaderBase.FieldIndexEnum indexEnum;
      private final BytesRefBuilder term = new BytesRefBuilder();

      /* This is true if indexEnum is "still" seek'd to the index term
      for the current term. We set it to true on seeking, and then it
      remains valid until next() is called enough times to load another
      terms block: */
      private boolean indexIsCurrent;

      /* True if we've already called .next() on the indexEnum, to "bracket"
      the current block of terms: */
      private boolean didIndexNext;

      /* Next index term, bracketing the current block of terms; this is
      only valid if didIndexNext is true: */
      private BytesRef nextIndexTerm;

      /* True after seekExact(TermState), do defer seeking.  If the app then
      calls next() (which is not "typical"), then we'll do the real seek */
      private boolean seekPending;

      private byte[] termSuffixes;
      private ByteArrayDataInput termSuffixesReader = new ByteArrayDataInput();

      /* Common prefix used for all terms in this block. */
      private int termBlockPrefix;

      /* How many terms in current block */
      private int blockTermCount;

      private byte[] docFreqBytes;
      private final ByteArrayDataInput freqReader = new ByteArrayDataInput();
      private int metaDataUpto;

      private byte[] bytes;
      private ByteArrayDataInput bytesReader;

      public SegmentTermsEnum() throws IOException {
        in = BlockTermsReader.this.in.clone();
        in.seek(termsStartPointer);
        indexEnum = indexReader.getFieldEnum(fieldInfo);
        doOrd = indexReader.supportsOrd();
        fieldTerm.field = fieldInfo.name;
        state = postingsReader.newTermState();
        state.totalTermFreq = -1;
        state.ord = -1;

        termSuffixes = new byte[128];
        docFreqBytes = new byte[64];
        // System.out.println("BTR.enum init this=" + this + " postingsReader=" + postingsReader);
      }

      // TODO: we may want an alternate mode here which is
      // "if you are about to return NOT_FOUND I won't use
      // the terms data from that"; eg FuzzyTermsEnum will
      // (usually) just immediately call seek again if we
      // return NOT_FOUND so it's a waste for us to fill in
      // the term that was actually NOT_FOUND
      @Override
      public SeekStatus seekCeil(final BytesRef target) throws IOException {

        if (indexEnum == null) {
          throw new IllegalStateException("terms index was not loaded");
        }

        // System.out.println("BTR.seek seg=" + segment + " target=" + fieldInfo.name + ":" +
        // target.utf8ToString() + " " + target + " current=" + term().utf8ToString() + " " + term()
        // + " indexIsCurrent=" + indexIsCurrent + " didIndexNext=" + didIndexNext + " seekPending="
        // + seekPending + " divisor=" + indexReader.getDivisor() + " this="  + this);
        if (didIndexNext) {
          if (nextIndexTerm == null) {
            // System.out.println("  nextIndexTerm=null");
          } else {
            // System.out.println("  nextIndexTerm=" + nextIndexTerm.utf8ToString());
          }
        }

        boolean doSeek = true;

        // See if we can avoid seeking, because target term
        // is after current term but before next index term:
        if (indexIsCurrent) {

          final int cmp = term.get().compareTo(target);

          if (cmp == 0) {
            // Already at the requested term
            return SeekStatus.FOUND;
          } else if (cmp < 0) {

            // Target term is after current term
            if (!didIndexNext) {
              if (indexEnum.next() == -1) {
                nextIndexTerm = null;
              } else {
                nextIndexTerm = indexEnum.term();
              }
              // System.out.println("  now do index next() nextIndexTerm=" + (nextIndexTerm == null
              // ? "null" : nextIndexTerm.utf8ToString()));
              didIndexNext = true;
            }

            if (nextIndexTerm == null || target.compareTo(nextIndexTerm) < 0) {
              // Optimization: requested term is within the
              // same term block we are now in; skip seeking
              // (but do scanning):
              doSeek = false;
              // System.out.println("  skip seek: nextIndexTerm=" + (nextIndexTerm == null ? "null"
              // : nextIndexTerm.utf8ToString()));
            }
          }
        }

        if (doSeek) {
          // System.out.println("  seek");

          // Ask terms index to find biggest indexed term (=
          // first term in a block) that's <= our text:
          in.seek(indexEnum.seek(target));
          boolean result = nextBlock();

          // Block must exist since, at least, the indexed term
          // is in the block:
          assert result;

          indexIsCurrent = true;
          didIndexNext = false;

          if (doOrd) {
            state.ord = indexEnum.ord() - 1;
          }

          term.copyBytes(indexEnum.term());
          // System.out.println("  seek: term=" + term.utf8ToString());
        } else {
          // System.out.println("  skip seek");
          if (state.termBlockOrd == blockTermCount && !nextBlock()) {
            indexIsCurrent = false;
            return SeekStatus.END;
          }
        }

        seekPending = false;

        int common = 0;

        // Scan within block.  We could do this by calling
        // _next() and testing the resulting term, but this
        // is wasteful.  Instead, we first confirm the
        // target matches the common prefix of this block,
        // and then we scan the term bytes directly from the
        // termSuffixesreader's byte[], saving a copy into
        // the BytesRef term per term.  Only when we return
        // do we then copy the bytes into the term.

        while (true) {

          // First, see if target term matches common prefix
          // in this block:
          if (common < termBlockPrefix) {
            final int cmp =
                (term.byteAt(common) & 0xFF) - (target.bytes[target.offset + common] & 0xFF);
            if (cmp < 0) {

              // TODO: maybe we should store common prefix
              // in block header?  (instead of relying on
              // last term of previous block)

              // Target's prefix is after the common block
              // prefix, so term cannot be in this block
              // but it could be in next block.  We
              // must scan to end-of-block to set common
              // prefix for next block:
              if (state.termBlockOrd < blockTermCount) {
                while (state.termBlockOrd < blockTermCount - 1) {
                  state.termBlockOrd++;
                  state.ord++;
                  termSuffixesReader.skipBytes(termSuffixesReader.readVInt());
                }
                final int suffix = termSuffixesReader.readVInt();
                term.setLength(termBlockPrefix + suffix);
                term.grow(term.length());
                termSuffixesReader.readBytes(term.bytes(), termBlockPrefix, suffix);
              }
              state.ord++;

              if (!nextBlock()) {
                indexIsCurrent = false;
                return SeekStatus.END;
              }
              common = 0;

            } else if (cmp > 0) {
              // Target's prefix is before the common prefix
              // of this block, so we position to start of
              // block and return NOT_FOUND:
              assert state.termBlockOrd == 0;

              final int suffix = termSuffixesReader.readVInt();
              term.setLength(termBlockPrefix + suffix);
              term.grow(term.length());
              termSuffixesReader.readBytes(term.bytes(), termBlockPrefix, suffix);
              return SeekStatus.NOT_FOUND;
            } else {
              common++;
            }

            continue;
          }

          // Test every term in this block
          while (true) {
            state.termBlockOrd++;
            state.ord++;

            final int suffix = termSuffixesReader.readVInt();

            // We know the prefix matches, so just compare the new suffix:
            final int termLen = termBlockPrefix + suffix;
            int bytePos = termSuffixesReader.getPosition();

            boolean next = false;
            final int limit = target.offset + (termLen < target.length ? termLen : target.length);
            int targetPos = target.offset + termBlockPrefix;
            while (targetPos < limit) {
              final int cmp = (termSuffixes[bytePos++] & 0xFF) - (target.bytes[targetPos++] & 0xFF);
              if (cmp < 0) {
                // Current term is still before the target;
                // keep scanning
                next = true;
                break;
              } else if (cmp > 0) {
                // Done!  Current term is after target. Stop
                // here, fill in real term, return NOT_FOUND.
                term.setLength(termBlockPrefix + suffix);
                term.grow(term.length());
                termSuffixesReader.readBytes(term.bytes(), termBlockPrefix, suffix);
                // System.out.println("  NOT_FOUND");
                return SeekStatus.NOT_FOUND;
              }
            }

            if (!next && target.length <= termLen) {
              term.setLength(termBlockPrefix + suffix);
              term.grow(term.length());
              termSuffixesReader.readBytes(term.bytes(), termBlockPrefix, suffix);

              if (target.length == termLen) {
                // Done!  Exact match.  Stop here, fill in
                // real term, return FOUND.
                // System.out.println("  FOUND");
                return SeekStatus.FOUND;
              } else {
                // System.out.println("  NOT_FOUND");
                return SeekStatus.NOT_FOUND;
              }
            }

            if (state.termBlockOrd == blockTermCount) {
              // Must pre-fill term for next block's common prefix
              term.setLength(termBlockPrefix + suffix);
              term.grow(term.length());
              termSuffixesReader.readBytes(term.bytes(), termBlockPrefix, suffix);
              break;
            } else {
              termSuffixesReader.skipBytes(suffix);
            }
          }

          // The purpose of the terms dict index is to seek
          // the enum to the closest index term before the
          // term we are looking for.  So, we should never
          // cross another index term (besides the first
          // one) while we are scanning:

          assert indexIsCurrent;

          if (!nextBlock()) {
            // System.out.println("  END");
            indexIsCurrent = false;
            return SeekStatus.END;
          }
          common = 0;
        }
      }

      @Override
      public BytesRef next() throws IOException {
        // System.out.println("BTR.next() seekPending=" + seekPending + " pendingSeekCount=" +
        // state.termBlockOrd);

        // If seek was previously called and the term was cached,
        // usually caller is just going to pull a D/&PEnum or get
        // docFreq, etc.  But, if they then call next(),
        // this method catches up all internal state so next()
        // works properly:
        if (seekPending) {
          assert !indexIsCurrent;
          in.seek(state.blockFilePointer);
          final int pendingSeekCount = state.termBlockOrd;
          boolean result = nextBlock();

          final long savOrd = state.ord;

          // Block must exist since seek(TermState) was called w/ a
          // TermState previously returned by this enum when positioned
          // on a real term:
          assert result;

          while (state.termBlockOrd < pendingSeekCount) {
            BytesRef nextResult = _next();
            assert nextResult != null;
          }
          seekPending = false;
          state.ord = savOrd;
        }
        return _next();
      }

      /* Decodes only the term bytes of the next term.  If caller then asks for
      metadata, ie docFreq, totalTermFreq or pulls a D/&PEnum, we then (lazily)
      decode all metadata up to the current term. */
      private BytesRef _next() throws IOException {
        // System.out.println("BTR._next seg=" + segment + " this=" + this + " termCount=" +
        // state.termBlockOrd + " (vs " + blockTermCount + ")");
        if (state.termBlockOrd == blockTermCount && !nextBlock()) {
          // System.out.println("  eof");
          indexIsCurrent = false;
          return null;
        }

        // TODO: cutover to something better for these ints!  simple64?
        final int suffix = termSuffixesReader.readVInt();
        // System.out.println("  suffix=" + suffix);

        term.setLength(termBlockPrefix + suffix);
        term.grow(term.length());
        termSuffixesReader.readBytes(term.bytes(), termBlockPrefix, suffix);
        state.termBlockOrd++;

        // NOTE: meaningless in the non-ord case
        state.ord++;

        // System.out.println("  return term=" + fieldInfo.name + ":" + term.utf8ToString() + " " +
        // term + " tbOrd=" + state.termBlockOrd);
        return term.get();
      }

      @Override
      public BytesRef term() {
        return term.get();
      }

      @Override
      public int docFreq() throws IOException {
        // System.out.println("BTR.docFreq");
        decodeMetaData();
        // System.out.println("  return " + state.docFreq);
        return state.docFreq;
      }

      @Override
      public long totalTermFreq() throws IOException {
        decodeMetaData();
        return state.totalTermFreq;
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        // System.out.println("BTR.docs this=" + this);
        decodeMetaData();
        // System.out.println("BTR.docs:  state.docFreq=" + state.docFreq);
        return postingsReader.postings(fieldInfo, state, reuse, flags);
      }

      @Override
      public ImpactsEnum impacts(int flags) throws IOException {
        decodeMetaData();
        return postingsReader.impacts(fieldInfo, state, flags);
      }

      @Override
      public void seekExact(BytesRef target, TermState otherState) {
        // System.out.println("BTR.seekExact termState target=" + target.utf8ToString() + " " +
        // target + " this=" + this);
        assert otherState != null && otherState instanceof BlockTermState;
        assert !doOrd || ((BlockTermState) otherState).ord < numTerms;
        state.copyFrom(otherState);
        seekPending = true;
        indexIsCurrent = false;
        term.copyBytes(target);
      }

      @Override
      public TermState termState() throws IOException {
        // System.out.println("BTR.termState this=" + this);
        decodeMetaData();
        TermState ts = state.clone();
        // System.out.println("  return ts=" + ts);
        return ts;
      }

      @Override
      public void seekExact(long ord) throws IOException {
        // System.out.println("BTR.seek by ord ord=" + ord);
        if (indexEnum == null) {
          throw new IllegalStateException("terms index was not loaded");
        }

        assert ord < numTerms;

        // TODO: if ord is in same terms block and
        // after current ord, we should avoid this seek just
        // like we do in the seek(BytesRef) case
        in.seek(indexEnum.seek(ord));
        boolean result = nextBlock();

        // Block must exist since ord < numTerms:
        assert result;

        indexIsCurrent = true;
        didIndexNext = false;
        seekPending = false;

        state.ord = indexEnum.ord() - 1;
        assert state.ord >= -1 : "ord=" + state.ord;
        term.copyBytes(indexEnum.term());

        // Now, scan:
        int left = (int) (ord - state.ord);
        while (left > 0) {
          final BytesRef term = _next();
          assert term != null;
          left--;
          assert indexIsCurrent;
        }
      }

      @Override
      public long ord() {
        if (!doOrd) {
          throw new UnsupportedOperationException();
        }
        return state.ord;
      }

      /* Does initial decode of next block of terms; this
      doesn't actually decode the docFreq, totalTermFreq,
      postings details (frq/prx offset, etc.) metadata;
      it just loads them as byte[] blobs which are then
      decoded on-demand if the metadata is ever requested
      for any term in this block.  This enables terms-only
      intensive consumes (eg certain MTQs, respelling) to
      not pay the price of decoding metadata they won't
      use. */
      private boolean nextBlock() throws IOException {

        // TODO: we still lazy-decode the byte[] for each
        // term (the suffix), but, if we decoded
        // all N terms up front then seeking could do a fast
        // bsearch w/in the block...

        // System.out.println("BTR.nextBlock() fp=" + in.getFilePointer() + " this=" + this);
        state.blockFilePointer = in.getFilePointer();
        blockTermCount = in.readVInt();
        // System.out.println("  blockTermCount=" + blockTermCount);
        if (blockTermCount == 0) {
          return false;
        }
        termBlockPrefix = in.readVInt();

        // term suffixes:
        int len = in.readVInt();
        if (termSuffixes.length < len) {
          termSuffixes = new byte[ArrayUtil.oversize(len, 1)];
        }
        // System.out.println("  termSuffixes len=" + len);
        in.readBytes(termSuffixes, 0, len);
        termSuffixesReader.reset(termSuffixes, 0, len);

        // docFreq, totalTermFreq
        len = in.readVInt();
        if (docFreqBytes.length < len) {
          docFreqBytes = new byte[ArrayUtil.oversize(len, 1)];
        }
        // System.out.println("  freq bytes len=" + len);
        in.readBytes(docFreqBytes, 0, len);
        freqReader.reset(docFreqBytes, 0, len);

        // metadata
        len = in.readVInt();
        if (bytes == null) {
          bytes = new byte[ArrayUtil.oversize(len, 1)];
          bytesReader = new ByteArrayDataInput();
        } else if (bytes.length < len) {
          bytes = new byte[ArrayUtil.oversize(len, 1)];
        }
        in.readBytes(bytes, 0, len);
        bytesReader.reset(bytes, 0, len);

        metaDataUpto = 0;
        state.termBlockOrd = 0;

        indexIsCurrent = false;
        // System.out.println("  indexIsCurrent=" + indexIsCurrent);

        return true;
      }

      private void decodeMetaData() throws IOException {
        // System.out.println("BTR.decodeMetadata mdUpto=" + metaDataUpto + " vs termCount=" +
        // state.termBlockOrd + " state=" + state);
        if (!seekPending) {
          // TODO: cutover to random-access API
          // here.... really stupid that we have to decode N
          // wasted term metadata just to get to the N+1th
          // that we really need...

          // lazily catch up on metadata decode:
          final int limit = state.termBlockOrd;
          boolean absolute = metaDataUpto == 0;
          // TODO: better API would be "jump straight to term=N"???
          while (metaDataUpto < limit) {
            // System.out.println("  decode mdUpto=" + metaDataUpto);
            // TODO: we could make "tiers" of metadata, ie,
            // decode docFreq/totalTF but don't decode postings
            // metadata; this way caller could get
            // docFreq/totalTF w/o paying decode cost for
            // postings

            // TODO: if docFreq were bulk decoded we could
            // just skipN here:

            // docFreq, totalTermFreq
            state.docFreq = freqReader.readVInt();
            // System.out.println("    dF=" + state.docFreq);
            if (fieldInfo.getIndexOptions() == IndexOptions.DOCS) {
              state.totalTermFreq = state.docFreq; // all postings have tf=1
            } else {
              state.totalTermFreq = state.docFreq + freqReader.readVLong();
              // System.out.println("    totTF=" + state.totalTermFreq);
            }
            // metadata
            postingsReader.decodeTerm(bytesReader, fieldInfo, state, absolute);
            metaDataUpto++;
            absolute = false;
          }
        } else {
          // System.out.println("  skip! seekPending");
        }
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = BASE_RAM_BYTES_USED;
    ramBytesUsed += (postingsReader != null) ? postingsReader.ramBytesUsed() : 0;
    ramBytesUsed += (indexReader != null) ? indexReader.ramBytesUsed() : 0;
    ramBytesUsed += fields.size() * 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    for (FieldReader reader : fields.values()) {
      ramBytesUsed += reader.ramBytesUsed();
    }
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    if (indexReader != null) {
      resources.add(Accountables.namedAccountable("term index", indexReader));
    }
    if (postingsReader != null) {
      resources.add(Accountables.namedAccountable("delegate", postingsReader));
    }
    return Collections.unmodifiableList(resources);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(index="
        + indexReader
        + ",delegate="
        + postingsReader
        + ")";
  }

  @Override
  public void checkIntegrity() throws IOException {
    // verify terms
    CodecUtil.checksumEntireFile(in);

    // verify postings
    postingsReader.checkIntegrity();
  }
}
