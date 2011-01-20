package org.apache.lucene.index.codecs;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.standard.StandardPostingsReader; // javadocs
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.DoubleBarrelLRUCache;

/** Handles a terms dict, but decouples all details of
 *  doc/freqs/positions reading to an instance of {@link
 *  PostingsReaderBase}.  This class is reusable for
 *  codecs that use a different format for
 *  docs/freqs/positions (though codecs are also free to
 *  make their own terms dict impl).
 *
 * <p>This class also interacts with an instance of {@link
 * TermsIndexReaderBase}, to abstract away the specific
 * implementation of the terms dict index. 
 * @lucene.experimental */

public class BlockTermsReader extends FieldsProducer {
  // Open input to the main terms dict file (_X.tis)
  private final IndexInput in;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  private final PostingsReaderBase postingsReader;

  private final TreeMap<String,FieldReader> fields = new TreeMap<String,FieldReader>();

  // Comparator that orders our terms
  private final Comparator<BytesRef> termComp;

  // Caches the most recently looked-up field + terms:
  private final DoubleBarrelLRUCache<FieldAndTerm,BlockTermState> termsCache;

  // Reads the terms index
  private TermsIndexReaderBase indexReader;

  // keeps the dirStart offset
  protected long dirOffset;

  // Used as key for the terms cache
  private static class FieldAndTerm extends DoubleBarrelLRUCache.CloneableKey {
    String field;
    BytesRef term;

    public FieldAndTerm() {
    }

    public FieldAndTerm(FieldAndTerm other) {
      field = other.field;
      term = new BytesRef(other.term);
    }

    @Override
    public boolean equals(Object _other) {
      FieldAndTerm other = (FieldAndTerm) _other;
      return other.field == field && term.bytesEquals(other.term);
    }

    @Override
    public Object clone() {
      return new FieldAndTerm(this);
    }

    @Override
    public int hashCode() {
      return field.hashCode() * 31 + term.hashCode();
    }
  }
  
  private String segment;
  
  public BlockTermsReader(TermsIndexReaderBase indexReader, Directory dir, FieldInfos fieldInfos, String segment, PostingsReaderBase postingsReader, int readBufferSize,
                          Comparator<BytesRef> termComp, int termsCacheSize, String codecId)
    throws IOException {
    
    this.postingsReader = postingsReader;
    termsCache = new DoubleBarrelLRUCache<FieldAndTerm,BlockTermState>(termsCacheSize);

    this.termComp = termComp;
    this.segment = segment;
    in = dir.openInput(IndexFileNames.segmentFileName(segment, codecId, BlockTermsWriter.TERMS_EXTENSION),
                       readBufferSize);

    boolean success = false;
    try {
      readHeader(in);

      // Have PostingsReader init itself
      postingsReader.init(in);

      // Read per-field details
      seekDir(in, dirOffset);

      final int numFields = in.readVInt();

      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final long numTerms = in.readVLong();
        assert numTerms >= 0;
        final long termsStartPointer = in.readVLong();
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        final long sumTotalTermFreq = fieldInfo.omitTermFreqAndPositions ? -1 : in.readVLong();
        assert !fields.containsKey(fieldInfo.name);
        fields.put(fieldInfo.name, new FieldReader(fieldInfo, numTerms, termsStartPointer, sumTotalTermFreq));
      }
      success = true;
    } finally {
      if (!success) {
        in.close();
      }
    }

    this.indexReader = indexReader;
  }

  protected void readHeader(IndexInput input) throws IOException {
    CodecUtil.checkHeader(in, BlockTermsWriter.CODEC_NAME,
                          BlockTermsWriter.VERSION_START,
                          BlockTermsWriter.VERSION_CURRENT);
    dirOffset = in.readLong();    
  }
  
  protected void seekDir(IndexInput input, long dirOffset)
      throws IOException {
    input.seek(dirOffset);
  }
  
  @Override
  public void loadTermsIndex(int indexDivisor) throws IOException {
    indexReader.loadTermsIndex(indexDivisor);
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
      try {
        if (postingsReader != null) {
          postingsReader.close();
        }
      } finally {
        for(FieldReader field : fields.values()) {
          field.close();
        }
      }
    }
  }

  public static void files(Directory dir, SegmentInfo segmentInfo, String id, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, id, BlockTermsWriter.TERMS_EXTENSION));
  }

  public static void getExtensions(Collection<String> extensions) {
    extensions.add(BlockTermsWriter.TERMS_EXTENSION);
  }

  @Override
  public FieldsEnum iterator() {
    return new TermFieldsEnum();
  }

  @Override
  public Terms terms(String field) throws IOException {
    return fields.get(field);
  }

  // Iterates through all fields
  private class TermFieldsEnum extends FieldsEnum {
    final Iterator<FieldReader> it;
    FieldReader current;

    TermFieldsEnum() {
      it = fields.values().iterator();
    }

    @Override
    public String next() {
      if (it.hasNext()) {
        current = it.next();
        return current.fieldInfo.name;
      } else {
        current = null;
        return null;
      }
    }
    
    @Override
    public TermsEnum terms() throws IOException {
      return current.iterator();
    }
  }

  private class FieldReader extends Terms implements Closeable {
    final long numTerms;
    final FieldInfo fieldInfo;
    final long termsStartPointer;
    final long sumTotalTermFreq;

    FieldReader(FieldInfo fieldInfo, long numTerms, long termsStartPointer, long sumTotalTermFreq) {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.termsStartPointer = termsStartPointer;
      this.sumTotalTermFreq = sumTotalTermFreq;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return termComp;
    }

    @Override
    public void close() {
      super.close();
    }
    
    @Override
    public TermsEnum iterator() throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public long getUniqueTermCount() {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }

    // Iterates through terms in this field
    private final class SegmentTermsEnum extends TermsEnum {
      private final IndexInput in;
      private final BlockTermState state;
      private final boolean doOrd;
      private final FieldAndTerm fieldTerm = new FieldAndTerm();
      private final TermsIndexReaderBase.FieldIndexEnum indexEnum;
      private final BytesRef term = new BytesRef();

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

      /* True after seek(TermState), do defer seeking.  If the app then
         calls next() (which is not "typical"), then we'll do the real seek */
      private boolean seekPending;

      /* How many blocks we've read since last seek.  Once this
         is >= indexEnum.getDivisor() we set indexIsCurrent to false (since
         the index can no long bracket seek-within-block). */
      private int blocksSinceSeek;

      private byte[] termSuffixes;
      private ByteArrayDataInput termSuffixesReader = new ByteArrayDataInput(null);

      /* Common prefix used for all terms in this block. */
      private int termBlockPrefix;

      private byte[] docFreqBytes;
      private final ByteArrayDataInput freqReader = new ByteArrayDataInput(null);
      private int metaDataUpto;

      public SegmentTermsEnum() throws IOException {
        in = (IndexInput) BlockTermsReader.this.in.clone();
        in.seek(termsStartPointer);
        indexEnum = indexReader.getFieldEnum(fieldInfo);
        doOrd = indexReader.supportsOrd();
        fieldTerm.field = fieldInfo.name;
        state = postingsReader.newTermState();
        state.totalTermFreq = -1;
        state.ord = -1;

        termSuffixes = new byte[128];
        docFreqBytes = new byte[64];
        //System.out.println("BTR.enum init this=" + this + " postingsReader=" + postingsReader);
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return termComp;
      }

      @Override
      public SeekStatus seek(final BytesRef target, final boolean useCache) throws IOException {

        if (indexEnum == null) {
          throw new IllegalStateException("terms index was not loaded");
        }
        
        //System.out.println("BTR.seek seg=" + segment + " target=" + fieldInfo.name + ":" + target.utf8ToString() + " " + target + " current=" + term().utf8ToString() + " " + term() + " useCache=" + useCache + " indexIsCurrent=" + indexIsCurrent + " didIndexNext=" + didIndexNext + " seekPending=" + seekPending + " divisor=" + indexReader.getDivisor() + " this="  + this);
        /*
        if (didIndexNext) {
          if (nextIndexTerm == null) {
            //System.out.println("  nextIndexTerm=null");
          } else {
            //System.out.println("  nextIndexTerm=" + nextIndexTerm.utf8ToString());
          }
        }
        */

        // Check cache
        if (useCache) {
          fieldTerm.term = target;
          // TODO: should we differentiate "frozen"
          // TermState (ie one that was cloned and
          // cached/returned by termState()) from the
          // malleable (primary) one?
          final TermState cachedState = termsCache.get(fieldTerm);
          if (cachedState != null) {
            seekPending = true;
            //System.out.println("  cached!");
            seek(target, cachedState);
            //System.out.println("  term=" + term.utf8ToString());
            return SeekStatus.FOUND;
          }
        }

        boolean doSeek = true;

        // See if we can avoid seeking, because target term
        // is after current term but before next index term:
        if (indexIsCurrent) {

          final int cmp = termComp.compare(term, target);

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
              //System.out.println("  now do index next() nextIndexTerm=" + (nextIndexTerm == null ? "null" : nextIndexTerm.utf8ToString()));
              didIndexNext = true;
            }

            if (nextIndexTerm == null || termComp.compare(target, nextIndexTerm) < 0) {
              // Optimization: requested term is within the
              // same term block we are now in; skip seeking
              // (but do scanning):
              doSeek = false;
              //System.out.println("  skip seek: nextIndexTerm=" + (nextIndexTerm == null ? "null" : nextIndexTerm.utf8ToString()));
            }
          }
        }

        if (doSeek) {
          //System.out.println("  seek");

          // Ask terms index to find biggest indexed term (=
          // first term in a block) that's <= our text:
          in.seek(indexEnum.seek(target));
          boolean result = nextBlock();

          // Block must exist since, at least, the indexed term
          // is in the block:
          assert result;

          indexIsCurrent = true;
          didIndexNext = false;
          blocksSinceSeek = 0;          

          if (doOrd) {
            state.ord = indexEnum.ord()-1;
          }

          // NOTE: the first _next() after an index seek is
          // a bit wasteful, since it redundantly reads some
          // suffix bytes into the buffer.  We could avoid storing
          // those bytes in the primary file, but then when
          // next()ing over an index term we'd have to
          // special case it:
          term.copy(indexEnum.term());
          //System.out.println("  seek: term=" + term.utf8ToString());
        } else {
          ////System.out.println("  skip seek");
        }

        seekPending = false;

        // Now scan:
        while (_next() != null) {
          final int cmp = termComp.compare(term, target);
          if (cmp == 0) {
            // Match!
            if (useCache) {
              // Store in cache
              decodeMetaData();
              termsCache.put(new FieldAndTerm(fieldTerm), (BlockTermState) state.clone());
            }
            //System.out.println("  FOUND");
            return SeekStatus.FOUND;
          } else if (cmp > 0) {
            //System.out.println("  NOT_FOUND term=" + term.utf8ToString());
            return SeekStatus.NOT_FOUND;
          }
          
          // The purpose of the terms dict index is to seek
          // the enum to the closest index term before the
          // term we are looking for.  So, we should never
          // cross another index term (besides the first
          // one) while we are scanning:
          assert indexIsCurrent;
        }

        indexIsCurrent = false;
        //System.out.println("  END");
        return SeekStatus.END;
      }

      @Override
      public BytesRef next() throws IOException {
        //System.out.println("BTR.next() seekPending=" + seekPending + " pendingSeekCount=" + state.termCount);

        // If seek was previously called and the term was cached,
        // usually caller is just going to pull a D/&PEnum or get
        // docFreq, etc.  But, if they then call next(),
        // this method catches up all internal state so next()
        // works properly:
        if (seekPending) {
          assert !indexIsCurrent;
          in.seek(state.blockFilePointer);
          final int pendingSeekCount = state.termCount;
          boolean result = nextBlock();

          final long savOrd = state.ord;

          // Block must exist since seek(TermState) was called w/ a
          // TermState previously returned by this enum when positioned
          // on a real term:
          assert result;

          while(state.termCount < pendingSeekCount) {
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
        //System.out.println("BTR._next this=" + this + " termCount=" + state.termCount + " (vs " + state.blockTermCount + ")");
        if (state.termCount == state.blockTermCount) {
          if (!nextBlock()) {
            //System.out.println("  eof");
            indexIsCurrent = false;
            return null;
          }
        }

        // TODO: cutover to something better for these ints!  simple64?
        final int suffix = termSuffixesReader.readVInt();
        //System.out.println("  suffix=" + suffix);

        term.length = termBlockPrefix + suffix;
        if (term.bytes.length < term.length) {
          term.grow(term.length);
        }
        termSuffixesReader.readBytes(term.bytes, termBlockPrefix, suffix);
        state.termCount++;

        // NOTE: meaningless in the non-ord case
        state.ord++;

        //System.out.println("  return term=" + fieldInfo.name + ":" + term.utf8ToString() + " " + term);
        return term;
      }

      @Override
      public BytesRef term() {
        return term;
      }

      @Override
      public int docFreq() throws IOException {
        //System.out.println("BTR.docFreq");
        decodeMetaData();
        //System.out.println("  return " + state.docFreq);
        return state.docFreq;
      }

      @Override
      public long totalTermFreq() throws IOException {
        decodeMetaData();
        return state.totalTermFreq;
      }

      @Override
      public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
        //System.out.println("BTR.docs this=" + this);
        decodeMetaData();
        //System.out.println("  state.docFreq=" + state.docFreq);
        final DocsEnum docsEnum = postingsReader.docs(fieldInfo, state, skipDocs, reuse);
        assert docsEnum != null;
        return docsEnum;
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
        //System.out.println("BTR.d&p this=" + this);
        decodeMetaData();
        if (fieldInfo.omitTermFreqAndPositions) {
          return null;
        } else {
          DocsAndPositionsEnum dpe = postingsReader.docsAndPositions(fieldInfo, state, skipDocs, reuse);
          //System.out.println("  return d&pe=" + dpe);
          return dpe;
        }
      }

      @Override
      public void seek(BytesRef target, TermState otherState) throws IOException {
        //System.out.println("BTR.seek termState target=" + target.utf8ToString() + " " + target + " this=" + this);
        assert otherState != null && otherState instanceof BlockTermState;
        assert !doOrd || ((BlockTermState) otherState).ord < numTerms;
        state.copyFrom(otherState);
        seekPending = true;
        indexIsCurrent = false;
        term.copy(target);
      }
      
      @Override
      public TermState termState() throws IOException {
        //System.out.println("BTR.termState this=" + this);
        decodeMetaData();
        TermState ts = (TermState) state.clone();
        //System.out.println("  return ts=" + ts);
        return ts;
      }

      @Override
      public SeekStatus seek(long ord) throws IOException {
        //System.out.println("BTR.seek by ord ord=" + ord);
        if (indexEnum == null) {
          throw new IllegalStateException("terms index was not loaded");
        }

        if (ord >= numTerms) {
          state.ord = numTerms-1;
          return SeekStatus.END;
        }

        // TODO: if ord is in same terms block and
        // after current ord, we should avoid this seek just
        // like we do in the seek(BytesRef) case
        in.seek(indexEnum.seek(ord));
        boolean result = nextBlock();

        // Block must exist since ord < numTerms:
        assert result;

        indexIsCurrent = true;
        didIndexNext = false;
        blocksSinceSeek = 0;
        seekPending = false;

        state.ord = indexEnum.ord()-1;
        assert state.ord >= -1: "ord=" + state.ord;
        term.copy(indexEnum.term());

        // Now, scan:
        int left = (int) (ord - state.ord);
        while(left > 0) {
          final BytesRef term = _next();
          assert term != null;
          left--;
          assert indexIsCurrent;
        }

        // always found
        return SeekStatus.FOUND;
      }

      public long ord() {
        if (!doOrd) {
          throw new UnsupportedOperationException();
        }
        return state.ord;
      }

      private void doPendingSeek() {
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

        //System.out.println("BTR.nextBlock() fp=" + in.getFilePointer() + " this=" + this);
        state.blockFilePointer = in.getFilePointer();
        state.blockTermCount = in.readVInt();
        //System.out.println("  blockTermCount=" + state.blockTermCount);
        if (state.blockTermCount == 0) {
          return false;
        }
        termBlockPrefix = in.readVInt();

        // term suffixes:
        int len = in.readVInt();
        if (termSuffixes.length < len) {
          termSuffixes = new byte[ArrayUtil.oversize(len, 1)];
        }
        //System.out.println("  termSuffixes len=" + len);
        in.readBytes(termSuffixes, 0, len);
        termSuffixesReader.reset(termSuffixes);

        // docFreq, totalTermFreq
        len = in.readVInt();
        if (docFreqBytes.length < len) {
          docFreqBytes = new byte[ArrayUtil.oversize(len, 1)];
        }
        //System.out.println("  freq bytes len=" + len);
        in.readBytes(docFreqBytes, 0, len);
        freqReader.reset(docFreqBytes);
        metaDataUpto = 0;

        state.termCount = 0;

        postingsReader.readTermsBlock(in, fieldInfo, state);

        blocksSinceSeek++;
        indexIsCurrent &= (blocksSinceSeek < indexReader.getDivisor());
        //System.out.println("  indexIsCurrent=" + indexIsCurrent);

        return true;
      }

      private void decodeMetaData() throws IOException {
        //System.out.println("BTR.decodeMetadata mdUpto=" + metaDataUpto + " vs termCount=" + state.termCount + " state=" + state);
        if (!seekPending) {
          // lazily catch up on metadata decode:
          final int limit = state.termCount;
          state.termCount = metaDataUpto;
          while (metaDataUpto < limit) {
            //System.out.println("  decode");
            // TODO: we could make "tiers" of metadata, ie,
            // decode docFreq/totalTF but don't decode postings
            // metadata; this way caller could get
            // docFreq/totalTF w/o paying decode cost for
            // postings
            state.docFreq = freqReader.readVInt();
            if (!fieldInfo.omitTermFreqAndPositions) {
              state.totalTermFreq = state.docFreq + freqReader.readVLong();
            }
            postingsReader.nextTerm(fieldInfo, state);
            metaDataUpto++;
            state.termCount++;
          }
        } else {
          //System.out.println("  skip! seekPending");
        }
      }
    }
  }
}
