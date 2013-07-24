package org.apache.lucene.codecs.temp;

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
import java.io.PrintWriter;
import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.TempPostingsReaderBase;
import org.apache.lucene.codecs.CodecUtil;

public class TempFSTTermsReader extends FieldsProducer {
  final TreeMap<String, TermsReader> fields = new TreeMap<String, TermsReader>();
  final TempPostingsReaderBase postingsReader;
  final IndexInput in;
  //static boolean DEBUG = false;

  public TempFSTTermsReader(SegmentReadState state, TempPostingsReaderBase postingsReader) throws IOException {
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TempFSTTermsWriter.TERMS_EXTENSION);

    this.postingsReader = postingsReader;
    this.in = state.directory.openInput(termsFileName, state.context);

    boolean success = false;
    try {
      readHeader(in);
      this.postingsReader.init(in);
      seekDir(in);

      final FieldInfos fieldInfos = state.fieldInfos;
      final int numFields = in.readVInt();
      for (int i = 0; i < numFields; i++) {
        int fieldNumber = in.readVInt();
        FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
        long numTerms = in.readVLong();
        long sumTotalTermFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY ? -1 : in.readVLong();
        long sumDocFreq = in.readVLong();
        int docCount = in.readVInt();
        int longsSize = in.readVInt();
        TermsReader current = new TermsReader(fieldInfo, numTerms, sumTotalTermFreq, sumDocFreq, docCount, longsSize);
        TermsReader previous = fields.put(fieldInfo.name, current);
        checkFieldSummary(state.segmentInfo, current, previous);
      }
      success = true;
    } finally {
      if (!success) {
        in.close();
      }
    }
  }

  private int readHeader(IndexInput in) throws IOException {
    return CodecUtil.checkHeader(in, TempFSTTermsWriter.TERMS_CODEC_NAME,
                                     TempFSTTermsWriter.TERMS_VERSION_START,
                                     TempFSTTermsWriter.TERMS_VERSION_CURRENT);
  }
  private void seekDir(IndexInput in) throws IOException {
    in.seek(in.length() - 8);
    in.seek(in.readLong());
  }
  private void checkFieldSummary(SegmentInfo info, TermsReader field, TermsReader previous) throws IOException {
    // #docs with field must be <= #docs
    if (field.docCount < 0 || field.docCount > info.getDocCount()) {
      throw new CorruptIndexException("invalid docCount: " + field.docCount + " maxDoc: " + info.getDocCount() + " (resource=" + in + ")");
    }
    // #postings must be >= #docs with field
    if (field.sumDocFreq < field.docCount) {
      throw new CorruptIndexException("invalid sumDocFreq: " + field.sumDocFreq + " docCount: " + field.docCount + " (resource=" + in + ")");
    }
    // #positions must be >= #postings
    if (field.sumTotalTermFreq != -1 && field.sumTotalTermFreq < field.sumDocFreq) {
      throw new CorruptIndexException("invalid sumTotalTermFreq: " + field.sumTotalTermFreq + " sumDocFreq: " + field.sumDocFreq + " (resource=" + in + ")");
    }
    if (previous != null) {
      throw new CorruptIndexException("duplicate fields: " + field.fieldInfo.name + " (resource=" + in + ")");
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

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(in, postingsReader);
    } finally {
      fields.clear();
    }
  }

  final class TermsReader extends Terms {
    final FieldInfo fieldInfo;
    final long numTerms;
    final long sumTotalTermFreq;
    final long sumDocFreq;
    final int docCount;
    final int longsSize;
    final FST<TempTermOutputs.TempMetaData> dict;

    TermsReader(FieldInfo fieldInfo, long numTerms, long sumTotalTermFreq, long sumDocFreq, int docCount, int longsSize) throws IOException {
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.sumTotalTermFreq = sumTotalTermFreq;
      this.sumDocFreq = sumDocFreq;
      this.docCount = docCount;
      this.longsSize = longsSize;
      this.dict = new FST<TempTermOutputs.TempMetaData>(in, new TempTermOutputs(fieldInfo, longsSize));
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
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

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new IntersectTermsEnum(compiled, startTerm);
    }

    // Only wraps common operations for PBF interact
    abstract class BaseTermsEnum extends TermsEnum {
      /* Current term, null when enum ends or unpositioned */
      BytesRef term;

      /* Current term stats + decoded metadata (customized by PBF) */
      final TempTermState state;

      /* Current term stats + undecoded metadata (long[] & byte[]) */
      TempTermOutputs.TempMetaData meta;
      ByteArrayDataInput bytesReader;

      /** Decodes metadata into customized term state */
      abstract void decodeMetaData() throws IOException;

      BaseTermsEnum() throws IOException {
        this.state = postingsReader.newTermState();
        this.bytesReader = new ByteArrayDataInput();
        this.term = null;
        // NOTE: metadata will only be initialized in child class
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public TermState termState() throws IOException {
        decodeMetaData();
        return state.clone();
      }

      @Override
      public BytesRef term() {
        return term;
      }

      @Override
      public int docFreq() throws IOException {
        return state.docFreq;
      }

      @Override
      public long totalTermFreq() throws IOException {
        return state.totalTermFreq;
      }

      @Override
      public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
        decodeMetaData();
        return postingsReader.docs(fieldInfo, state, liveDocs, reuse, flags);
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
        if (!hasPositions()) {
          return null;
        }
        decodeMetaData();
        return postingsReader.docsAndPositions(fieldInfo, state, liveDocs, reuse, flags);
      }

      // nocommit: do we need this? for SegmentTermsEnum, we can maintain
      // a stack to record how current term is constructed on FST, (and ord on each alphabet)
      // so that during seek we don't have to start from the first arc.
      // however, we'll be implementing a new fstEnum instead of wrapping current one.
      //
      // nocommit: this can also be achieved by making use of Util.getByOutput()
      @Override
      public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() {
        throw new UnsupportedOperationException();
      }
    }


    // Iterates through all terms in this field
    private final class SegmentTermsEnum extends BaseTermsEnum {
      final BytesRefFSTEnum<TempTermOutputs.TempMetaData> fstEnum;

      /* True when current term's metadata is decoded */
      boolean decoded;

      /* True when current enum is 'positioned' by seekExact(TermState) */
      boolean seekPending;

      SegmentTermsEnum() throws IOException {
        super();
        this.fstEnum = new BytesRefFSTEnum<TempTermOutputs.TempMetaData>(dict);
        this.decoded = false;
        this.seekPending = false;
        this.meta = null;
      }

      // Let PBF decode metadata from long[] and byte[]
      @Override
      void decodeMetaData() throws IOException {
        if (!decoded && !seekPending) {
          if (meta.bytes != null) {
            bytesReader.reset(meta.bytes, 0, meta.bytes.length);
          }
          postingsReader.decodeTerm(meta.longs, bytesReader, fieldInfo, state);
          decoded = true;
        }
      }

      // Update current enum according to FSTEnum
      void updateEnum(final InputOutput<TempTermOutputs.TempMetaData> pair) {
        if (pair == null) {
          term = null;
        } else {
          term = pair.input;
          meta = pair.output;
          state.docFreq = meta.docFreq;
          state.totalTermFreq = meta.totalTermFreq;
        }
        decoded = false;
        seekPending = false;
      }

      @Override
      public BytesRef next() throws IOException {
        if (seekPending) {  // previously positioned, but termOutputs not fetched
          seekPending = false;
          SeekStatus status = seekCeil(term, false);
          assert status == SeekStatus.FOUND;  // must positioned on valid term
        }
        updateEnum(fstEnum.next());
        return term;
      }

      @Override
      public boolean seekExact(BytesRef target, boolean useCache) throws IOException {
        updateEnum(fstEnum.seekExact(target));
        return term != null;
      }

      @Override
      public SeekStatus seekCeil(BytesRef target, boolean useCache) throws IOException {
        updateEnum(fstEnum.seekCeil(target));
        if (term == null) {
          return SeekStatus.END;
        } else {
          return term.equals(target) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
        }
      }

      @Override
      public void seekExact(BytesRef target, TermState otherState) {
        if (!target.equals(term)) {
          state.copyFrom(otherState);
          term = BytesRef.deepCopyOf(target);
          seekPending = true;
        }
      }
    }

    // Iterates intersect result with automaton (cannot seek!)
    private final class IntersectTermsEnum extends BaseTermsEnum {
      /* True when current term's metadata is decoded */
      boolean decoded;

      /* True when there is pending term when calling next() */
      boolean pending;

      /* stack to record how current term is constructed, used to accumulate
       * metadata or rewind term:
       *   level == term.length + 1,
       *         == 0 when term is null */
      Frame[] stack;
      int level;

      /* term dict fst */
      final FST<TempTermOutputs.TempMetaData> fst;
      final FST.BytesReader fstReader;
      final Outputs<TempTermOutputs.TempMetaData> fstOutputs;

      /* query automaton to intersect with */
      final ByteRunAutomaton fsa;

      private final class Frame {
        /* fst stats */
        FST.Arc<TempTermOutputs.TempMetaData> fstArc;

        /* automaton stats */
        int fsaState;

        Frame() {
          this.fstArc = new FST.Arc<TempTermOutputs.TempMetaData>();
          this.fsaState = -1;
        }

        public String toString() {
          return "arc=" + fstArc + " state=" + fsaState;
        }
      }

      IntersectTermsEnum(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
        super();
        //if (DEBUG) System.out.println("Enum init, startTerm=" + startTerm);
        this.fst = dict;
        this.fstReader = fst.getBytesReader();
        this.fstOutputs = dict.outputs;
        this.fsa = compiled.runAutomaton;
        /*
        PrintWriter pw1 = new PrintWriter(new File("../temp/fst.txt"));
        Util.toDot(dict,pw1, false, false);
        pw1.close();
        PrintWriter pw2 = new PrintWriter(new File("../temp/fsa.txt"));
        pw2.write(compiled.toDot());
        pw2.close();
        */
        this.meta = fstOutputs.getNoOutput();
        this.level = -1;
        this.stack = new Frame[16];
        for (int i = 0 ; i < stack.length; i++) {
          this.stack[i] = new Frame();
        }

        Frame frame;
        frame = loadVirtualFrame(newFrame());
        this.level++;
        frame = loadFirstFrame(newFrame());
        pushFrame(frame);

        this.decoded = false;
        this.pending = false;

        if (startTerm == null) {
          pending = isAccept(topFrame());
        } else {
          doSeekCeil(startTerm);
          pending = !startTerm.equals(term) && isValid(topFrame()) && isAccept(topFrame());
        }
      }

      @Override
      void decodeMetaData() throws IOException {
        assert term != null;
        if (!decoded) {
          if (meta.bytes != null) {
            bytesReader.reset(meta.bytes, 0, meta.bytes.length);
          }
          postingsReader.decodeTerm(meta.longs, bytesReader, fieldInfo, state);
          decoded = true;
        }
      }

      @Override
      public SeekStatus seekCeil(BytesRef target, boolean useCache) throws IOException {
        decoded = false;
        term = doSeekCeil(target);
        if (term == null) {
          return SeekStatus.END;
        } else {
          return term.equals(target) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
        }
      }

      @Override
      public BytesRef next() throws IOException {
        //if (DEBUG) System.out.println("Enum next()");
        if (pending) {
          pending = false;
          return term;
        }
        decoded = false;
      DFS:
        while (level > 0) {
          Frame frame = newFrame();
          if (loadExpandFrame(topFrame(), frame) != null) {  // has valid target
            pushFrame(frame);
            if (isAccept(frame)) {  // gotcha
              return term;
            }
            continue;  // check next target
          } 
          frame = popFrame();
          while(level > 0) {
            if (loadNextFrame(topFrame(), frame) != null) {  // has valid sibling 
              pushFrame(frame);
              if (isAccept(frame)) {  // gotcha
                return term;
              }
              continue DFS;   // check next target 
            }
            frame = popFrame();
          }
          return null;
        }
        return null;
      }

      private BytesRef doSeekCeil(BytesRef target) throws IOException {
        //if (DEBUG) System.out.println("Enum doSeekCeil()");
        Frame frame= null;
        int label, upto = 0, limit = target.length;
        while (upto < limit) {  // to target prefix, or ceil label (rewind prefix)
          frame = newFrame();
          label = target.bytes[upto] & 0xff;
          frame = loadCeilFrame(label, topFrame(), frame);
          if (frame == null || frame.fstArc.label != label) {
            break;
          }
          assert isValid(frame);  // target must be fetched from automaton
          pushFrame(frame);
          upto++;
        }
        if (upto == limit) {  // got target
          return term;
        }
        if (frame != null) {  // got larger term('s prefix)
          pushFrame(frame);
          return isAccept(frame) ? term : next();
        }
        while (level > 0) {  // got target's prefix, advance to larger term
          frame = popFrame();
          while (level > 0 && !canRewind(frame)) {
            frame = popFrame();
          }
          if (loadNextFrame(topFrame(), frame) != null) {
            pushFrame(frame);
            return isAccept(frame) ? term : next();
          }
        }
        return null;
      }

      // nocommit: might be great if we can set flag BIT_LAST_ARC
      // nocommit: actually we can use first arc as candidate...
      // it always has NO_OUTPUT as output, and BIT_LAST_ARC set.
      // but we'll have problem if later FST supports output sharing
      // on first arc!

      /** Virtual frame, never pop */
      Frame loadVirtualFrame(Frame frame) throws IOException {
        frame.fstArc.output = fstOutputs.getNoOutput();
        frame.fstArc.nextFinalOutput = fstOutputs.getNoOutput();
        frame.fsaState = -1;
        return frame;
      }

      /** Load frame for start arc(node) on fst */
      Frame loadFirstFrame(Frame frame) throws IOException {
        frame.fstArc = fst.getFirstArc(frame.fstArc);
        frame.fsaState = fsa.getInitialState();
        return frame;
      }

      // nocommit: expected to use readFirstTargetArc here?

      /** Load frame for target arc(node) on fst */
      Frame loadExpandFrame(Frame top, Frame frame) throws IOException {
        if (!canGrow(top)) {
          return null;
        }
        frame.fstArc = fst.readFirstRealTargetArc(top.fstArc.target, frame.fstArc, fstReader);
        frame.fsaState = fsa.step(top.fsaState, frame.fstArc.label);
        //if (DEBUG) System.out.println(" loadExpand frame="+frame);
        if (frame.fsaState == -1) {
          return loadNextFrame(top, frame);
        }
        return frame;
      }

      // nocommit: actually, here we're looking for a valid state for fsa, 
      //           so if numArcs is large in fst, we should try a reverse lookup?
      //           but we don have methods like advance(label) in fst, even 
      //           binary search hurts. 
      
      /** Load frame for sibling arc(node) on fst */
      Frame loadNextFrame(Frame top, Frame frame) throws IOException {
        if (!canRewind(frame)) {
          return null;
        }
        while (!frame.fstArc.isLast()) {
          frame.fstArc = fst.readNextRealArc(frame.fstArc, fstReader);
          frame.fsaState = fsa.step(top.fsaState, frame.fstArc.label);
          if (frame.fsaState != -1) {
            break;
          }
        }
        //if (DEBUG) System.out.println(" loadNext frame="+frame);
        if (frame.fsaState == -1) {
          return null;
        }
        return frame;
      }

      /** Load frame for target arc(node) on fst, so that 
       *  arc.label >= label and !fsa.reject(arc.label) */
      Frame loadCeilFrame(int label, Frame top, Frame frame) throws IOException {
        FST.Arc<TempTermOutputs.TempMetaData> arc = frame.fstArc;
        arc = Util.readCeilArc(label, fst, top.fstArc, arc, fstReader);
        if (arc == null) {
          return null;
        }
        frame.fsaState = fsa.step(top.fsaState, arc.label);
        //if (DEBUG) System.out.println(" loadCeil frame="+frame);
        if (frame.fsaState == -1) {
          return loadNextFrame(top, frame);
        }
        return frame;
      }

      boolean isAccept(Frame frame) {  // reach a term both fst&fsa accepts
        return fsa.isAccept(frame.fsaState) && frame.fstArc.isFinal();
      }
      boolean isValid(Frame frame) {   // reach a prefix both fst&fsa won't reject
        return /*frame != null &&*/ frame.fsaState != -1;
      }
      boolean canGrow(Frame frame) {   // can walk forward on both fst&fsa
        return frame.fsaState != -1 && FST.targetHasArcs(frame.fstArc);
      }
      boolean canRewind(Frame frame) { // can jump to sibling
        return !frame.fstArc.isLast();
      }

      void pushFrame(Frame frame) throws IOException {
        final FST.Arc<TempTermOutputs.TempMetaData> arc = frame.fstArc;
        arc.output = fstOutputs.add(topFrame().fstArc.output, arc.output);
        if (arc.isFinal()) {
          arc.nextFinalOutput = fstOutputs.add(arc.output, arc.nextFinalOutput);
          meta = arc.nextFinalOutput;
        } else {
          meta = arc.output;
        }
        term = grow(arc.label);
        state.docFreq = meta.docFreq;
        state.totalTermFreq = meta.totalTermFreq;
        level++;
        //if (DEBUG) System.out.println("  term=" + term + " level=" + level);
      }

      Frame popFrame() throws IOException {
        final Frame pop = stack[level--], top = topFrame();
        if (top.fstArc.isFinal()) {
          meta = top.fstArc.nextFinalOutput;
        } else {
          meta = top.fstArc.output;
        }
        term = shrink();
        //if (DEBUG) System.out.println("  term=" + term + " level=" + level);
        return pop;
      }

      Frame newFrame() throws IOException {
        if (level+1 == stack.length) {
          final Frame[] next = new Frame[ArrayUtil.oversize(level+2, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          System.arraycopy(stack, 0, next, 0, stack.length);
          for (int i = stack.length; i < next.length; i++) {
            next[i] = new Frame();
          }
          stack = next;
        }
        return stack[level+1];
      }

      Frame topFrame() throws IOException {
        return stack[level];
      }

      BytesRef grow(int label) {
        if (term == null) {
          term = new BytesRef(new byte[16], 0, 0);
        } else {
          if (term.length == term.bytes.length) {
            term.grow(term.length+1);
          }
          term.bytes[term.length++] = (byte)label;
        }
        return term;
      }

      BytesRef shrink() {
        if (term.length == 0) {
          term = null;
        } else {
          term.length--;
        }
        return term;
      }
    }
  }

  static<T> void walk(FST<T> fst) throws IOException {
    final ArrayList<FST.Arc<T>> queue = new ArrayList<FST.Arc<T>>();
    final BitSet seen = new BitSet();
    final FST.BytesReader reader = fst.getBytesReader();
    final FST.Arc<T> startArc = fst.getFirstArc(new FST.Arc<T>());
    queue.add(startArc);
    while (!queue.isEmpty()) {
      final FST.Arc<T> arc = queue.remove(0);
      final long node = arc.target;
      //System.out.println(arc);
      if (FST.targetHasArcs(arc) && !seen.get((int) node)) {
        seen.set((int) node);
        fst.readFirstRealTargetArc(node, arc, reader);
        while (true) {
          queue.add(new FST.Arc<T>().copyFrom(arc));
          if (arc.isLast()) {
            break;
          } else {
            fst.readNextRealArc(arc, reader);
          }
        }
      }
    }
  }
}
