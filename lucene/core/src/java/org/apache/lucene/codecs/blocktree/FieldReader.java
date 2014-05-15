package org.apache.lucene.codecs.blocktree;

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
import java.io.PrintStream;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RunAutomaton;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

/** BlockTree's implementation of {@link Terms}. */
// public for CheckIndex:
public final class FieldReader extends Terms {
  final long numTerms;
  final FieldInfo fieldInfo;
  final long sumTotalTermFreq;
  final long sumDocFreq;
  final int docCount;
  final long indexStartFP;
  final long rootBlockFP;
  final BytesRef rootCode;
  final BytesRef minTerm;
  final BytesRef maxTerm;
  final int longsSize;
  final BlockTreeTermsReader parent;

  final FST<BytesRef> index;
  //private boolean DEBUG;

  FieldReader(BlockTreeTermsReader parent, FieldInfo fieldInfo, long numTerms, BytesRef rootCode, long sumTotalTermFreq, long sumDocFreq, int docCount,
              long indexStartFP, int longsSize, IndexInput indexIn, BytesRef minTerm, BytesRef maxTerm) throws IOException {
    assert numTerms > 0;
    this.fieldInfo = fieldInfo;
    //DEBUG = BlockTreeTermsReader.DEBUG && fieldInfo.name.equals("id");
    this.parent = parent;
    this.numTerms = numTerms;
    this.sumTotalTermFreq = sumTotalTermFreq; 
    this.sumDocFreq = sumDocFreq; 
    this.docCount = docCount;
    this.indexStartFP = indexStartFP;
    this.rootCode = rootCode;
    this.longsSize = longsSize;
    this.minTerm = minTerm;
    this.maxTerm = maxTerm;
    // if (DEBUG) {
    //   System.out.println("BTTR: seg=" + segment + " field=" + fieldInfo.name + " rootBlockCode=" + rootCode + " divisor=" + indexDivisor);
    // }

    rootBlockFP = (new ByteArrayDataInput(rootCode.bytes, rootCode.offset, rootCode.length)).readVLong() >>> BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS;

    if (indexIn != null) {
      final IndexInput clone = indexIn.clone();
      //System.out.println("start=" + indexStartFP + " field=" + fieldInfo.name);
      clone.seek(indexStartFP);
      index = new FST<>(clone, ByteSequenceOutputs.getSingleton());
        
      /*
        if (false) {
        final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
        Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
        Util.toDot(index, w, false, false);
        System.out.println("FST INDEX: SAVED to " + dotFileName);
        w.close();
        }
      */
    } else {
      index = null;
    }
  }

  @Override
  public BytesRef getMin() throws IOException {
    if (minTerm == null) {
      // Older index that didn't store min/maxTerm
      return super.getMin();
    } else {
      return minTerm;
    }
  }

  @Override
  public BytesRef getMax() throws IOException {
    if (maxTerm == null) {
      // Older index that didn't store min/maxTerm
      return super.getMax();
    } else {
      return maxTerm;
    }
  }

  /** For debugging -- used by CheckIndex too*/
  // TODO: maybe push this into Terms?
  public Stats computeStats() throws IOException {
    return new SegmentTermsEnum().computeBlockStats();
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
    return fieldInfo.hasPayloads();
  }

  @Override
  public TermsEnum iterator(TermsEnum reuse) throws IOException {
    return new SegmentTermsEnum();
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
  public long getSumDocFreq() {
    return sumDocFreq;
  }

  @Override
  public int getDocCount() {
    return docCount;
  }

  @Override
  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
      throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
    }
    return new IntersectEnum(this, compiled, startTerm);
  }
    
  /** Returns approximate RAM bytes used */
  public long ramBytesUsed() {
    return ((index!=null)? index.sizeInBytes() : 0);
  }
    
  // Iterates through terms in this field
  final class SegmentTermsEnum extends TermsEnum {
    private IndexInput in;

    private Frame[] stack;
    private final Frame staticFrame;
    private Frame currentFrame;
    private boolean termExists;

    // nocommit make this public "for casting" and add a getVersion method?

    private int targetBeforeCurrentLength;

    private final ByteArrayDataInput scratchReader = new ByteArrayDataInput();

    // What prefix of the current term was present in the index:
    private int validIndexPrefix;

    // assert only:
    private boolean eof;

    final BytesRef term = new BytesRef();
    private final FST.BytesReader fstReader;

    @SuppressWarnings({"rawtypes","unchecked"}) private FST.Arc<BytesRef>[] arcs =
    new FST.Arc[1];

    public SegmentTermsEnum() throws IOException {
      //if (DEBUG) System.out.println("BTTR.init seg=" + segment);
      stack = new Frame[0];
        
      // Used to hold seek by TermState, or cached seek
      staticFrame = new Frame(-1);

      if (index == null) {
        fstReader = null;
      } else {
        fstReader = index.getBytesReader();
      }

      // Init w/ root block; don't use index since it may
      // not (and need not) have been loaded
      for(int arcIdx=0;arcIdx<arcs.length;arcIdx++) {
        arcs[arcIdx] = new FST.Arc<>();
      }

      currentFrame = staticFrame;
      final FST.Arc<BytesRef> arc;
      if (index != null) {
        arc = index.getFirstArc(arcs[0]);
        // Empty string prefix must have an output in the index!
        assert arc.isFinal();
      } else {
        arc = null;
      }
      currentFrame = staticFrame;
      //currentFrame = pushFrame(arc, rootCode, 0);
      //currentFrame.loadBlock();
      validIndexPrefix = 0;
      // if (DEBUG) {
      //   System.out.println("init frame state " + currentFrame.ord);
      //   printSeekState();
      // }

      //System.out.println();
      // computeBlockStats().print(System.out);
    }
      
    // Not private to avoid synthetic access$NNN methods
    void initIndexInput() {
      if (this.in == null) {
        this.in = parent.in.clone();
      }
    }

    /** Runs next() through the entire terms dict,
     *  computing aggregate statistics. */
    public Stats computeBlockStats() throws IOException {

      Stats stats = new Stats(parent.segment, fieldInfo.name);
      if (index != null) {
        stats.indexNodeCount = index.getNodeCount();
        stats.indexArcCount = index.getArcCount();
        stats.indexNumBytes = index.sizeInBytes();
      }
        
      currentFrame = staticFrame;
      FST.Arc<BytesRef> arc;
      if (index != null) {
        arc = index.getFirstArc(arcs[0]);
        // Empty string prefix must have an output in the index!
        assert arc.isFinal();
      } else {
        arc = null;
      }

      // Empty string prefix must have an output in the
      // index!
      currentFrame = pushFrame(arc, rootCode, 0);
      currentFrame.fpOrig = currentFrame.fp;
      currentFrame.loadBlock();
      validIndexPrefix = 0;

      stats.startBlock(currentFrame, !currentFrame.isLastInFloor);

      allTerms:
      while (true) {

        // Pop finished blocks
        while (currentFrame.nextEnt == currentFrame.entCount) {
          stats.endBlock(currentFrame);
          if (!currentFrame.isLastInFloor) {
            currentFrame.loadNextFloorBlock();
            stats.startBlock(currentFrame, true);
          } else {
            if (currentFrame.ord == 0) {
              break allTerms;
            }
            final long lastFP = currentFrame.fpOrig;
            currentFrame = stack[currentFrame.ord-1];
            assert lastFP == currentFrame.lastSubFP;
            // if (DEBUG) {
            //   System.out.println("  reset validIndexPrefix=" + validIndexPrefix);
            // }
          }
        }

        while(true) {
          if (currentFrame.next()) {
            // Push to new block:
            currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length);
            currentFrame.fpOrig = currentFrame.fp;
            // This is a "next" frame -- even if it's
            // floor'd we must pretend it isn't so we don't
            // try to scan to the right floor frame:
            currentFrame.isFloor = false;
            //currentFrame.hasTerms = true;
            currentFrame.loadBlock();
            stats.startBlock(currentFrame, !currentFrame.isLastInFloor);
          } else {
            stats.term(term);
            break;
          }
        }
      }

      stats.finish();

      // Put root frame back:
      currentFrame = staticFrame;
      if (index != null) {
        arc = index.getFirstArc(arcs[0]);
        // Empty string prefix must have an output in the index!
        assert arc.isFinal();
      } else {
        arc = null;
      }
      currentFrame = pushFrame(arc, rootCode, 0);
      currentFrame.rewind();
      currentFrame.loadBlock();
      validIndexPrefix = 0;
      term.length = 0;

      return stats;
    }

    private Frame getFrame(int ord) throws IOException {
      if (ord >= stack.length) {
        final Frame[] next = new Frame[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(stack, 0, next, 0, stack.length);
        for(int stackOrd=stack.length;stackOrd<next.length;stackOrd++) {
          next[stackOrd] = new Frame(stackOrd);
        }
        stack = next;
      }
      assert stack[ord].ord == ord;
      return stack[ord];
    }

    private FST.Arc<BytesRef> getArc(int ord) {
      if (ord >= arcs.length) {
        @SuppressWarnings({"rawtypes","unchecked"}) final FST.Arc<BytesRef>[] next =
        new FST.Arc[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(arcs, 0, next, 0, arcs.length);
        for(int arcOrd=arcs.length;arcOrd<next.length;arcOrd++) {
          next[arcOrd] = new FST.Arc<>();
        }
        arcs = next;
      }
      return arcs[ord];
    }

    // Pushes a frame we seek'd to
    Frame pushFrame(FST.Arc<BytesRef> arc, BytesRef frameData, int length) throws IOException {
      scratchReader.reset(frameData.bytes, frameData.offset, frameData.length);
      final long code = scratchReader.readVLong();
      final long fpSeek = code >>> BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS;
      final Frame f = getFrame(1+currentFrame.ord);
      f.hasTerms = (code & BlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS) != 0;
      f.hasTermsOrig = f.hasTerms;
      f.isFloor = (code & BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR) != 0;
      if (f.isFloor) {
        f.setFloorData(scratchReader, frameData);
      }
      pushFrame(arc, fpSeek, length);

      return f;
    }

    // Pushes next'd frame or seek'd frame; we later
    // lazy-load the frame only when needed
    Frame pushFrame(FST.Arc<BytesRef> arc, long fp, int length) throws IOException {
      final Frame f = getFrame(1+currentFrame.ord);
      f.arc = arc;
      if (f.fpOrig == fp && f.nextEnt != -1) {
        //if (DEBUG) System.out.println("      push reused frame ord=" + f.ord + " fp=" + f.fp + " isFloor?=" + f.isFloor + " hasTerms=" + f.hasTerms + " pref=" + term + " nextEnt=" + f.nextEnt + " targetBeforeCurrentLength=" + targetBeforeCurrentLength + " term.length=" + term.length + " vs prefix=" + f.prefix);
        if (f.prefix > targetBeforeCurrentLength) {
          f.rewind();
        } else {
          // if (DEBUG) {
          //   System.out.println("        skip rewind!");
          // }
        }
        assert length == f.prefix;
      } else {
        f.nextEnt = -1;
        f.prefix = length;
        f.state.termBlockOrd = 0;
        f.fpOrig = f.fp = fp;
        f.lastSubFP = -1;
        // if (DEBUG) {
        //   final int sav = term.length;
        //   term.length = length;
        //   System.out.println("      push new frame ord=" + f.ord + " fp=" + f.fp + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " pref=" + brToString(term));
        //   term.length = sav;
        // }
      }

      return f;
    }

    // asserts only
    private boolean clearEOF() {
      eof = false;
      return true;
    }

    // asserts only
    private boolean setEOF() {
      eof = true;
      return true;
    }

    // nocommit we need a seekExact(BytesRef target, long minVersion) API?

    @Override
    public boolean seekExact(final BytesRef target) throws IOException {

      if (index == null) {
        throw new IllegalStateException("terms index was not loaded");
      }

      if (term.bytes.length <= target.length) {
        term.bytes = ArrayUtil.grow(term.bytes, 1+target.length);
      }

      assert clearEOF();

      // if (DEBUG) {
      //   System.out.println("\nBTTR.seekExact seg=" + segment + " target=" + fieldInfo.name + ":" + brToString(target) + " current=" + brToString(term) + " (exists?=" + termExists + ") validIndexPrefix=" + validIndexPrefix);
      //   printSeekState();
      // }

      FST.Arc<BytesRef> arc;
      int targetUpto;
      BytesRef output;

      targetBeforeCurrentLength = currentFrame.ord;

      if (currentFrame != staticFrame) {

        // We are already seek'd; find the common
        // prefix of new seek term vs current term and
        // re-use the corresponding seek state.  For
        // example, if app first seeks to foobar, then
        // seeks to foobaz, we can re-use the seek state
        // for the first 5 bytes.

        // if (DEBUG) {
        //   System.out.println("  re-use current seek state validIndexPrefix=" + validIndexPrefix);
        // }

        arc = arcs[0];
        assert arc.isFinal();
        output = arc.output;
        targetUpto = 0;
          
        Frame lastFrame = stack[0];
        assert validIndexPrefix <= term.length;

        final int targetLimit = Math.min(target.length, validIndexPrefix);

        int cmp = 0;

        // TODO: reverse vLong byte order for better FST
        // prefix output sharing

        // First compare up to valid seek frames:
        while (targetUpto < targetLimit) {
          cmp = (term.bytes[targetUpto]&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
          // if (DEBUG) {
          //   System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")"   + " arc.output=" + arc.output + " output=" + output);
          // }
          if (cmp != 0) {
            break;
          }
          arc = arcs[1+targetUpto];
          //if (arc.label != (target.bytes[target.offset + targetUpto] & 0xFF)) {
          //System.out.println("FAIL: arc.label=" + (char) arc.label + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF));
          //}
          assert arc.label == (target.bytes[target.offset + targetUpto] & 0xFF): "arc.label=" + (char) arc.label + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF);
          if (arc.output != parent.NO_OUTPUT) {
            output = parent.fstOutputs.add(output, arc.output);
          }
          if (arc.isFinal()) {
            lastFrame = stack[1+lastFrame.ord];
          }
          targetUpto++;
        }

        if (cmp == 0) {
          final int targetUptoMid = targetUpto;

          // Second compare the rest of the term, but
          // don't save arc/output/frame; we only do this
          // to find out if the target term is before,
          // equal or after the current term
          final int targetLimit2 = Math.min(target.length, term.length);
          while (targetUpto < targetLimit2) {
            cmp = (term.bytes[targetUpto]&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
            // if (DEBUG) {
            //   System.out.println("    cycle2 targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")");
            // }
            if (cmp != 0) {
              break;
            }
            targetUpto++;
          }

          if (cmp == 0) {
            cmp = term.length - target.length;
          }
          targetUpto = targetUptoMid;
        }

        if (cmp < 0) {
          // Common case: target term is after current
          // term, ie, app is seeking multiple terms
          // in sorted order
          // if (DEBUG) {
          //   System.out.println("  target is after current (shares prefixLen=" + targetUpto + "); frame.ord=" + lastFrame.ord);
          // }
          currentFrame = lastFrame;

        } else if (cmp > 0) {
          // Uncommon case: target term
          // is before current term; this means we can
          // keep the currentFrame but we must rewind it
          // (so we scan from the start)
          targetBeforeCurrentLength = 0;
          // if (DEBUG) {
          //   System.out.println("  target is before current (shares prefixLen=" + targetUpto + "); rewind frame ord=" + lastFrame.ord);
          // }
          currentFrame = lastFrame;
          currentFrame.rewind();
        } else {
          // Target is exactly the same as current term
          assert term.length == target.length;
          if (termExists) {
            // if (DEBUG) {
            //   System.out.println("  target is same as current; return true");
            // }
            return true;
          } else {
            // if (DEBUG) {
            //   System.out.println("  target is same as current but term doesn't exist");
            // }
          }
          //validIndexPrefix = currentFrame.depth;
          //term.length = target.length;
          //return termExists;
        }

      } else {

        targetBeforeCurrentLength = -1;
        arc = index.getFirstArc(arcs[0]);

        // Empty string prefix must have an output (block) in the index!
        assert arc.isFinal();
        assert arc.output != null;

        // if (DEBUG) {
        //   System.out.println("    no seek state; push root frame");
        // }

        output = arc.output;

        currentFrame = staticFrame;

        //term.length = 0;
        targetUpto = 0;
        currentFrame = pushFrame(arc, parent.fstOutputs.add(output, arc.nextFinalOutput), 0);
      }

      // if (DEBUG) {
      //   System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output + " currentFrame.ord=" + currentFrame.ord + " targetBeforeCurrentLength=" + targetBeforeCurrentLength);
      // }

      while (targetUpto < target.length) {

        final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

        final FST.Arc<BytesRef> nextArc = index.findTargetArc(targetLabel, arc, getArc(1+targetUpto), fstReader);

        if (nextArc == null) {

          // Index is exhausted
          // if (DEBUG) {
          //   System.out.println("    index: index exhausted label=" + ((char) targetLabel) + " " + toHex(targetLabel));
          // }
            
          validIndexPrefix = currentFrame.prefix;
          //validIndexPrefix = targetUpto;

          currentFrame.scanToFloorFrame(target);

          if (!currentFrame.hasTerms) {
            termExists = false;
            term.bytes[targetUpto] = (byte) targetLabel;
            term.length = 1+targetUpto;
            // if (DEBUG) {
            //   System.out.println("  FAST NOT_FOUND term=" + brToString(term));
            // }
            return false;
          }

          currentFrame.loadBlock();

          final SeekStatus result = currentFrame.scanToTerm(target, true);            
          if (result == SeekStatus.FOUND) {
            // if (DEBUG) {
            //   System.out.println("  return FOUND term=" + term.utf8ToString() + " " + term);
            // }
            return true;
          } else {
            // if (DEBUG) {
            //   System.out.println("  got " + result + "; return NOT_FOUND term=" + brToString(term));
            // }
            return false;
          }
        } else {
          // Follow this arc
          arc = nextArc;
          term.bytes[targetUpto] = (byte) targetLabel;
          // Aggregate output as we go:
          assert arc.output != null;
          if (arc.output != parent.NO_OUTPUT) {
            output = parent.fstOutputs.add(output, arc.output);
          }

          // if (DEBUG) {
          //   System.out.println("    index: follow label=" + toHex(target.bytes[target.offset + targetUpto]&0xff) + " arc.output=" + arc.output + " arc.nfo=" + arc.nextFinalOutput);
          // }
          targetUpto++;

          if (arc.isFinal()) {
            //if (DEBUG) System.out.println("    arc is final!");
            currentFrame = pushFrame(arc, parent.fstOutputs.add(output, arc.nextFinalOutput), targetUpto);
            //if (DEBUG) System.out.println("    curFrame.ord=" + currentFrame.ord + " hasTerms=" + currentFrame.hasTerms);
          }
        }
      }

      //validIndexPrefix = targetUpto;
      validIndexPrefix = currentFrame.prefix;

      currentFrame.scanToFloorFrame(target);

      // Target term is entirely contained in the index:
      if (!currentFrame.hasTerms) {
        termExists = false;
        term.length = targetUpto;
        // if (DEBUG) {
        //   System.out.println("  FAST NOT_FOUND term=" + brToString(term));
        // }
        return false;
      }

      currentFrame.loadBlock();

      final SeekStatus result = currentFrame.scanToTerm(target, true);            
      if (result == SeekStatus.FOUND) {
        // if (DEBUG) {
        //   System.out.println("  return FOUND term=" + term.utf8ToString() + " " + term);
        // }
        return true;
      } else {
        // if (DEBUG) {
        //   System.out.println("  got result " + result + "; return NOT_FOUND term=" + term.utf8ToString());
        // }

        return false;
      }
    }

    @Override
    public SeekStatus seekCeil(final BytesRef target) throws IOException {
      if (index == null) {
        throw new IllegalStateException("terms index was not loaded");
      }
   
      if (term.bytes.length <= target.length) {
        term.bytes = ArrayUtil.grow(term.bytes, 1+target.length);
      }

      assert clearEOF();

      //if (DEBUG) {
      //System.out.println("\nBTTR.seekCeil seg=" + segment + " target=" + fieldInfo.name + ":" + target.utf8ToString() + " " + target + " current=" + brToString(term) + " (exists?=" + termExists + ") validIndexPrefix=  " + validIndexPrefix);
      //printSeekState();
      //}

      FST.Arc<BytesRef> arc;
      int targetUpto;
      BytesRef output;

      targetBeforeCurrentLength = currentFrame.ord;

      if (currentFrame != staticFrame) {

        // We are already seek'd; find the common
        // prefix of new seek term vs current term and
        // re-use the corresponding seek state.  For
        // example, if app first seeks to foobar, then
        // seeks to foobaz, we can re-use the seek state
        // for the first 5 bytes.

        //if (DEBUG) {
        //System.out.println("  re-use current seek state validIndexPrefix=" + validIndexPrefix);
        //}

        arc = arcs[0];
        assert arc.isFinal();
        output = arc.output;
        targetUpto = 0;
          
        Frame lastFrame = stack[0];
        assert validIndexPrefix <= term.length;

        final int targetLimit = Math.min(target.length, validIndexPrefix);

        int cmp = 0;

        // TOOD: we should write our vLong backwards (MSB
        // first) to get better sharing from the FST

        // First compare up to valid seek frames:
        while (targetUpto < targetLimit) {
          cmp = (term.bytes[targetUpto]&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
          //if (DEBUG) {
          //System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")"   + " arc.output=" + arc.output + " output=" + output);
          //}
          if (cmp != 0) {
            break;
          }
          arc = arcs[1+targetUpto];
          assert arc.label == (target.bytes[target.offset + targetUpto] & 0xFF): "arc.label=" + (char) arc.label + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF);
          // TOOD: we could save the outputs in local
          // byte[][] instead of making new objs ever
          // seek; but, often the FST doesn't have any
          // shared bytes (but this could change if we
          // reverse vLong byte order)
          if (arc.output != parent.NO_OUTPUT) {
            output = parent.fstOutputs.add(output, arc.output);
          }
          if (arc.isFinal()) {
            lastFrame = stack[1+lastFrame.ord];
          }
          targetUpto++;
        }


        if (cmp == 0) {
          final int targetUptoMid = targetUpto;
          // Second compare the rest of the term, but
          // don't save arc/output/frame:
          final int targetLimit2 = Math.min(target.length, term.length);
          while (targetUpto < targetLimit2) {
            cmp = (term.bytes[targetUpto]&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
            //if (DEBUG) {
            //System.out.println("    cycle2 targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")");
            //}
            if (cmp != 0) {
              break;
            }
            targetUpto++;
          }

          if (cmp == 0) {
            cmp = term.length - target.length;
          }
          targetUpto = targetUptoMid;
        }

        if (cmp < 0) {
          // Common case: target term is after current
          // term, ie, app is seeking multiple terms
          // in sorted order
          //if (DEBUG) {
          //System.out.println("  target is after current (shares prefixLen=" + targetUpto + "); clear frame.scanned ord=" + lastFrame.ord);
          //}
          currentFrame = lastFrame;

        } else if (cmp > 0) {
          // Uncommon case: target term
          // is before current term; this means we can
          // keep the currentFrame but we must rewind it
          // (so we scan from the start)
          targetBeforeCurrentLength = 0;
          //if (DEBUG) {
          //System.out.println("  target is before current (shares prefixLen=" + targetUpto + "); rewind frame ord=" + lastFrame.ord);
          //}
          currentFrame = lastFrame;
          currentFrame.rewind();
        } else {
          // Target is exactly the same as current term
          assert term.length == target.length;
          if (termExists) {
            //if (DEBUG) {
            //System.out.println("  target is same as current; return FOUND");
            //}
            return SeekStatus.FOUND;
          } else {
            //if (DEBUG) {
            //System.out.println("  target is same as current but term doesn't exist");
            //}
          }
        }

      } else {

        targetBeforeCurrentLength = -1;
        arc = index.getFirstArc(arcs[0]);

        // Empty string prefix must have an output (block) in the index!
        assert arc.isFinal();
        assert arc.output != null;

        //if (DEBUG) {
        //System.out.println("    no seek state; push root frame");
        //}

        output = arc.output;

        currentFrame = staticFrame;

        //term.length = 0;
        targetUpto = 0;
        currentFrame = pushFrame(arc, parent.fstOutputs.add(output, arc.nextFinalOutput), 0);
      }

      //if (DEBUG) {
      //System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output + " currentFrame.ord+1=" + currentFrame.ord + " targetBeforeCurrentLength=" + targetBeforeCurrentLength);
      //}

      while (targetUpto < target.length) {

        final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

        final FST.Arc<BytesRef> nextArc = index.findTargetArc(targetLabel, arc, getArc(1+targetUpto), fstReader);

        if (nextArc == null) {

          // Index is exhausted
          // if (DEBUG) {
          //   System.out.println("    index: index exhausted label=" + ((char) targetLabel) + " " + toHex(targetLabel));
          // }
            
          validIndexPrefix = currentFrame.prefix;
          //validIndexPrefix = targetUpto;

          currentFrame.scanToFloorFrame(target);

          currentFrame.loadBlock();

          final SeekStatus result = currentFrame.scanToTerm(target, false);
          if (result == SeekStatus.END) {
            term.copyBytes(target);
            termExists = false;

            if (next() != null) {
              //if (DEBUG) {
              //System.out.println("  return NOT_FOUND term=" + brToString(term) + " " + term);
              //}
              return SeekStatus.NOT_FOUND;
            } else {
              //if (DEBUG) {
              //System.out.println("  return END");
              //}
              return SeekStatus.END;
            }
          } else {
            //if (DEBUG) {
            //System.out.println("  return " + result + " term=" + brToString(term) + " " + term);
            //}
            return result;
          }
        } else {
          // Follow this arc
          term.bytes[targetUpto] = (byte) targetLabel;
          arc = nextArc;
          // Aggregate output as we go:
          assert arc.output != null;
          if (arc.output != parent.NO_OUTPUT) {
            output = parent.fstOutputs.add(output, arc.output);
          }

          //if (DEBUG) {
          //System.out.println("    index: follow label=" + toHex(target.bytes[target.offset + targetUpto]&0xff) + " arc.output=" + arc.output + " arc.nfo=" + arc.nextFinalOutput);
          //}
          targetUpto++;

          if (arc.isFinal()) {
            //if (DEBUG) System.out.println("    arc is final!");
            currentFrame = pushFrame(arc, parent.fstOutputs.add(output, arc.nextFinalOutput), targetUpto);
            //if (DEBUG) System.out.println("    curFrame.ord=" + currentFrame.ord + " hasTerms=" + currentFrame.hasTerms);
          }
        }
      }

      //validIndexPrefix = targetUpto;
      validIndexPrefix = currentFrame.prefix;

      currentFrame.scanToFloorFrame(target);

      currentFrame.loadBlock();

      final SeekStatus result = currentFrame.scanToTerm(target, false);

      if (result == SeekStatus.END) {
        term.copyBytes(target);
        termExists = false;
        if (next() != null) {
          //if (DEBUG) {
          //System.out.println("  return NOT_FOUND term=" + term.utf8ToString() + " " + term);
          //}
          return SeekStatus.NOT_FOUND;
        } else {
          //if (DEBUG) {
          //System.out.println("  return END");
          //}
          return SeekStatus.END;
        }
      } else {
        return result;
      }
    }

    @SuppressWarnings("unused")
    private void printSeekState(PrintStream out) throws IOException {
      if (currentFrame == staticFrame) {
        out.println("  no prior seek");
      } else {
        out.println("  prior seek state:");
        int ord = 0;
        boolean isSeekFrame = true;
        while(true) {
          Frame f = getFrame(ord);
          assert f != null;
          final BytesRef prefix = new BytesRef(term.bytes, 0, f.prefix);
          if (f.nextEnt == -1) {
            out.println("    frame " + (isSeekFrame ? "(seek)" : "(next)") + " ord=" + ord + " fp=" + f.fp + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "") + " prefixLen=" + f.prefix + " prefix=" + prefix + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")")) + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " code=" + ((f.fp<<BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) + (f.hasTerms ? BlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) + (f.isFloor ? BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0)) + " isLastInFloor=" + f.isLastInFloor + " mdUpto=" + f.metaDataUpto + " tbOrd=" + f.getTermBlockOrd());
          } else {
            out.println("    frame " + (isSeekFrame ? "(seek, loaded)" : "(next, loaded)") + " ord=" + ord + " fp=" + f.fp + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "") + " prefixLen=" + f.prefix + " prefix=" + prefix + " nextEnt=" + f.nextEnt + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")")) + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " code=" + ((f.fp<<BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) + (f.hasTerms ? BlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) + (f.isFloor ? BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0)) + " lastSubFP=" + f.lastSubFP + " isLastInFloor=" + f.isLastInFloor + " mdUpto=" + f.metaDataUpto + " tbOrd=" + f.getTermBlockOrd());
          }
          if (index != null) {
            assert !isSeekFrame || f.arc != null: "isSeekFrame=" + isSeekFrame + " f.arc=" + f.arc;
            if (f.prefix > 0 && isSeekFrame && f.arc.label != (term.bytes[f.prefix-1]&0xFF)) {
              out.println("      broken seek state: arc.label=" + (char) f.arc.label + " vs term byte=" + (char) (term.bytes[f.prefix-1]&0xFF));
              throw new RuntimeException("seek state is broken");
            }
            BytesRef output = Util.get(index, prefix);
            if (output == null) {
              out.println("      broken seek state: prefix is not final in index");
              throw new RuntimeException("seek state is broken");
            } else if (isSeekFrame && !f.isFloor) {
              final ByteArrayDataInput reader = new ByteArrayDataInput(output.bytes, output.offset, output.length);
              final long codeOrig = reader.readVLong();
              final long code = (f.fp << BlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) | (f.hasTerms ? BlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) | (f.isFloor ? BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0);
              if (codeOrig != code) {
                out.println("      broken seek state: output code=" + codeOrig + " doesn't match frame code=" + code);
                throw new RuntimeException("seek state is broken");
              }
            }
          }
          if (f == currentFrame) {
            break;
          }
          if (f.prefix == validIndexPrefix) {
            isSeekFrame = false;
          }
          ord++;
        }
      }
    }

    /* Decodes only the term bytes of the next term.  If caller then asks for
       metadata, ie docFreq, totalTermFreq or pulls a D/&PEnum, we then (lazily)
       decode all metadata up to the current term. */
    @Override
    public BytesRef next() throws IOException {

      if (in == null) {
        // Fresh TermsEnum; seek to first term:
        final FST.Arc<BytesRef> arc;
        if (index != null) {
          arc = index.getFirstArc(arcs[0]);
          // Empty string prefix must have an output in the index!
          assert arc.isFinal();
        } else {
          arc = null;
        }
        currentFrame = pushFrame(arc, rootCode, 0);
        currentFrame.loadBlock();
      }

      targetBeforeCurrentLength = currentFrame.ord;

      assert !eof;
      //if (DEBUG) {
      //System.out.println("\nBTTR.next seg=" + segment + " term=" + brToString(term) + " termExists?=" + termExists + " field=" + fieldInfo.name + " termBlockOrd=" + currentFrame.state.termBlockOrd + " validIndexPrefix=" + validIndexPrefix);
      //printSeekState();
      //}

      if (currentFrame == staticFrame) {
        // If seek was previously called and the term was
        // cached, or seek(TermState) was called, usually
        // caller is just going to pull a D/&PEnum or get
        // docFreq, etc.  But, if they then call next(),
        // this method catches up all internal state so next()
        // works properly:
        //if (DEBUG) System.out.println("  re-seek to pending term=" + term.utf8ToString() + " " + term);
        final boolean result = seekExact(term);
        assert result;
      }

      // Pop finished blocks
      while (currentFrame.nextEnt == currentFrame.entCount) {
        if (!currentFrame.isLastInFloor) {
          currentFrame.loadNextFloorBlock();
        } else {
          //if (DEBUG) System.out.println("  pop frame");
          if (currentFrame.ord == 0) {
            //if (DEBUG) System.out.println("  return null");
            assert setEOF();
            term.length = 0;
            validIndexPrefix = 0;
            currentFrame.rewind();
            termExists = false;
            return null;
          }
          final long lastFP = currentFrame.fpOrig;
          currentFrame = stack[currentFrame.ord-1];

          if (currentFrame.nextEnt == -1 || currentFrame.lastSubFP != lastFP) {
            // We popped into a frame that's not loaded
            // yet or not scan'd to the right entry
            currentFrame.scanToFloorFrame(term);
            currentFrame.loadBlock();
            currentFrame.scanToSubBlock(lastFP);
          }

          // Note that the seek state (last seek) has been
          // invalidated beyond this depth
          validIndexPrefix = Math.min(validIndexPrefix, currentFrame.prefix);
          //if (DEBUG) {
          //System.out.println("  reset validIndexPrefix=" + validIndexPrefix);
          //}
        }
      }

      while(true) {
        if (currentFrame.next()) {
          // Push to new block:
          //if (DEBUG) System.out.println("  push frame");
          currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length);
          // This is a "next" frame -- even if it's
          // floor'd we must pretend it isn't so we don't
          // try to scan to the right floor frame:
          currentFrame.isFloor = false;
          //currentFrame.hasTerms = true;
          currentFrame.loadBlock();
        } else {
          //if (DEBUG) System.out.println("  return term=" + term.utf8ToString() + " " + term + " currentFrame.ord=" + currentFrame.ord);
          return term;
        }
      }
    }

    @Override
    public BytesRef term() {
      assert !eof;
      return term;
    }

    @Override
    public int docFreq() throws IOException {
      assert !eof;
      //if (DEBUG) System.out.println("BTR.docFreq");
      currentFrame.decodeMetaData();
      //if (DEBUG) System.out.println("  return " + currentFrame.state.docFreq);
      return currentFrame.state.docFreq;
    }

    @Override
    public long totalTermFreq() throws IOException {
      assert !eof;
      currentFrame.decodeMetaData();
      return currentFrame.state.totalTermFreq;
    }

    @Override
    public DocsEnum docs(Bits skipDocs, DocsEnum reuse, int flags) throws IOException {
      assert !eof;
      //if (DEBUG) {
      //System.out.println("BTTR.docs seg=" + segment);
      //}
      currentFrame.decodeMetaData();
      //if (DEBUG) {
      //System.out.println("  state=" + currentFrame.state);
      //}
      return parent.postingsReader.docs(fieldInfo, currentFrame.state, skipDocs, reuse, flags);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
        // Positions were not indexed:
        return null;
      }

      assert !eof;
      currentFrame.decodeMetaData();
      return parent.postingsReader.docsAndPositions(fieldInfo, currentFrame.state, skipDocs, reuse, flags);
    }

    @Override
    public void seekExact(BytesRef target, TermState otherState) {
      // if (DEBUG) {
      //   System.out.println("BTTR.seekExact termState seg=" + segment + " target=" + target.utf8ToString() + " " + target + " state=" + otherState);
      // }
      assert clearEOF();
      if (target.compareTo(term) != 0 || !termExists) {
        assert otherState != null && otherState instanceof BlockTermState;
        currentFrame = staticFrame;
        currentFrame.state.copyFrom(otherState);
        term.copyBytes(target);
        currentFrame.metaDataUpto = currentFrame.getTermBlockOrd();
        assert currentFrame.metaDataUpto > 0;
        validIndexPrefix = 0;
      } else {
        // if (DEBUG) {
        //   System.out.println("  skip seek: already on target state=" + currentFrame.state);
        // }
      }
    }
      
    @Override
    public TermState termState() throws IOException {
      assert !eof;
      currentFrame.decodeMetaData();
      TermState ts = currentFrame.state.clone();
      //if (DEBUG) System.out.println("BTTR.termState seg=" + segment + " state=" + ts);
      return ts;
    }

    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }

    // Not static -- references term, postingsReader,
    // fieldInfo, in
    final class Frame {
      // Our index in stack[]:
      final int ord;

      boolean hasTerms;
      boolean hasTermsOrig;
      boolean isFloor;

      FST.Arc<BytesRef> arc;

      // File pointer where this block was loaded from
      long fp;
      long fpOrig;
      long fpEnd;

      byte[] suffixBytes = new byte[128];
      final ByteArrayDataInput suffixesReader = new ByteArrayDataInput();

      byte[] statBytes = new byte[64];
      final ByteArrayDataInput statsReader = new ByteArrayDataInput();

      byte[] floorData = new byte[32];
      final ByteArrayDataInput floorDataReader = new ByteArrayDataInput();

      // Length of prefix shared by all terms in this block
      int prefix;

      // Number of entries (term or sub-block) in this block
      int entCount;

      // Which term we will next read, or -1 if the block
      // isn't loaded yet
      int nextEnt;

      // True if this block is either not a floor block,
      // or, it's the last sub-block of a floor block
      boolean isLastInFloor;

      // True if all entries are terms
      boolean isLeafBlock;

      long lastSubFP;

      int nextFloorLabel;
      int numFollowFloorBlocks;

      // Next term to decode metaData; we decode metaData
      // lazily so that scanning to find the matching term is
      // fast and only if you find a match and app wants the
      // stats or docs/positions enums, will we decode the
      // metaData
      int metaDataUpto;

      final BlockTermState state;

      // metadata buffer, holding monotonic values
      public long[] longs;
      // metadata buffer, holding general values
      public byte[] bytes;
      ByteArrayDataInput bytesReader;

      public Frame(int ord) throws IOException {
        this.ord = ord;
        this.state = parent.postingsReader.newTermState();
        this.state.totalTermFreq = -1;
        this.longs = new long[longsSize];
      }

      public void setFloorData(ByteArrayDataInput in, BytesRef source) {
        final int numBytes = source.length - (in.getPosition() - source.offset);
        if (numBytes > floorData.length) {
          floorData = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        System.arraycopy(source.bytes, source.offset+in.getPosition(), floorData, 0, numBytes);
        floorDataReader.reset(floorData, 0, numBytes);
        numFollowFloorBlocks = floorDataReader.readVInt();
        nextFloorLabel = floorDataReader.readByte() & 0xff;
        //if (DEBUG) {
        //System.out.println("    setFloorData fpOrig=" + fpOrig + " bytes=" + new BytesRef(source.bytes, source.offset + in.getPosition(), numBytes) + " numFollowFloorBlocks=" + numFollowFloorBlocks + " nextFloorLabel=" + toHex(nextFloorLabel));
        //}
      }

      public int getTermBlockOrd() {
        return isLeafBlock ? nextEnt : state.termBlockOrd;
      }

      void loadNextFloorBlock() throws IOException {
        //if (DEBUG) {
        //System.out.println("    loadNextFloorBlock fp=" + fp + " fpEnd=" + fpEnd);
        //}
        assert arc == null || isFloor: "arc=" + arc + " isFloor=" + isFloor;
        fp = fpEnd;
        nextEnt = -1;
        loadBlock();
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
      void loadBlock() throws IOException {

        // Clone the IndexInput lazily, so that consumers
        // that just pull a TermsEnum to
        // seekExact(TermState) don't pay this cost:
        initIndexInput();

        if (nextEnt != -1) {
          // Already loaded
          return;
        }
        //System.out.println("blc=" + blockLoadCount);

        in.seek(fp);
        int code = in.readVInt();
        entCount = code >>> 1;
        assert entCount > 0;
        isLastInFloor = (code & 1) != 0;
        assert arc == null || (isLastInFloor || isFloor);

        // TODO: if suffixes were stored in random-access
        // array structure, then we could do binary search
        // instead of linear scan to find target term; eg
        // we could have simple array of offsets

        // term suffixes:
        code = in.readVInt();
        isLeafBlock = (code & 1) != 0;
        int numBytes = code >>> 1;
        if (suffixBytes.length < numBytes) {
          suffixBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        in.readBytes(suffixBytes, 0, numBytes);
        suffixesReader.reset(suffixBytes, 0, numBytes);

        /*if (DEBUG) {
          if (arc == null) {
          System.out.println("    loadBlock (next) fp=" + fp + " entCount=" + entCount + " prefixLen=" + prefix + " isLastInFloor=" + isLastInFloor + " leaf?=" + isLeafBlock);
          } else {
          System.out.println("    loadBlock (seek) fp=" + fp + " entCount=" + entCount + " prefixLen=" + prefix + " hasTerms?=" + hasTerms + " isFloor?=" + isFloor + " isLastInFloor=" + isLastInFloor + " leaf?=" + isLeafBlock);
          }
          }*/

        // stats
        numBytes = in.readVInt();
        if (statBytes.length < numBytes) {
          statBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        in.readBytes(statBytes, 0, numBytes);
        statsReader.reset(statBytes, 0, numBytes);
        metaDataUpto = 0;

        state.termBlockOrd = 0;
        nextEnt = 0;
        lastSubFP = -1;

        // TODO: we could skip this if !hasTerms; but
        // that's rare so won't help much
        // metadata
        numBytes = in.readVInt();
        if (bytes == null) {
          bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
          bytesReader = new ByteArrayDataInput();
        } else if (bytes.length < numBytes) {
          bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        in.readBytes(bytes, 0, numBytes);
        bytesReader.reset(bytes, 0, numBytes);


        // Sub-blocks of a single floor block are always
        // written one after another -- tail recurse:
        fpEnd = in.getFilePointer();
        // if (DEBUG) {
        //   System.out.println("      fpEnd=" + fpEnd);
        // }
      }

      void rewind() {

        // Force reload:
        fp = fpOrig;
        nextEnt = -1;
        hasTerms = hasTermsOrig;
        if (isFloor) {
          floorDataReader.rewind();
          numFollowFloorBlocks = floorDataReader.readVInt();
          nextFloorLabel = floorDataReader.readByte() & 0xff;
        }

        /*
        //System.out.println("rewind");
        // Keeps the block loaded, but rewinds its state:
        if (nextEnt > 0 || fp != fpOrig) {
        if (DEBUG) {
        System.out.println("      rewind frame ord=" + ord + " fpOrig=" + fpOrig + " fp=" + fp + " hasTerms?=" + hasTerms + " isFloor?=" + isFloor + " nextEnt=" + nextEnt + " prefixLen=" + prefix);
        }
        if (fp != fpOrig) {
        fp = fpOrig;
        nextEnt = -1;
        } else {
        nextEnt = 0;
        }
        hasTerms = hasTermsOrig;
        if (isFloor) {
        floorDataReader.rewind();
        numFollowFloorBlocks = floorDataReader.readVInt();
        nextFloorLabel = floorDataReader.readByte() & 0xff;
        }
        assert suffixBytes != null;
        suffixesReader.rewind();
        assert statBytes != null;
        statsReader.rewind();
        metaDataUpto = 0;
        state.termBlockOrd = 0;
        // TODO: skip this if !hasTerms?  Then postings
        // impl wouldn't have to write useless 0 byte
        postingsReader.resetTermsBlock(fieldInfo, state);
        lastSubFP = -1;
        } else if (DEBUG) {
        System.out.println("      skip rewind fp=" + fp + " fpOrig=" + fpOrig + " nextEnt=" + nextEnt + " ord=" + ord);
        }
        */
      }

      public boolean next() {
        return isLeafBlock ? nextLeaf() : nextNonLeaf();
      }

      // Decodes next entry; returns true if it's a sub-block
      public boolean nextLeaf() {
        //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
        assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
        nextEnt++;
        suffix = suffixesReader.readVInt();
        startBytePos = suffixesReader.getPosition();
        term.length = prefix + suffix;
        if (term.bytes.length < term.length) {
          term.grow(term.length);
        }
        suffixesReader.readBytes(term.bytes, prefix, suffix);
        // A normal term
        termExists = true;
        return false;
      }

      public boolean nextNonLeaf() {
        //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
        assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
        nextEnt++;
        final int code = suffixesReader.readVInt();
        suffix = code >>> 1;
        startBytePos = suffixesReader.getPosition();
        term.length = prefix + suffix;
        if (term.bytes.length < term.length) {
          term.grow(term.length);
        }
        suffixesReader.readBytes(term.bytes, prefix, suffix);
        if ((code & 1) == 0) {
          // A normal term
          termExists = true;
          subCode = 0;
          state.termBlockOrd++;
          return false;
        } else {
          // A sub-block; make sub-FP absolute:
          termExists = false;
          subCode = suffixesReader.readVLong();
          lastSubFP = fp - subCode;
          //if (DEBUG) {
          //System.out.println("    lastSubFP=" + lastSubFP);
          //}
          return true;
        }
      }
        
      // TODO: make this array'd so we can do bin search?
      // likely not worth it?  need to measure how many
      // floor blocks we "typically" get
      public void scanToFloorFrame(BytesRef target) {

        if (!isFloor || target.length <= prefix) {
          // if (DEBUG) {
          //   System.out.println("    scanToFloorFrame skip: isFloor=" + isFloor + " target.length=" + target.length + " vs prefix=" + prefix);
          // }
          return;
        }

        final int targetLabel = target.bytes[target.offset + prefix] & 0xFF;

        // if (DEBUG) {
        //   System.out.println("    scanToFloorFrame fpOrig=" + fpOrig + " targetLabel=" + toHex(targetLabel) + " vs nextFloorLabel=" + toHex(nextFloorLabel) + " numFollowFloorBlocks=" + numFollowFloorBlocks);
        // }

        if (targetLabel < nextFloorLabel) {
          // if (DEBUG) {
          //   System.out.println("      already on correct block");
          // }
          return;
        }

        assert numFollowFloorBlocks != 0;

        long newFP = fpOrig;
        while (true) {
          final long code = floorDataReader.readVLong();
          newFP = fpOrig + (code >>> 1);
          hasTerms = (code & 1) != 0;
          // if (DEBUG) {
          //   System.out.println("      label=" + toHex(nextFloorLabel) + " fp=" + newFP + " hasTerms?=" + hasTerms + " numFollowFloor=" + numFollowFloorBlocks);
          // }
            
          isLastInFloor = numFollowFloorBlocks == 1;
          numFollowFloorBlocks--;

          if (isLastInFloor) {
            nextFloorLabel = 256;
            // if (DEBUG) {
            //   System.out.println("        stop!  last block nextFloorLabel=" + toHex(nextFloorLabel));
            // }
            break;
          } else {
            nextFloorLabel = floorDataReader.readByte() & 0xff;
            if (targetLabel < nextFloorLabel) {
              // if (DEBUG) {
              //   System.out.println("        stop!  nextFloorLabel=" + toHex(nextFloorLabel));
              // }
              break;
            }
          }
        }

        if (newFP != fp) {
          // Force re-load of the block:
          // if (DEBUG) {
          //   System.out.println("      force switch to fp=" + newFP + " oldFP=" + fp);
          // }
          nextEnt = -1;
          fp = newFP;
        } else {
          // if (DEBUG) {
          //   System.out.println("      stay on same fp=" + newFP);
          // }
        }
      }
    
      public void decodeMetaData() throws IOException {

        //if (DEBUG) System.out.println("\nBTTR.decodeMetadata seg=" + segment + " mdUpto=" + metaDataUpto + " vs termBlockOrd=" + state.termBlockOrd);

        // lazily catch up on metadata decode:
        final int limit = getTermBlockOrd();
        boolean absolute = metaDataUpto == 0;
        assert limit > 0;

        // TODO: better API would be "jump straight to term=N"???
        while (metaDataUpto < limit) {

          // TODO: we could make "tiers" of metadata, ie,
          // decode docFreq/totalTF but don't decode postings
          // metadata; this way caller could get
          // docFreq/totalTF w/o paying decode cost for
          // postings

          // TODO: if docFreq were bulk decoded we could
          // just skipN here:

          // stats
          state.docFreq = statsReader.readVInt();
          //if (DEBUG) System.out.println("    dF=" + state.docFreq);
          if (fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY) {
            state.totalTermFreq = state.docFreq + statsReader.readVLong();
            //if (DEBUG) System.out.println("    totTF=" + state.totalTermFreq);
          }
          // metadata 
          for (int i = 0; i < longsSize; i++) {
            longs[i] = bytesReader.readVLong();
          }
          parent.postingsReader.decodeTerm(longs, bytesReader, fieldInfo, state, absolute);

          metaDataUpto++;
          absolute = false;
        }
        state.termBlockOrd = metaDataUpto;
      }

      // Used only by assert
      private boolean prefixMatches(BytesRef target) {
        for(int bytePos=0;bytePos<prefix;bytePos++) {
          if (target.bytes[target.offset + bytePos] != term.bytes[bytePos]) {
            return false;
          }
        }

        return true;
      }

      // Scans to sub-block that has this target fp; only
      // called by next(); NOTE: does not set
      // startBytePos/suffix as a side effect
      public void scanToSubBlock(long subFP) {
        assert !isLeafBlock;
        //if (DEBUG) System.out.println("  scanToSubBlock fp=" + fp + " subFP=" + subFP + " entCount=" + entCount + " lastSubFP=" + lastSubFP);
        //assert nextEnt == 0;
        if (lastSubFP == subFP) {
          //if (DEBUG) System.out.println("    already positioned");
          return;
        }
        assert subFP < fp : "fp=" + fp + " subFP=" + subFP;
        final long targetSubCode = fp - subFP;
        //if (DEBUG) System.out.println("    targetSubCode=" + targetSubCode);
        while(true) {
          assert nextEnt < entCount;
          nextEnt++;
          final int code = suffixesReader.readVInt();
          suffixesReader.skipBytes(isLeafBlock ? code : code >>> 1);
          //if (DEBUG) System.out.println("    " + nextEnt + " (of " + entCount + ") ent isSubBlock=" + ((code&1)==1));
          if ((code & 1) != 0) {
            final long subCode = suffixesReader.readVLong();
            //if (DEBUG) System.out.println("      subCode=" + subCode);
            if (targetSubCode == subCode) {
              //if (DEBUG) System.out.println("        match!");
              lastSubFP = subFP;
              return;
            }
          } else {
            state.termBlockOrd++;
          }
        }
      }

      // NOTE: sets startBytePos/suffix as a side effect
      public SeekStatus scanToTerm(BytesRef target, boolean exactOnly) throws IOException {
        return isLeafBlock ? scanToTermLeaf(target, exactOnly) : scanToTermNonLeaf(target, exactOnly);
      }

      private int startBytePos;
      private int suffix;
      private long subCode;

      // Target's prefix matches this block's prefix; we
      // scan the entries check if the suffix matches.
      public SeekStatus scanToTermLeaf(BytesRef target, boolean exactOnly) throws IOException {

        // if (DEBUG) System.out.println("    scanToTermLeaf: block fp=" + fp + " prefix=" + prefix + " nextEnt=" + nextEnt + " (of " + entCount + ") target=" + brToString(target) + " term=" + brToString(term));

        assert nextEnt != -1;

        termExists = true;
        subCode = 0;

        if (nextEnt == entCount) {
          if (exactOnly) {
            fillTerm();
          }
          return SeekStatus.END;
        }

        assert prefixMatches(target);

        // Loop over each entry (term or sub-block) in this block:
        //nextTerm: while(nextEnt < entCount) {
        nextTerm: while (true) {
          nextEnt++;

          suffix = suffixesReader.readVInt();

          // if (DEBUG) {
          //   BytesRef suffixBytesRef = new BytesRef();
          //   suffixBytesRef.bytes = suffixBytes;
          //   suffixBytesRef.offset = suffixesReader.getPosition();
          //   suffixBytesRef.length = suffix;
          //   System.out.println("      cycle: term " + (nextEnt-1) + " (of " + entCount + ") suffix=" + brToString(suffixBytesRef));
          // }

          final int termLen = prefix + suffix;
          startBytePos = suffixesReader.getPosition();
          suffixesReader.skipBytes(suffix);

          final int targetLimit = target.offset + (target.length < termLen ? target.length : termLen);
          int targetPos = target.offset + prefix;

          // Loop over bytes in the suffix, comparing to
          // the target
          int bytePos = startBytePos;
          while(true) {
            final int cmp;
            final boolean stop;
            if (targetPos < targetLimit) {
              cmp = (suffixBytes[bytePos++]&0xFF) - (target.bytes[targetPos++]&0xFF);
              stop = false;
            } else {
              assert targetPos == targetLimit;
              cmp = termLen - target.length;
              stop = true;
            }

            if (cmp < 0) {
              // Current entry is still before the target;
              // keep scanning

              if (nextEnt == entCount) {
                if (exactOnly) {
                  fillTerm();
                }
                // We are done scanning this block
                break nextTerm;
              } else {
                continue nextTerm;
              }
            } else if (cmp > 0) {

              // Done!  Current entry is after target --
              // return NOT_FOUND:
              fillTerm();

              if (!exactOnly && !termExists) {
                // We are on a sub-block, and caller wants
                // us to position to the next term after
                // the target, so we must recurse into the
                // sub-frame(s):
                currentFrame = pushFrame(null, currentFrame.lastSubFP, termLen);
                currentFrame.loadBlock();
                while (currentFrame.next()) {
                  currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length);
                  currentFrame.loadBlock();
                }
              }
                
              //if (DEBUG) System.out.println("        not found");
              return SeekStatus.NOT_FOUND;
            } else if (stop) {
              // Exact match!

              // This cannot be a sub-block because we
              // would have followed the index to this
              // sub-block from the start:

              assert termExists;
              fillTerm();
              //if (DEBUG) System.out.println("        found!");
              return SeekStatus.FOUND;
            }
          }
        }

        // It is possible (and OK) that terms index pointed us
        // at this block, but, we scanned the entire block and
        // did not find the term to position to.  This happens
        // when the target is after the last term in the block
        // (but, before the next term in the index).  EG
        // target could be foozzz, and terms index pointed us
        // to the foo* block, but the last term in this block
        // was fooz (and, eg, first term in the next block will
        // bee fop).
        //if (DEBUG) System.out.println("      block end");
        if (exactOnly) {
          fillTerm();
        }

        // TODO: not consistent that in the
        // not-exact case we don't next() into the next
        // frame here
        return SeekStatus.END;
      }

      // Target's prefix matches this block's prefix; we
      // scan the entries check if the suffix matches.
      public SeekStatus scanToTermNonLeaf(BytesRef target, boolean exactOnly) throws IOException {

        //if (DEBUG) System.out.println("    scanToTermNonLeaf: block fp=" + fp + " prefix=" + prefix + " nextEnt=" + nextEnt + " (of " + entCount + ") target=" + brToString(target) + " term=" + brToString(term));

        assert nextEnt != -1;

        if (nextEnt == entCount) {
          if (exactOnly) {
            fillTerm();
            termExists = subCode == 0;
          }
          return SeekStatus.END;
        }

        assert prefixMatches(target);

        // Loop over each entry (term or sub-block) in this block:
        //nextTerm: while(nextEnt < entCount) {
        nextTerm: while (true) {
          nextEnt++;

          final int code = suffixesReader.readVInt();
          suffix = code >>> 1;
          // if (DEBUG) {
          //   BytesRef suffixBytesRef = new BytesRef();
          //   suffixBytesRef.bytes = suffixBytes;
          //   suffixBytesRef.offset = suffixesReader.getPosition();
          //   suffixBytesRef.length = suffix;
          //   System.out.println("      cycle: " + ((code&1)==1 ? "sub-block" : "term") + " " + (nextEnt-1) + " (of " + entCount + ") suffix=" + brToString(suffixBytesRef));
          // }

          termExists = (code & 1) == 0;
          final int termLen = prefix + suffix;
          startBytePos = suffixesReader.getPosition();
          suffixesReader.skipBytes(suffix);
          if (termExists) {
            state.termBlockOrd++;
            subCode = 0;
          } else {
            subCode = suffixesReader.readVLong();
            lastSubFP = fp - subCode;
          }

          final int targetLimit = target.offset + (target.length < termLen ? target.length : termLen);
          int targetPos = target.offset + prefix;

          // Loop over bytes in the suffix, comparing to
          // the target
          int bytePos = startBytePos;
          while(true) {
            final int cmp;
            final boolean stop;
            if (targetPos < targetLimit) {
              cmp = (suffixBytes[bytePos++]&0xFF) - (target.bytes[targetPos++]&0xFF);
              stop = false;
            } else {
              assert targetPos == targetLimit;
              cmp = termLen - target.length;
              stop = true;
            }

            if (cmp < 0) {
              // Current entry is still before the target;
              // keep scanning

              if (nextEnt == entCount) {
                if (exactOnly) {
                  fillTerm();
                  //termExists = true;
                }
                // We are done scanning this block
                break nextTerm;
              } else {
                continue nextTerm;
              }
            } else if (cmp > 0) {

              // Done!  Current entry is after target --
              // return NOT_FOUND:
              fillTerm();

              if (!exactOnly && !termExists) {
                // We are on a sub-block, and caller wants
                // us to position to the next term after
                // the target, so we must recurse into the
                // sub-frame(s):
                currentFrame = pushFrame(null, currentFrame.lastSubFP, termLen);
                currentFrame.loadBlock();
                while (currentFrame.next()) {
                  currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length);
                  currentFrame.loadBlock();
                }
              }
                
              //if (DEBUG) System.out.println("        not found");
              return SeekStatus.NOT_FOUND;
            } else if (stop) {
              // Exact match!

              // This cannot be a sub-block because we
              // would have followed the index to this
              // sub-block from the start:

              assert termExists;
              fillTerm();
              //if (DEBUG) System.out.println("        found!");
              return SeekStatus.FOUND;
            }
          }
        }

        // It is possible (and OK) that terms index pointed us
        // at this block, but, we scanned the entire block and
        // did not find the term to position to.  This happens
        // when the target is after the last term in the block
        // (but, before the next term in the index).  EG
        // target could be foozzz, and terms index pointed us
        // to the foo* block, but the last term in this block
        // was fooz (and, eg, first term in the next block will
        // bee fop).
        //if (DEBUG) System.out.println("      block end");
        if (exactOnly) {
          fillTerm();
        }

        // TODO: not consistent that in the
        // not-exact case we don't next() into the next
        // frame here
        return SeekStatus.END;
      }

      private void fillTerm() {
        final int termLength = prefix + suffix;
        term.length = prefix + suffix;
        if (term.bytes.length < termLength) {
          term.grow(termLength);
        }
        System.arraycopy(suffixBytes, startBytePos, term.bytes, prefix, suffix);
      }
    }
  }
}
