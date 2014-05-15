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

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.TermState;
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
import org.apache.lucene.util.fst.FST;

// NOTE: cannot seek!
final class IntersectEnum extends TermsEnum {
  private final IndexInput in;

  private Frame[] stack;
      
  @SuppressWarnings({"rawtypes","unchecked"}) private FST.Arc<BytesRef>[] arcs = new FST.Arc[5];

  private final RunAutomaton runAutomaton;
  private final CompiledAutomaton compiledAutomaton;

  private Frame currentFrame;

  private final BytesRef term = new BytesRef();

  private final FST.BytesReader fstReader;

  private final FieldReader fr;

  // TODO: can we share this with the frame in STE?
  private final class Frame {
    final int ord;
    long fp;
    long fpOrig;
    long fpEnd;
    long lastSubFP;

    // State in automaton
    int state;

    int metaDataUpto;

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

    // Which term we will next read
    int nextEnt;

    // True if this block is either not a floor block,
    // or, it's the last sub-block of a floor block
    boolean isLastInFloor;

    // True if all entries are terms
    boolean isLeafBlock;

    int numFollowFloorBlocks;
    int nextFloorLabel;
        
    Transition[] transitions;
    int curTransitionMax;
    int transitionIndex;

    FST.Arc<BytesRef> arc;

    final BlockTermState termState;
  
    // metadata buffer, holding monotonic values
    public long[] longs;
    // metadata buffer, holding general values
    public byte[] bytes;
    ByteArrayDataInput bytesReader;

    // Cumulative output so far
    BytesRef outputPrefix;

    private int startBytePos;
    private int suffix;

    public Frame(int ord) throws IOException {
      this.ord = ord;
      this.termState = fr.parent.postingsReader.newTermState();
      this.termState.totalTermFreq = -1;
      this.longs = new long[fr.longsSize];
    }

    void loadNextFloorBlock() throws IOException {
      assert numFollowFloorBlocks > 0;
      //if (DEBUG) System.out.println("    loadNextFoorBlock trans=" + transitions[transitionIndex]);

      do {
        fp = fpOrig + (floorDataReader.readVLong() >>> 1);
        numFollowFloorBlocks--;
        // if (DEBUG) System.out.println("    skip floor block2!  nextFloorLabel=" + (char) nextFloorLabel + " vs target=" + (char) transitions[transitionIndex].getMin() + " newFP=" + fp + " numFollowFloorBlocks=" + numFollowFloorBlocks);
        if (numFollowFloorBlocks != 0) {
          nextFloorLabel = floorDataReader.readByte() & 0xff;
        } else {
          nextFloorLabel = 256;
        }
        // if (DEBUG) System.out.println("    nextFloorLabel=" + (char) nextFloorLabel);
      } while (numFollowFloorBlocks != 0 && nextFloorLabel <= transitions[transitionIndex].getMin());

      load(null);
    }

    public void setState(int state) {
      this.state = state;
      transitionIndex = 0;
      transitions = compiledAutomaton.sortedTransitions[state];
      if (transitions.length != 0) {
        curTransitionMax = transitions[0].getMax();
      } else {
        curTransitionMax = -1;
      }
    }

    void load(BytesRef frameIndexData) throws IOException {

      // if (DEBUG) System.out.println("    load fp=" + fp + " fpOrig=" + fpOrig + " frameIndexData=" + frameIndexData + " trans=" + (transitions.length != 0 ? transitions[0] : "n/a" + " state=" + state));

      if (frameIndexData != null && transitions.length != 0) {
        // Floor frame
        if (floorData.length < frameIndexData.length) {
          this.floorData = new byte[ArrayUtil.oversize(frameIndexData.length, 1)];
        }
        System.arraycopy(frameIndexData.bytes, frameIndexData.offset, floorData, 0, frameIndexData.length);
        floorDataReader.reset(floorData, 0, frameIndexData.length);
        // Skip first long -- has redundant fp, hasTerms
        // flag, isFloor flag
        final long code = floorDataReader.readVLong();
        if ((code & BlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR) != 0) {
          numFollowFloorBlocks = floorDataReader.readVInt();
          nextFloorLabel = floorDataReader.readByte() & 0xff;
          // if (DEBUG) System.out.println("    numFollowFloorBlocks=" + numFollowFloorBlocks + " nextFloorLabel=" + nextFloorLabel);

          // If current state is accept, we must process
          // first block in case it has empty suffix:
          if (!runAutomaton.isAccept(state)) {
            // Maybe skip floor blocks:
            while (numFollowFloorBlocks != 0 && nextFloorLabel <= transitions[0].getMin()) {
              fp = fpOrig + (floorDataReader.readVLong() >>> 1);
              numFollowFloorBlocks--;
              // if (DEBUG) System.out.println("    skip floor block!  nextFloorLabel=" + (char) nextFloorLabel + " vs target=" + (char) transitions[0].getMin() + " newFP=" + fp + " numFollowFloorBlocks=" + numFollowFloorBlocks);
              if (numFollowFloorBlocks != 0) {
                nextFloorLabel = floorDataReader.readByte() & 0xff;
              } else {
                nextFloorLabel = 256;
              }
            }
          }
        }
      }

      in.seek(fp);
      int code = in.readVInt();
      entCount = code >>> 1;
      assert entCount > 0;
      isLastInFloor = (code & 1) != 0;

      // term suffixes:
      code = in.readVInt();
      isLeafBlock = (code & 1) != 0;
      int numBytes = code >>> 1;
      // if (DEBUG) System.out.println("      entCount=" + entCount + " lastInFloor?=" + isLastInFloor + " leafBlock?=" + isLeafBlock + " numSuffixBytes=" + numBytes);
      if (suffixBytes.length < numBytes) {
        suffixBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
      }
      in.readBytes(suffixBytes, 0, numBytes);
      suffixesReader.reset(suffixBytes, 0, numBytes);

      // stats
      numBytes = in.readVInt();
      if (statBytes.length < numBytes) {
        statBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
      }
      in.readBytes(statBytes, 0, numBytes);
      statsReader.reset(statBytes, 0, numBytes);
      metaDataUpto = 0;

      termState.termBlockOrd = 0;
      nextEnt = 0;
         
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

      if (!isLastInFloor) {
        // Sub-blocks of a single floor block are always
        // written one after another -- tail recurse:
        fpEnd = in.getFilePointer();
      }
    }

    // TODO: maybe add scanToLabel; should give perf boost

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
      suffixesReader.skipBytes(suffix);
      return false;
    }

    public boolean nextNonLeaf() {
      //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
      assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
      nextEnt++;
      final int code = suffixesReader.readVInt();
      suffix = code >>> 1;
      startBytePos = suffixesReader.getPosition();
      suffixesReader.skipBytes(suffix);
      if ((code & 1) == 0) {
        // A normal term
        termState.termBlockOrd++;
        return false;
      } else {
        // A sub-block; make sub-FP absolute:
        lastSubFP = fp - suffixesReader.readVLong();
        return true;
      }
    }

    public int getTermBlockOrd() {
      return isLeafBlock ? nextEnt : termState.termBlockOrd;
    }

    public void decodeMetaData() throws IOException {

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
        termState.docFreq = statsReader.readVInt();
        //if (DEBUG) System.out.println("    dF=" + state.docFreq);
        if (fr.fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY) {
          termState.totalTermFreq = termState.docFreq + statsReader.readVLong();
          //if (DEBUG) System.out.println("    totTF=" + state.totalTermFreq);
        }
        // metadata 
        for (int i = 0; i < fr.longsSize; i++) {
          longs[i] = bytesReader.readVLong();
        }
        fr.parent.postingsReader.decodeTerm(longs, bytesReader, fr.fieldInfo, termState, absolute);

        metaDataUpto++;
        absolute = false;
      }
      termState.termBlockOrd = metaDataUpto;
    }
  }

  private BytesRef savedStartTerm;
      
  // TODO: in some cases we can filter by length?  eg
  // regexp foo*bar must be at least length 6 bytes
  public IntersectEnum(FieldReader fr, CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    // if (DEBUG) {
    //   System.out.println("\nintEnum.init seg=" + segment + " commonSuffix=" + brToString(compiled.commonSuffixRef));
    // }
    this.fr = fr;
    runAutomaton = compiled.runAutomaton;
    compiledAutomaton = compiled;
    in = fr.parent.in.clone();
    stack = new Frame[5];
    for(int idx=0;idx<stack.length;idx++) {
      stack[idx] = new Frame(idx);
    }
    for(int arcIdx=0;arcIdx<arcs.length;arcIdx++) {
      arcs[arcIdx] = new FST.Arc<>();
    }

    if (fr.index == null) {
      fstReader = null;
    } else {
      fstReader = fr.index.getBytesReader();
    }

    // TODO: if the automaton is "smallish" we really
    // should use the terms index to seek at least to
    // the initial term and likely to subsequent terms
    // (or, maybe just fallback to ATE for such cases).
    // Else the seek cost of loading the frames will be
    // too costly.

    final FST.Arc<BytesRef> arc = fr.index.getFirstArc(arcs[0]);
    // Empty string prefix must have an output in the index!
    assert arc.isFinal();

    // Special pushFrame since it's the first one:
    final Frame f = stack[0];
    f.fp = f.fpOrig = fr.rootBlockFP;
    f.prefix = 0;
    f.setState(runAutomaton.getInitialState());
    f.arc = arc;
    f.outputPrefix = arc.output;
    f.load(fr.rootCode);

    // for assert:
    assert setSavedStartTerm(startTerm);

    currentFrame = f;
    if (startTerm != null) {
      seekToStartTerm(startTerm);
    }
  }

  // only for assert:
  private boolean setSavedStartTerm(BytesRef startTerm) {
    savedStartTerm = startTerm == null ? null : BytesRef.deepCopyOf(startTerm);
    return true;
  }

  @Override
  public TermState termState() throws IOException {
    currentFrame.decodeMetaData();
    return currentFrame.termState.clone();
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

  private Frame pushFrame(int state) throws IOException {
    final Frame f = getFrame(currentFrame == null ? 0 : 1+currentFrame.ord);
        
    f.fp = f.fpOrig = currentFrame.lastSubFP;
    f.prefix = currentFrame.prefix + currentFrame.suffix;
    // if (DEBUG) System.out.println("    pushFrame state=" + state + " prefix=" + f.prefix);
    f.setState(state);

    // Walk the arc through the index -- we only
    // "bother" with this so we can get the floor data
    // from the index and skip floor blocks when
    // possible:
    FST.Arc<BytesRef> arc = currentFrame.arc;
    int idx = currentFrame.prefix;
    assert currentFrame.suffix > 0;
    BytesRef output = currentFrame.outputPrefix;
    while (idx < f.prefix) {
      final int target = term.bytes[idx] & 0xff;
      // TODO: we could be more efficient for the next()
      // case by using current arc as starting point,
      // passed to findTargetArc
      arc = fr.index.findTargetArc(target, arc, getArc(1+idx), fstReader);
      assert arc != null;
      output = fr.parent.fstOutputs.add(output, arc.output);
      idx++;
    }

    f.arc = arc;
    f.outputPrefix = output;
    assert arc.isFinal();
    f.load(fr.parent.fstOutputs.add(output, arc.nextFinalOutput));
    return f;
  }

  @Override
  public BytesRef term() {
    return term;
  }

  @Override
  public int docFreq() throws IOException {
    //if (DEBUG) System.out.println("BTIR.docFreq");
    currentFrame.decodeMetaData();
    //if (DEBUG) System.out.println("  return " + currentFrame.termState.docFreq);
    return currentFrame.termState.docFreq;
  }

  @Override
  public long totalTermFreq() throws IOException {
    currentFrame.decodeMetaData();
    return currentFrame.termState.totalTermFreq;
  }

  @Override
  public DocsEnum docs(Bits skipDocs, DocsEnum reuse, int flags) throws IOException {
    currentFrame.decodeMetaData();
    return fr.parent.postingsReader.docs(fr.fieldInfo, currentFrame.termState, skipDocs, reuse, flags);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
    if (fr.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
      // Positions were not indexed:
      return null;
    }

    currentFrame.decodeMetaData();
    return fr.parent.postingsReader.docsAndPositions(fr.fieldInfo, currentFrame.termState, skipDocs, reuse, flags);
  }

  private int getState() {
    int state = currentFrame.state;
    for(int idx=0;idx<currentFrame.suffix;idx++) {
      state = runAutomaton.step(state,  currentFrame.suffixBytes[currentFrame.startBytePos+idx] & 0xff);
      assert state != -1;
    }
    return state;
  }

  // NOTE: specialized to only doing the first-time
  // seek, but we could generalize it to allow
  // arbitrary seekExact/Ceil.  Note that this is a
  // seekFloor!
  private void seekToStartTerm(BytesRef target) throws IOException {
    //if (DEBUG) System.out.println("seek to startTerm=" + target.utf8ToString());
    assert currentFrame.ord == 0;
    if (term.length < target.length) {
      term.bytes = ArrayUtil.grow(term.bytes, target.length);
    }
    FST.Arc<BytesRef> arc = arcs[0];
    assert arc == currentFrame.arc;

    for(int idx=0;idx<=target.length;idx++) {

      while (true) {
        final int savePos = currentFrame.suffixesReader.getPosition();
        final int saveStartBytePos = currentFrame.startBytePos;
        final int saveSuffix = currentFrame.suffix;
        final long saveLastSubFP = currentFrame.lastSubFP;
        final int saveTermBlockOrd = currentFrame.termState.termBlockOrd;

        final boolean isSubBlock = currentFrame.next();

        //if (DEBUG) System.out.println("    cycle ent=" + currentFrame.nextEnt + " (of " + currentFrame.entCount + ") prefix=" + currentFrame.prefix + " suffix=" + currentFrame.suffix + " isBlock=" + isSubBlock + " firstLabel=" + (currentFrame.suffix == 0 ? "" : (currentFrame.suffixBytes[currentFrame.startBytePos])&0xff));
        term.length = currentFrame.prefix + currentFrame.suffix;
        if (term.bytes.length < term.length) {
          term.bytes = ArrayUtil.grow(term.bytes, term.length);
        }
        System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);

        if (isSubBlock && StringHelper.startsWith(target, term)) {
          // Recurse
          //if (DEBUG) System.out.println("      recurse!");
          currentFrame = pushFrame(getState());
          break;
        } else {
          final int cmp = term.compareTo(target);
          if (cmp < 0) {
            if (currentFrame.nextEnt == currentFrame.entCount) {
              if (!currentFrame.isLastInFloor) {
                //if (DEBUG) System.out.println("  load floorBlock");
                currentFrame.loadNextFloorBlock();
                continue;
              } else {
                //if (DEBUG) System.out.println("  return term=" + brToString(term));
                return;
              }
            }
            continue;
          } else if (cmp == 0) {
            //if (DEBUG) System.out.println("  return term=" + brToString(term));
            return;
          } else {
            // Fallback to prior entry: the semantics of
            // this method is that the first call to
            // next() will return the term after the
            // requested term
            currentFrame.nextEnt--;
            currentFrame.lastSubFP = saveLastSubFP;
            currentFrame.startBytePos = saveStartBytePos;
            currentFrame.suffix = saveSuffix;
            currentFrame.suffixesReader.setPosition(savePos);
            currentFrame.termState.termBlockOrd = saveTermBlockOrd;
            System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);
            term.length = currentFrame.prefix + currentFrame.suffix;
            // If the last entry was a block we don't
            // need to bother recursing and pushing to
            // the last term under it because the first
            // next() will simply skip the frame anyway
            return;
          }
        }
      }
    }

    assert false;
  }

  @Override
  public BytesRef next() throws IOException {

    // if (DEBUG) {
    //   System.out.println("\nintEnum.next seg=" + segment);
    //   System.out.println("  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
    // }

    nextTerm:
    while(true) {
      // Pop finished frames
      while (currentFrame.nextEnt == currentFrame.entCount) {
        if (!currentFrame.isLastInFloor) {
          //if (DEBUG) System.out.println("    next-floor-block");
          currentFrame.loadNextFloorBlock();
          //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
        } else {
          //if (DEBUG) System.out.println("  pop frame");
          if (currentFrame.ord == 0) {
            return null;
          }
          final long lastFP = currentFrame.fpOrig;
          currentFrame = stack[currentFrame.ord-1];
          assert currentFrame.lastSubFP == lastFP;
          //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
        }
      }

      final boolean isSubBlock = currentFrame.next();
      // if (DEBUG) {
      //   final BytesRef suffixRef = new BytesRef();
      //   suffixRef.bytes = currentFrame.suffixBytes;
      //   suffixRef.offset = currentFrame.startBytePos;
      //   suffixRef.length = currentFrame.suffix;
      //   System.out.println("    " + (isSubBlock ? "sub-block" : "term") + " " + currentFrame.nextEnt + " (of " + currentFrame.entCount + ") suffix=" + brToString(suffixRef));
      // }

      if (currentFrame.suffix != 0) {
        final int label = currentFrame.suffixBytes[currentFrame.startBytePos] & 0xff;
        while (label > currentFrame.curTransitionMax) {
          if (currentFrame.transitionIndex >= currentFrame.transitions.length-1) {
            // Stop processing this frame -- no further
            // matches are possible because we've moved
            // beyond what the max transition will allow
            //if (DEBUG) System.out.println("      break: trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]));

            // sneaky!  forces a pop above
            currentFrame.isLastInFloor = true;
            currentFrame.nextEnt = currentFrame.entCount;
            continue nextTerm;
          }
          currentFrame.transitionIndex++;
          currentFrame.curTransitionMax = currentFrame.transitions[currentFrame.transitionIndex].getMax();
          //if (DEBUG) System.out.println("      next trans=" + currentFrame.transitions[currentFrame.transitionIndex]);
        }
      }

      // First test the common suffix, if set:
      if (compiledAutomaton.commonSuffixRef != null && !isSubBlock) {
        final int termLen = currentFrame.prefix + currentFrame.suffix;
        if (termLen < compiledAutomaton.commonSuffixRef.length) {
          // No match
          // if (DEBUG) {
          //   System.out.println("      skip: common suffix length");
          // }
          continue nextTerm;
        }

        final byte[] suffixBytes = currentFrame.suffixBytes;
        final byte[] commonSuffixBytes = compiledAutomaton.commonSuffixRef.bytes;

        final int lenInPrefix = compiledAutomaton.commonSuffixRef.length - currentFrame.suffix;
        assert compiledAutomaton.commonSuffixRef.offset == 0;
        int suffixBytesPos;
        int commonSuffixBytesPos = 0;

        if (lenInPrefix > 0) {
          // A prefix of the common suffix overlaps with
          // the suffix of the block prefix so we first
          // test whether the prefix part matches:
          final byte[] termBytes = term.bytes;
          int termBytesPos = currentFrame.prefix - lenInPrefix;
          assert termBytesPos >= 0;
          final int termBytesPosEnd = currentFrame.prefix;
          while (termBytesPos < termBytesPosEnd) {
            if (termBytes[termBytesPos++] != commonSuffixBytes[commonSuffixBytesPos++]) {
              // if (DEBUG) {
              //   System.out.println("      skip: common suffix mismatch (in prefix)");
              // }
              continue nextTerm;
            }
          }
          suffixBytesPos = currentFrame.startBytePos;
        } else {
          suffixBytesPos = currentFrame.startBytePos + currentFrame.suffix - compiledAutomaton.commonSuffixRef.length;
        }

        // Test overlapping suffix part:
        final int commonSuffixBytesPosEnd = compiledAutomaton.commonSuffixRef.length;
        while (commonSuffixBytesPos < commonSuffixBytesPosEnd) {
          if (suffixBytes[suffixBytesPos++] != commonSuffixBytes[commonSuffixBytesPos++]) {
            // if (DEBUG) {
            //   System.out.println("      skip: common suffix mismatch");
            // }
            continue nextTerm;
          }
        }
      }

      // TODO: maybe we should do the same linear test
      // that AutomatonTermsEnum does, so that if we
      // reach a part of the automaton where .* is
      // "temporarily" accepted, we just blindly .next()
      // until the limit

      // See if the term prefix matches the automaton:
      int state = currentFrame.state;
      for (int idx=0;idx<currentFrame.suffix;idx++) {
        state = runAutomaton.step(state,  currentFrame.suffixBytes[currentFrame.startBytePos+idx] & 0xff);
        if (state == -1) {
          // No match
          //System.out.println("    no s=" + state);
          continue nextTerm;
        } else {
          //System.out.println("    c s=" + state);
        }
      }

      if (isSubBlock) {
        // Match!  Recurse:
        //if (DEBUG) System.out.println("      sub-block match to state=" + state + "; recurse fp=" + currentFrame.lastSubFP);
        copyTerm();
        currentFrame = pushFrame(state);
        //if (DEBUG) System.out.println("\n  frame ord=" + currentFrame.ord + " prefix=" + brToString(new BytesRef(term.bytes, term.offset, currentFrame.prefix)) + " state=" + currentFrame.state + " lastInFloor?=" + currentFrame.isLastInFloor + " fp=" + currentFrame.fp + " trans=" + (currentFrame.transitions.length == 0 ? "n/a" : currentFrame.transitions[currentFrame.transitionIndex]) + " outputPrefix=" + currentFrame.outputPrefix);
      } else if (runAutomaton.isAccept(state)) {
        copyTerm();
        //if (DEBUG) System.out.println("      term match to state=" + state + "; return term=" + brToString(term));
        assert savedStartTerm == null || term.compareTo(savedStartTerm) > 0: "saveStartTerm=" + savedStartTerm.utf8ToString() + " term=" + term.utf8ToString();
        return term;
      } else {
        //System.out.println("    no s=" + state);
      }
    }
  }

  private void copyTerm() {
    //System.out.println("      copyTerm cur.prefix=" + currentFrame.prefix + " cur.suffix=" + currentFrame.suffix + " first=" + (char) currentFrame.suffixBytes[currentFrame.startBytePos]);
    final int len = currentFrame.prefix + currentFrame.suffix;
    if (term.bytes.length < len) {
      term.bytes = ArrayUtil.grow(term.bytes, len);
    }
    System.arraycopy(currentFrame.suffixBytes, currentFrame.startBytePos, term.bytes, currentFrame.prefix, currentFrame.suffix);
    term.length = len;
  }

  @Override
  public boolean seekExact(BytesRef text) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long ord() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) {
    throw new UnsupportedOperationException();
  }
}
