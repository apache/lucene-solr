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
package org.apache.lucene.codecs.blocktreeords;


//import java.io.*;
//import java.nio.charset.StandardCharsets;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.blocktreeords.FSTOrdsOutputs.Output;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

/** Iterates through terms in this field. */
public final class OrdsSegmentTermsEnum extends TermsEnum {

  // Lazy init:
  IndexInput in;

  // static boolean DEBUG = true;

  private OrdsSegmentTermsEnumFrame[] stack;
  private final OrdsSegmentTermsEnumFrame staticFrame;
  OrdsSegmentTermsEnumFrame currentFrame;
  boolean termExists;
  final OrdsFieldReader fr;

  private int targetBeforeCurrentLength;

  private final ByteArrayDataInput scratchReader = new ByteArrayDataInput();

  // What prefix of the current term was present in the index:
  private int validIndexPrefix;

  // assert only:
  private boolean eof;

  final BytesRefBuilder term = new BytesRefBuilder();
  private final FST.BytesReader fstReader;

  @SuppressWarnings({"rawtypes","unchecked"}) private FST.Arc<Output>[] arcs =
  new FST.Arc[1];

  boolean positioned;

  OrdsSegmentTermsEnum(OrdsFieldReader fr) throws IOException {
    this.fr = fr;

    //if (DEBUG) System.out.println("BTTR.init seg=" + segment);
    stack = new OrdsSegmentTermsEnumFrame[0];
        
    // Used to hold seek by TermState, or cached seek
    staticFrame = new OrdsSegmentTermsEnumFrame(this, -1);

    if (fr.index == null) {
      fstReader = null;
    } else {
      fstReader = fr.index.getBytesReader();
    }

    // Init w/ root block; don't use index since it may
    // not (and need not) have been loaded
    for(int arcIdx=0;arcIdx<arcs.length;arcIdx++) {
      arcs[arcIdx] = new FST.Arc<>();
    }
    
    currentFrame = staticFrame;
    final FST.Arc<Output> arc;
    if (fr.index != null) {
      arc = fr.index.getFirstArc(arcs[0]);
      // Empty string prefix must have an output in the index!
      assert arc.isFinal();
    } else {
      arc = null;
    }
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
      this.in = fr.parent.in.clone();
    }
  }

  private OrdsSegmentTermsEnumFrame getFrame(int ord) throws IOException {
    if (ord >= stack.length) {
      final OrdsSegmentTermsEnumFrame[] next = new OrdsSegmentTermsEnumFrame[ArrayUtil.oversize(1+ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(stack, 0, next, 0, stack.length);
      for(int stackOrd=stack.length;stackOrd<next.length;stackOrd++) {
        next[stackOrd] = new OrdsSegmentTermsEnumFrame(this, stackOrd);
      }
      stack = next;
    }
    assert stack[ord].ord == ord;
    return stack[ord];
  }

  private FST.Arc<Output> getArc(int ord) {
    if (ord >= arcs.length) {
      @SuppressWarnings({"rawtypes","unchecked"}) final FST.Arc<Output>[] next =
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
  OrdsSegmentTermsEnumFrame pushFrame(FST.Arc<Output> arc, Output frameData, int length) throws IOException {
    scratchReader.reset(frameData.bytes.bytes, frameData.bytes.offset, frameData.bytes.length);
    final long code = scratchReader.readVLong();
    final long fpSeek = code >>> OrdsBlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS;
    // System.out.println("    fpSeek=" + fpSeek);
    final OrdsSegmentTermsEnumFrame f = getFrame(1+currentFrame.ord);
    f.hasTerms = (code & OrdsBlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS) != 0;
    f.hasTermsOrig = f.hasTerms;
    f.isFloor = (code & OrdsBlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR) != 0;

    // Must setFloorData before pushFrame in case pushFrame tries to rewind:
    if (f.isFloor) {
      f.termOrdOrig = frameData.startOrd;
      f.setFloorData(scratchReader, frameData.bytes);
    }

    pushFrame(arc, fpSeek, length, frameData.startOrd);

    return f;
  }

  // Pushes next'd frame or seek'd frame; we later
  // lazy-load the frame only when needed
  OrdsSegmentTermsEnumFrame pushFrame(FST.Arc<Output> arc, long fp, int length, long termOrd) throws IOException {
    final OrdsSegmentTermsEnumFrame f = getFrame(1+currentFrame.ord);
    f.arc = arc;
    // System.out.println("pushFrame termOrd= " + termOrd + " fpOrig=" + f.fpOrig + " fp=" + fp + " nextEnt=" + f.nextEnt);
    if (f.fpOrig == fp && f.nextEnt != -1) {
      //if (DEBUG) System.out.println("      push reused frame ord=" + f.ord + " fp=" + f.fp + " isFloor?=" + f.isFloor + " hasTerms=" + f.hasTerms + " pref=" + term + " nextEnt=" + f.nextEnt + " targetBeforeCurrentLength=" + targetBeforeCurrentLength + " term.length=" + term.length + " vs prefix=" + f.prefix);
      if (f.prefix > targetBeforeCurrentLength) {
        // System.out.println("        do rewind!");
        f.rewind();
      } else {
        // if (DEBUG) {
        // System.out.println("        skip rewind!");
        // }
      }
      assert length == f.prefix;
      assert termOrd == f.termOrdOrig;
    } else {
      f.nextEnt = -1;
      f.prefix = length;
      f.state.termBlockOrd = 0;
      f.termOrdOrig = termOrd;
      // System.out.println("set termOrdOrig=" + termOrd);
      f.termOrd = termOrd;
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

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      // If BytesRef isn't actually UTF8, or it's eg a
      // prefix of UTF8 that ends mid-unicode-char, we
      // fallback to hex:
      return b.toString();
    }
  }

  @Override
  public boolean seekExact(final BytesRef target) throws IOException {

    if (fr.index == null) {
      throw new IllegalStateException("terms index was not loaded");
    }

    term.grow(1+target.length);

    assert clearEOF();

    /*
    if (DEBUG) {
      System.out.println("\nBTTR.seekExact seg=" + fr.parent.segment + " target=" + fr.fieldInfo.name + ":" + brToString(target) + " current=" + brToString(term) + " (exists?=" + termExists + ") validIndexPrefix=" + validIndexPrefix);
      printSeekState(System.out);
    }
    */

    FST.Arc<Output> arc;
    int targetUpto;
    Output output;

    targetBeforeCurrentLength = currentFrame.ord;

    if (positioned && currentFrame != staticFrame) {

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
          
      OrdsSegmentTermsEnumFrame lastFrame = stack[0];
      assert validIndexPrefix <= term.length();

      final int targetLimit = Math.min(target.length, validIndexPrefix);

      int cmp = 0;

      // TODO: reverse vLong byte order for better FST
      // prefix output sharing

      // First compare up to valid seek frames:
      while (targetUpto < targetLimit) {
        cmp = (term.byteAt(targetUpto)&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
        // if (DEBUG) {
        //    System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")"   + " arc.output=" + arc.output + " output=" + output);
        // }
        if (cmp != 0) {
          break;
        }
        arc = arcs[1+targetUpto];
        assert arc.label == (target.bytes[target.offset + targetUpto] & 0xFF): "arc.label=" + (char) arc.label + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF);
        if (arc.output != OrdsBlockTreeTermsWriter.NO_OUTPUT) {
          output = OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.output);
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
        final int targetLimit2 = Math.min(target.length, term.length());
        while (targetUpto < targetLimit2) {
          cmp = (term.byteAt(targetUpto)&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
          // if (DEBUG) {
          //    System.out.println("    cycle2 targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")");
          // }
          if (cmp != 0) {
            break;
          }
          targetUpto++;
        }

        if (cmp == 0) {
          cmp = term.length() - target.length;
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
        targetBeforeCurrentLength = lastFrame.ord;
        // if (DEBUG) {
        //   System.out.println("  target is before current (shares prefixLen=" + targetUpto + "); rewind frame ord=" + lastFrame.ord);
        // }
        currentFrame = lastFrame;
        currentFrame.rewind();
      } else {
        // Target is exactly the same as current term
        assert term.length() == target.length;
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
      arc = fr.index.getFirstArc(arcs[0]);

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
      currentFrame = pushFrame(arc, OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.nextFinalOutput), 0);
    }

    positioned = true;

    // if (DEBUG) {
    //   System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output + " currentFrame.ord=" + currentFrame.ord + " targetBeforeCurrentLength=" + targetBeforeCurrentLength);
    // }

    // We are done sharing the common prefix with the incoming target and where we are currently seek'd; now continue walking the index:
    while (targetUpto < target.length) {

      final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

      final FST.Arc<Output> nextArc = fr.index.findTargetArc(targetLabel, arc, getArc(1+targetUpto), fstReader);

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
          term.setByteAt(targetUpto, (byte) targetLabel);
          term.setLength(1+targetUpto);
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
        term.setByteAt(targetUpto, (byte) targetLabel);
        // Aggregate output as we go:
        assert arc.output != null;
        if (arc.output != OrdsBlockTreeTermsWriter.NO_OUTPUT) {
          output = OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.output);
        }

        // if (DEBUG) {
        //   System.out.println("    index: follow label=" + toHex(target.bytes[target.offset + targetUpto]&0xff) + " arc.output=" + arc.output + " arc.nfo=" + arc.nextFinalOutput);
        // }
        targetUpto++;

        if (arc.isFinal()) {
          //if (DEBUG) System.out.println("    arc is final!");
          currentFrame = pushFrame(arc, OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.nextFinalOutput), targetUpto);
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
      term.setLength(targetUpto);
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
    if (fr.index == null) {
      throw new IllegalStateException("terms index was not loaded");
    }
   
    term.grow(1+target.length);

    assert clearEOF();

    //if (DEBUG) {
    //System.out.println("\nBTTR.seekCeil seg=" + segment + " target=" + fieldInfo.name + ":" + target.utf8ToString() + " " + target + " current=" + brToString(term) + " (exists?=" + termExists + ") validIndexPrefix=  " + validIndexPrefix);
    //printSeekState();
    //}

    FST.Arc<Output> arc;
    int targetUpto;
    Output output;

    targetBeforeCurrentLength = currentFrame.ord;

    if (positioned && currentFrame != staticFrame) {

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
          
      OrdsSegmentTermsEnumFrame lastFrame = stack[0];
      assert validIndexPrefix <= term.length();

      final int targetLimit = Math.min(target.length, validIndexPrefix);

      int cmp = 0;

      // TODO: we should write our vLong backwards (MSB
      // first) to get better sharing from the FST

      // First compare up to valid seek frames:
      while (targetUpto < targetLimit) {
        cmp = (term.byteAt(targetUpto)&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
        //if (DEBUG) {
        //System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")"   + " arc.output=" + arc.output + " output=" + output);
        //}
        if (cmp != 0) {
          break;
        }
        arc = arcs[1+targetUpto];
        assert arc.label == (target.bytes[target.offset + targetUpto] & 0xFF): "arc.label=" + (char) arc.label + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF);
        // TODO: we could save the outputs in local
        // byte[][] instead of making new objs ever
        // seek; but, often the FST doesn't have any
        // shared bytes (but this could change if we
        // reverse vLong byte order)
        if (arc.output != OrdsBlockTreeTermsWriter.NO_OUTPUT) {
          output = OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.output);
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
        final int targetLimit2 = Math.min(target.length, term.length());
        while (targetUpto < targetLimit2) {
          cmp = (term.byteAt(targetUpto)&0xFF) - (target.bytes[target.offset + targetUpto]&0xFF);
          //if (DEBUG) {
          //System.out.println("    cycle2 targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")");
          //}
          if (cmp != 0) {
            break;
          }
          targetUpto++;
        }

        if (cmp == 0) {
          cmp = term.length() - target.length;
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
        assert term.length() == target.length;
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
      arc = fr.index.getFirstArc(arcs[0]);

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
      currentFrame = pushFrame(arc, OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.nextFinalOutput), 0);
    }

    positioned = true;

    //if (DEBUG) {
    //System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output + " currentFrame.ord+1=" + currentFrame.ord + " targetBeforeCurrentLength=" + targetBeforeCurrentLength);
    //}

    // We are done sharing the common prefix with the incoming target and where we are currently seek'd; now continue walking the index:
    while (targetUpto < target.length) {

      final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

      final FST.Arc<Output> nextArc = fr.index.findTargetArc(targetLabel, arc, getArc(1+targetUpto), fstReader);

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
        term.setByteAt(targetUpto, (byte) targetLabel);
        arc = nextArc;
        // Aggregate output as we go:
        assert arc.output != null;
        if (arc.output != OrdsBlockTreeTermsWriter.NO_OUTPUT) {
          output = OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.output);
        }

        //if (DEBUG) {
        //System.out.println("    index: follow label=" + toHex(target.bytes[target.offset + targetUpto]&0xff) + " arc.output=" + arc.output + " arc.nfo=" + arc.nextFinalOutput);
        //}
        targetUpto++;

        if (arc.isFinal()) {
          //if (DEBUG) System.out.println("    arc is final!");
          currentFrame = pushFrame(arc, OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.nextFinalOutput), targetUpto);
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
        OrdsSegmentTermsEnumFrame f = getFrame(ord);
        assert f != null;
        final BytesRef prefix = new BytesRef(term.bytes(), 0, f.prefix);
        if (f.nextEnt == -1) {
          out.println("    frame " + (isSeekFrame ? "(seek)" : "(next)") + " ord=" + ord + " fp=" + f.fp + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "") + " prefixLen=" + f.prefix + " prefix=" + brToString(prefix) + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")")) + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " code=" + ((f.fp<<OrdsBlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) + (f.hasTerms ? OrdsBlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) + (f.isFloor ? OrdsBlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0)) + " isLastInFloor=" + f.isLastInFloor + " mdUpto=" + f.metaDataUpto + " tbOrd=" + f.getTermBlockOrd() + " termOrd=" + f.termOrd);
        } else {
          out.println("    frame " + (isSeekFrame ? "(seek, loaded)" : "(next, loaded)") + " ord=" + ord + " fp=" + f.fp + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "") + " prefixLen=" + f.prefix + " prefix=" + brToString(prefix) + " nextEnt=" + f.nextEnt + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")")) + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " code=" + ((f.fp<<OrdsBlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) + (f.hasTerms ? OrdsBlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) + (f.isFloor ? OrdsBlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0)) + " lastSubFP=" + f.lastSubFP + " isLastInFloor=" + f.isLastInFloor + " mdUpto=" + f.metaDataUpto + " tbOrd=" + f.getTermBlockOrd() + " termOrd=" + f.termOrd);
        }
        if (fr.index != null) {
          assert !isSeekFrame || f.arc != null: "isSeekFrame=" + isSeekFrame + " f.arc=" + f.arc;
          if (f.prefix > 0 && isSeekFrame && f.arc.label != (term.byteAt(f.prefix-1)&0xFF)) {
            out.println("      broken seek state: arc.label=" + (char) f.arc.label + " vs term byte=" + (char) (term.byteAt(f.prefix-1)&0xFF));
            throw new RuntimeException("seek state is broken");
          }
          Output output = Util.get(fr.index, prefix);
          if (output == null) {
            out.println("      broken seek state: prefix is not final in index");
            throw new RuntimeException("seek state is broken");
          } else if (isSeekFrame && !f.isFloor) {
            final ByteArrayDataInput reader = new ByteArrayDataInput(output.bytes.bytes, output.bytes.offset, output.bytes.length);
            final long codeOrig = reader.readVLong();
            final long code = (f.fp << OrdsBlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS) | (f.hasTerms ? OrdsBlockTreeTermsWriter.OUTPUT_FLAG_HAS_TERMS:0) | (f.isFloor ? OrdsBlockTreeTermsWriter.OUTPUT_FLAG_IS_FLOOR:0);
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
      final FST.Arc<Output> arc;
      if (fr.index != null) {
        arc = fr.index.getFirstArc(arcs[0]);
        // Empty string prefix must have an output in the index!
        assert arc.isFinal();
      } else {
        arc = null;
      }
      currentFrame = pushFrame(arc, fr.rootCode, 0);
      currentFrame.loadBlock();
      positioned = true;
    }

    targetBeforeCurrentLength = currentFrame.ord;

    assert !eof;
    //if (DEBUG) {
    //System.out.println("\nBTTR.next seg=" + segment + " term=" + brToString(term) + " termExists?=" + termExists + " field=" + fieldInfo.name + " termBlockOrd=" + currentFrame.state.termBlockOrd + " validIndexPrefix=" + validIndexPrefix);
    //printSeekState();
    //}

    if (currentFrame == staticFrame || positioned == false) {
      // If seek was previously called and the term was
      // cached, or seek(TermState) was called, usually
      // caller is just going to pull a D/&PEnum or get
      // docFreq, etc.  But, if they then call next(),
      // this method catches up all internal state so next()
      // works properly:
      // if (DEBUG) System.out.println("  re-seek to pending term=" + term.utf8ToString() + " " + term);
      final boolean result = seekExact(term.get());
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
          term.setLength(0);
          validIndexPrefix = 0;
          currentFrame.rewind();
          termExists = false;
          positioned = false;
          return null;
        }
        final long lastFP = currentFrame.fpOrig;
        currentFrame = stack[currentFrame.ord-1];

        if (currentFrame.nextEnt == -1 || currentFrame.lastSubFP != lastFP) {
          // We popped into a frame that's not loaded
          // yet or not scan'd to the right entry
          currentFrame.scanToFloorFrame(term.get());
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
      long prevTermOrd = currentFrame.termOrd;
      if (currentFrame.next()) {
        // Push to new block:
        //if (DEBUG) System.out.println("  push frame");
        currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length(), prevTermOrd);
        // This is a "next" frame -- even if it's
        // floor'd we must pretend it isn't so we don't
        // try to scan to the right floor frame:
        currentFrame.isFloor = false;
        //currentFrame.hasTerms = true;
        currentFrame.loadBlock();
      } else {
        //if (DEBUG) System.out.println("  return term=" + term.utf8ToString() + " " + term + " currentFrame.ord=" + currentFrame.ord);
        positioned = true;
        return term.get();
      }
    }
  }

  @Override
  public BytesRef term() {
    assert !eof;
    return term.get();
  }

  @Override
  public long ord() {
    assert !eof;
    assert currentFrame.termOrd > 0;
    return currentFrame.termOrd-1;
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
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {

    if (PostingsEnum.featureRequested(flags, DocsAndPositionsEnum.OLD_NULL_SEMANTICS)) {
      if (fr.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
        // Positions were not indexed:
        return null;
      }
    }

    assert !eof;
    //if (DEBUG) {
    //System.out.println("BTTR.docs seg=" + segment);
    //}
    currentFrame.decodeMetaData();
    //if (DEBUG) {
    //System.out.println("  state=" + currentFrame.state);
    //}
    return fr.parent.postingsReader.postings(fr.fieldInfo, currentFrame.state, reuse, flags);
  }

  @Override
  public void seekExact(BytesRef target, TermState otherState) {
    // if (DEBUG) {
    //   System.out.println("BTTR.seekExact termState seg=" + segment + " target=" + target.utf8ToString() + " " + target + " state=" + otherState);
    // }
    assert clearEOF();
    if (target.compareTo(term.get()) != 0 || !termExists) {
      assert otherState != null && otherState instanceof BlockTermState;
      BlockTermState blockState = (BlockTermState) otherState;
      currentFrame = staticFrame;
      currentFrame.state.copyFrom(otherState);
      term.copyBytes(target);
      currentFrame.metaDataUpto = currentFrame.getTermBlockOrd();
      currentFrame.termOrd = blockState.ord+1;
      assert currentFrame.metaDataUpto > 0;
      validIndexPrefix = 0;
    } else {
      // if (DEBUG) {
      //   System.out.println("  skip seek: already on target state=" + currentFrame.state);
      // }
    }
    positioned = true;
  }
      
  @Override
  public TermState termState() throws IOException {
    assert !eof;
    currentFrame.decodeMetaData();
    BlockTermState ts = (BlockTermState) currentFrame.state.clone();
    assert currentFrame.termOrd > 0;
    ts.ord = currentFrame.termOrd-1;
    //if (DEBUG) System.out.println("BTTR.termState seg=" + segment + " state=" + ts);
    return ts;
  }

  @Override
  public void seekExact(long targetOrd) throws IOException {
    // System.out.println("seekExact targetOrd=" + targetOrd);
    if (targetOrd < 0 || targetOrd >= fr.numTerms) {
      throw new IllegalArgumentException("targetOrd out of bounds (got: " + targetOrd + ", numTerms=" + fr.numTerms + ")");
    }

    assert clearEOF();

    // First do reverse lookup in the index to find the block that holds this term:
    InputOutput io = getByOutput(targetOrd);
    term.grow(io.input.length);

    Util.toBytesRef(io.input, term);
    if (io.input.length == 0) {
      currentFrame = staticFrame;
    } else {
      currentFrame = getFrame(io.input.length-1);
    }
    FST.Arc<Output> arc = getArc(io.input.length);

    // Don't force rewind based on term length; we rewind below based on ord:
    targetBeforeCurrentLength = Integer.MAX_VALUE;
    currentFrame = pushFrame(arc, io.output, io.input.length);
    if (currentFrame.termOrd > targetOrd) {
      //System.out.println("  do rewind: " + currentFrame.termOrd);
      currentFrame.rewind();
    }

    currentFrame.scanToFloorFrame(targetOrd);
    currentFrame.loadBlock();
    // System.out.println("  after loadBlock termOrd=" + currentFrame.termOrd + " vs " + targetOrd);

    while (currentFrame.termOrd <= targetOrd) {
      currentFrame.next();
    }

    assert currentFrame.termOrd == targetOrd+1: "currentFrame.termOrd=" + currentFrame.termOrd + " vs ord=" + targetOrd;
    assert termExists;

    // Leave enum fully unpositioned, because we didn't set frames for each byte leading up to current term:
    validIndexPrefix = 0;
    positioned = false;
  }

  @Override
  public String toString() {
    return "OrdsSegmentTermsEnum(seg=" + fr.parent + ")";
  }

  /** Holds a single input (IntsRef) + output pair. */
  private static class InputOutput {
    public IntsRef input;
    public Output output;

    @Override
    public String toString() {
      return "InputOutput(input=" + input + " output=" + output + ")";
    }
  }

  private final FST.Arc<Output> arc = new FST.Arc<>();
  
  // TODO: this is similar to Util.getByOutput ... can we refactor/share?
  /** Specialized getByOutput that can understand the ranges (startOrd to endOrd) we use here, not just startOrd. */
  private InputOutput getByOutput(long targetOrd) throws IOException {

    final IntsRefBuilder result = new IntsRefBuilder();

    fr.index.getFirstArc(arc);
    Output output = arc.output;
    int upto = 0;

    int bestUpto = 0;
    Output bestOutput = null;

    /*
    Writer w = new OutputStreamWriter(new FileOutputStream("/tmp/out.dot"));
    Util.toDot(fr.index, w, true, true);
    w.close();
    */

    // System.out.println("reverseLookup seg=" + fr.parent.segment + " output=" + targetOrd);

    while (true) {
      // System.out.println("  loop: output=" + output.startOrd + "-" + (Long.MAX_VALUE-output.endOrd) + " upto=" + upto + " arc=" + arc + " final?=" + arc.isFinal());
      if (arc.isFinal()) {
        final Output finalOutput = OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.nextFinalOutput);
        // System.out.println("  isFinal: " + finalOutput.startOrd + "-" + (Long.MAX_VALUE-finalOutput.endOrd));
        if (targetOrd >= finalOutput.startOrd && targetOrd <= Long.MAX_VALUE-finalOutput.endOrd) {
          // Only one range should match across all arc leaving this node
          //assert bestOutput == null;
          bestOutput = finalOutput;
          bestUpto = upto;
        }
      }

      if (FST.targetHasArcs(arc)) {
        // System.out.println("  targetHasArcs");
        result.grow(1+upto);
        
        fr.index.readFirstRealTargetArc(arc.target, arc, fstReader);

        if (arc.bytesPerArc != 0) {
          // System.out.println("  array arcs");

          int low = 0;
          int high = arc.numArcs-1;
          int mid = 0;
          //System.out.println("bsearch: numArcs=" + arc.numArcs + " target=" + targetOutput + " output=" + output);
          boolean found = false;
          while (low <= high) {
            mid = (low + high) >>> 1;
            fstReader.setPosition(arc.posArcsStart);
            fstReader.skipBytes(arc.bytesPerArc*mid);
            final byte flags = fstReader.readByte();
            fr.index.readLabel(fstReader);
            final Output minArcOutput;
            if ((flags & FST.BIT_ARC_HAS_OUTPUT) != 0) {
              minArcOutput = OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, OrdsBlockTreeTermsWriter.FST_OUTPUTS.read(fstReader));
            } else {
              minArcOutput = output;
            }
            // System.out.println("  cycle mid=" + mid + " targetOrd=" + targetOrd + " output=" + minArcOutput.startOrd + "-" + (Long.MAX_VALUE-minArcOutput.endOrd));
            if (targetOrd > Long.MAX_VALUE-minArcOutput.endOrd) {
              low = mid + 1;
            } else if (targetOrd < minArcOutput.startOrd) {
              high = mid - 1;
            } else {
              // System.out.println("    found!!");
              found = true;
              break;
            }
          }

          if (found) {
            // Keep recursing
            arc.arcIdx = mid-1;
          } else {
            result.setLength(bestUpto);
            InputOutput io = new InputOutput();
            io.input = result.get();
            io.output = bestOutput;
            // System.out.println("  ret0=" + io);
            return io;
          }

          fr.index.readNextRealArc(arc, fstReader);

          // Recurse on this arc:
          result.setIntAt(upto++, arc.label);
          output = OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.output);

        } else {
          // System.out.println("    non-array arc");

          while (true) {
            // System.out.println("    cycle label=" + arc.label + " output=" + arc.output);

            // This is the min output we'd hit if we follow
            // this arc:
            final Output minArcOutput = OrdsBlockTreeTermsWriter.FST_OUTPUTS.add(output, arc.output);
            long endOrd = Long.MAX_VALUE - minArcOutput.endOrd;
            // System.out.println("    endOrd=" + endOrd + " targetOrd=" + targetOrd);

            if (targetOrd >= minArcOutput.startOrd && targetOrd <= endOrd) {
              // Recurse on this arc:
              output = minArcOutput;
              result.setIntAt(upto++, arc.label);
              break;
            } else if (targetOrd < endOrd || arc.isLast()) {
              result.setLength(bestUpto);
              InputOutput io = new InputOutput();
              io.input = result.get();
              assert bestOutput != null;
              io.output = bestOutput;
              // System.out.println("  ret2=" + io);
              return io;
            } else {
              // System.out.println("  next arc");
              // Read next arc in this node:
              fr.index.readNextRealArc(arc, fstReader);
            }
          }
        }
      } else {
        result.setLength(bestUpto);
        InputOutput io = new InputOutput();
        io.input = result.get();
        io.output = bestOutput;
        // System.out.println("  ret3=" + io);
        return io;
      }
    }
  }
}
