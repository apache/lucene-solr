package org.apache.lucene.util.automaton.fst;

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

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/** Static helper methods */
public final class Util {
  private Util() {
  }

  /** Looks up the output for this input, or null if the
   *  input is not accepted. FST must be
   *  INPUT_TYPE.BYTE4. */
  public static<T> T get(FST<T> fst, IntsRef input) throws IOException {
    assert fst.inputType == FST.INPUT_TYPE.BYTE4;

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    // Accumulate output as we go
    final T NO_OUTPUT = fst.outputs.getNoOutput();
    T output = NO_OUTPUT;
    for(int i=0;i<input.length;i++) {
      if (fst.findTargetArc(input.ints[input.offset + i], arc, arc) == null) {
        return null;
      } else if (arc.output != NO_OUTPUT) {
        output = fst.outputs.add(output, arc.output);
      }
    }

    if (fst.findTargetArc(FST.END_LABEL, arc, arc) == null) {
      return null;
    } else if (arc.output != NO_OUTPUT) {
      return fst.outputs.add(output, arc.output);
    } else {
      return output;
    }
  }

  /** Logically casts input to UTF32 ints then looks up the output
   *  or null if the input is not accepted.  FST must be
   *  INPUT_TYPE.BYTE4.  */
  public static<T> T get(FST<T> fst, char[] input, int offset, int length) throws IOException {
    assert fst.inputType == FST.INPUT_TYPE.BYTE4;

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    int charIdx = offset;
    final int charLimit = offset + length;

    // Accumulate output as we go
    final T NO_OUTPUT = fst.outputs.getNoOutput();
    T output = NO_OUTPUT;
    while(charIdx < charLimit) {
      final int utf32 = Character.codePointAt(input, charIdx);
      charIdx += Character.charCount(utf32);

      if (fst.findTargetArc(utf32, arc, arc) == null) {
        return null;
      } else if (arc.output != NO_OUTPUT) {
        output = fst.outputs.add(output, arc.output);
      }
    }

    if (fst.findTargetArc(FST.END_LABEL, arc, arc) == null) {
      return null;
    } else if (arc.output != NO_OUTPUT) {
      return fst.outputs.add(output, arc.output);
    } else {
      return output;
    }
  }


  /** Logically casts input to UTF32 ints then looks up the output
   *  or null if the input is not accepted.  FST must be
   *  INPUT_TYPE.BYTE4.  */
  public static<T> T get(FST<T> fst, CharSequence input) throws IOException {
    assert fst.inputType == FST.INPUT_TYPE.BYTE4;
    
    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    int charIdx = 0;
    final int charLimit = input.length();

    // Accumulate output as we go
    final T NO_OUTPUT = fst.outputs.getNoOutput();
    T output = NO_OUTPUT;

    while(charIdx < charLimit) {
      final int utf32 = Character.codePointAt(input, charIdx);
      charIdx += Character.charCount(utf32);

      if (fst.findTargetArc(utf32, arc, arc) == null) {
        return null;
      } else if (arc.output != NO_OUTPUT) {
        output = fst.outputs.add(output, arc.output);
      }
    }

    if (fst.findTargetArc(FST.END_LABEL, arc, arc) == null) {
      return null;
    } else if (arc.output != NO_OUTPUT) {
      return fst.outputs.add(output, arc.output);
    } else {
      return output;
    }
  }

  /** Looks up the output for this input, or null if the
   *  input is not accepted */
  public static<T> T get(FST<T> fst, BytesRef input) throws IOException {
    assert fst.inputType == FST.INPUT_TYPE.BYTE1;

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    // Accumulate output as we go
    final T NO_OUTPUT = fst.outputs.getNoOutput();
    T output = NO_OUTPUT;
    for(int i=0;i<input.length;i++) {
      if (fst.findTargetArc(input.bytes[i+input.offset] & 0xFF, arc, arc) == null) {
        return null;
      } else if (arc.output != NO_OUTPUT) {
        output = fst.outputs.add(output, arc.output);
      }
    }

    if (fst.findTargetArc(FST.END_LABEL, arc, arc) == null) {
      return null;
    } else if (arc.output != NO_OUTPUT) {
      return fst.outputs.add(output, arc.output);
    } else {
      return output;
    }
  }


  // NOTE: this consumes alot of RAM!
  // arcs w/ NEXT opto are in blue
  /*
    eg:
      PrintStream ps = new PrintStream("out.dot");
      fst.toDot(ps);
      ps.close();
      System.out.println("SAVED out.dot");
      
    then dot -Tpng out.dot > /x/tmp/out.png
  */

  public static<T> void toDot(FST<T> fst, PrintStream out) throws IOException {
    
    final FST.Arc<T> startArc = fst.getFirstArc(new FST.Arc<T>());

    final List<FST.Arc<T>> queue = new ArrayList<FST.Arc<T>>();
    queue.add(startArc);

    final Set<Integer> seen = new HashSet<Integer>();
    seen.add(startArc.target);
    
    out.println("digraph FST {");
    out.println("  rankdir = LR;");
    //out.println("  " + startNode + " [shape=circle label=" + startNode + "];");
    out.println("  " + startArc.target + " [label=\"\" shape=circle];");
    out.println("  initial [shape=point color=white label=\"\"];");
    out.println("  initial -> " + startArc.target);

    final T NO_OUTPUT = fst.outputs.getNoOutput();

    while(queue.size() != 0) {
      FST.Arc<T> arc = queue.get(queue.size()-1);
      queue.remove(queue.size()-1);
      //System.out.println("dot cycle target=" + arc.target);

      if (fst.targetHasArcs(arc)) {

        // scan all arcs
        final int node = arc.target;
        fst.readFirstTargetArc(arc, arc);
        while(true) {

          //System.out.println("  cycle label=" + arc.label + " (" + (char) arc.label + ") target=" + arc.target);
          if (!seen.contains(arc.target)) {
            final String shape;
            if (arc.target == -1) {
              shape = "doublecircle";
            } else {
              shape = "circle";
            }
            out.println("  " + arc.target + " [shape=" + shape + "];");
            seen.add(arc.target);
            queue.add(new FST.Arc<T>().copyFrom(arc));
            //System.out.println("    new!");
          }
          String outs;
          if (arc.output != NO_OUTPUT) {
            outs = "/" + fst.outputs.outputToString(arc.output);
          } else {
            outs = "";
          }
          final char cl;
          if (arc.label == FST.END_LABEL) {
            cl = '~';
          } else {
            cl = (char) arc.label;
          }
          out.println("  " + node + " -> " + arc.target + " [label=\"" + cl + outs + "\"]");
          //if (arc.flag(FST.BIT_TARGET_NEXT)) {
          //out.print(" color=blue");
          //}
          //out.println("];");
        
          if (arc.isLast()) {
            break;
          } else {
            fst.readNextArc(arc);
          }
        }
      }
    }
    out.println("}");
  }
}
