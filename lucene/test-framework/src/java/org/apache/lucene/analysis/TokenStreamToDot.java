package org.apache.lucene.analysis;

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

import java.io.PrintWriter;
import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;

/** Consumes a TokenStream and outputs the dot (graphviz) string (graph). */
public class TokenStreamToDot {

  private final TokenStream in;
  private final CharTermAttribute termAtt;
  private final PositionIncrementAttribute posIncAtt;
  private final PositionLengthAttribute posLengthAtt;
  private final OffsetAttribute offsetAtt;
  private final String inputText;
  protected final PrintWriter out;

  /** If inputText is non-null, and the TokenStream has
   *  offsets, we include the surface form in each arc's
   *  label. */
  public TokenStreamToDot(String inputText, TokenStream in, PrintWriter out) {
    this.in = in;
    this.out = out;
    this.inputText = inputText;
    termAtt = in.addAttribute(CharTermAttribute.class);
    posIncAtt = in.addAttribute(PositionIncrementAttribute.class);
    posLengthAtt = in.addAttribute(PositionLengthAttribute.class);
    if (in.hasAttribute(OffsetAttribute.class)) {
      offsetAtt = in.addAttribute(OffsetAttribute.class);
    } else {
      offsetAtt = null;
    }
  }

  public void toDot() throws IOException {
    in.reset();
    writeHeader();

    // TODO: is there some way to tell dot that it should
    // make the "main path" a straight line and have the
    // non-sausage arcs not affect node placement...

    int pos = -1;
    int lastEndPos = -1;
    while (in.incrementToken()) {
      final boolean isFirst = pos == -1;
      int posInc = posIncAtt.getPositionIncrement();
      if (isFirst && posInc == 0) {
        // TODO: hmm are TS's still allowed to do this...?
        System.err.println("WARNING: first posInc was 0; correcting to 1");
        posInc = 1;
      }

      if (posInc > 0) {
        // New node:
        pos += posInc;
        writeNode(pos, Integer.toString(pos));
      }

      if (posInc > 1) {
        // Gap!
        writeArc(lastEndPos, pos, null, "dotted");
      }

      if (isFirst) {
        writeNode(-1, null);
        writeArc(-1, pos, null, null);
      }

      String arcLabel = termAtt.toString();
      if (offsetAtt != null) {
        final int startOffset = offsetAtt.startOffset();
        final int endOffset = offsetAtt.endOffset();
        //System.out.println("start=" + startOffset + " end=" + endOffset + " len=" + inputText.length());
        if (inputText != null) {
          arcLabel += " / " + inputText.substring(startOffset, endOffset);
        } else {
          arcLabel += " / " + startOffset + "-" + endOffset;
        }
      }

      writeArc(pos, pos + posLengthAtt.getPositionLength(), arcLabel, null);
      lastEndPos = pos + posLengthAtt.getPositionLength();
    }

    in.end();

    if (lastEndPos != -1) {
      // TODO: should we output any final text (from end
      // offsets) on this arc...?
      writeNode(-2, null);
      writeArc(lastEndPos, -2, null, null);
    }

    writeTrailer();
  }

  protected void writeArc(int fromNode, int toNode, String label, String style) {
    out.print("  " + fromNode + " -> " + toNode + " [");
    if (label != null) {
      out.print(" label=\"" + label + "\"");
    }
    if (style != null) {
      out.print(" style=\"" + style + "\"");
    }
    out.println("]");
  }

  protected void writeNode(int name, String label) {
    out.print("  " + name);
    if (label != null) {
      out.print(" [label=\"" + label + "\"]");
    } else {
      out.print(" [shape=point color=white]");
    }
    out.println();
  }

  private final static String FONT_NAME = "Helvetica";

  /** Override to customize. */
  protected void writeHeader() {
    out.println("digraph tokens {");
    out.println("  graph [ fontsize=30 labelloc=\"t\" label=\"\" splines=true overlap=false rankdir = \"LR\" ];");
    out.println("  // A2 paper size");
    out.println("  size = \"34.4,16.5\";");
    //out.println("  // try to fill paper");
    //out.println("  ratio = fill;");
    out.println("  edge [ fontname=\"" + FONT_NAME + "\" fontcolor=\"red\" color=\"#606060\" ]");
    out.println("  node [ style=\"filled\" fillcolor=\"#e8e8f0\" shape=\"Mrecord\" fontname=\"" + FONT_NAME + "\" ]");
    out.println();
  }

  /** Override to customize. */
  protected void writeTrailer() {
    out.println("}");
  }
}
