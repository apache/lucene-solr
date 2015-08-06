package org.apache.lucene.analysis.stages;

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

import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

// nocommit does not do keepOrig

// nocommit should we ... allow recursing on ourselves?  ie
// so sometimes an output could be parsed against an input
// rule?

/** Synonym filter, that improves on existing one: it can
 *  handle any graph input, and it can create new positions
 *  (e.g., dns -> domain name service). */

public class SynonymFilterStage extends Stage {

  // We change the term:
  private final TermAttribute termAttIn;
  private final TermAttribute termAttOut;

  // We change the to/from:
  private final ArcAttribute arcAttIn;
  private final ArcAttribute arcAttOut;

  // We set offsets:
  private final OffsetAttribute offsetAttIn;
  private final OffsetAttribute offsetAttOut;

  // Used only inside addMatch
  private final BytesRef scratchBytes = new BytesRef();
  private final CharsRef scratchChars = new CharsRef();

  private final LinkedList<OutputToken> pendingOutputs = new LinkedList<OutputToken>();

  private final List<PartialMatch> pending = new ArrayList<PartialMatch>();

  private final SynonymMap synonyms;

  private final FST.Arc<BytesRef> scratchArc;

  private final FST<BytesRef> fst;

  private final boolean ignoreCase;

  private final FST.BytesReader fstReader;

  /** Used to decode outputs */
  private final ByteArrayDataInput bytesReader = new ByteArrayDataInput();

  private static class PartialMatch {
    final int fromNode;
    final int toNode;
    final int startOffset;
    final FST.Arc<BytesRef> fstNode;
    final BytesRef output;

    public PartialMatch(int startOffset, int fromNode, int toNode, FST.Arc<BytesRef> fstNode, BytesRef output) {
      this.startOffset = startOffset;
      this.fromNode = fromNode;
      this.toNode = toNode;
      this.fstNode = fstNode;
      this.output = output;
    }
  }

  private static class OutputToken {
    final String text;
    final int startOffset;
    final int endOffset;
    final int fromNode;
    final int toNode;

    public OutputToken(String text, int startOffset, int endOffset, int fromNode, int toNode) {
      this.text = text;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.fromNode = fromNode;
      this.toNode = toNode;
    }
  }

  public SynonymFilterStage(Stage prevStage, SynonymMap synonyms, boolean ignoreCase) {
    super(prevStage);
    termAttIn = prevStage.get(TermAttribute.class);
    termAttOut = create(TermAttribute.class);
    arcAttIn = prevStage.get(ArcAttribute.class);
    arcAttOut = create(ArcAttribute.class);
    offsetAttIn = prevStage.get(OffsetAttribute.class);
    offsetAttOut = create(OffsetAttribute.class);
    this.synonyms = synonyms;
    this.ignoreCase = ignoreCase;
    fst = synonyms.fst;
    fstReader = fst.getBytesReader();
    if (fst == null) {
      throw new IllegalArgumentException("fst must be non-null");
    }
    scratchArc = new FST.Arc<BytesRef>();
  }

  // nocommit we need reset!  make test that fails if only
  // partial consume
  @Override
  public void reset(Reader reader) {
    super.reset(reader);
    pending.clear();
    pendingOutputs.clear();
  }

  /** Extends all pending paths, and starts new paths,
   *  matching the current token. */
  private void matchOne(PartialMatch match) throws IOException {
    BytesRef output;
    if (match != null) {
      scratchArc.copyFrom(match.fstNode);
      output = match.output;
    } else {
      // Start a new match here:
      fst.getFirstArc(scratchArc);
      output = fst.outputs.getNoOutput();
    }

    int bufferLen = termAttIn.get().length();
    int bufUpto = 0;
    while(bufUpto < bufferLen) {
      final int codePoint = Character.codePointAt(termAttIn.get(), bufUpto);
      if (fst.findTargetArc(ignoreCase ? Character.toLowerCase(codePoint) : codePoint, scratchArc, scratchArc, fstReader) == null) {
        return;
      }

      // Accum the output
      output = fst.outputs.add(output, scratchArc.output);
      bufUpto += Character.charCount(codePoint);
    }

    // Entire token matched; now see if this is a final
    // state:
    if (scratchArc.isFinal()) {
      // A match!
      addMatch(match, fst.outputs.add(output, scratchArc.nextFinalOutput));
    }

    // See if the FST wants to continue matching (ie, needs to
    // see the next input token):
    if (fst.findTargetArc(SynonymMap.WORD_SEPARATOR, scratchArc, scratchArc, fstReader) != null) {
      // More matching is possible -- accum the output (if
      // any) of the WORD_SEP arc and add a new
      // PartialMatch:
      int startOffset;
      int fromNode;
      if (match == null) {
        startOffset = offsetAttIn.startOffset();
        fromNode = arcAttIn.from();
      } else {
        startOffset = match.startOffset;
        fromNode = match.fromNode;
      }

      // Reserve the right to use this node as a future fromNode:
      nodes.mayChange(fromNode);
      //System.out.println("  incr mayChange node=" + fromNode);
      //System.out.println("  add pending to node=" + arcAttIn.to());
      pending.add(new PartialMatch(startOffset,
                                   fromNode,
                                   arcAttIn.to(),
                                   new FST.Arc<BytesRef>().copyFrom(scratchArc),
                                   fst.outputs.add(output, scratchArc.output)));
    }
  }

  /** Records a full match; on the next next() we will feed
   *  output tokens from it. */
  private void addMatch(PartialMatch match, BytesRef bytes) {
    //System.out.println("  add full match!");

    bytesReader.reset(bytes.bytes, bytes.offset, bytes.length);

    final int code = bytesReader.readVInt();
    final boolean keepOrig = (code & 0x1) == 0;
    if (!keepOrig) {
      throw new IllegalArgumentException("this SynonymFilter requires keepOrig = true");
    }

    final int count = code >>> 1;

    for (int outputIDX=0;outputIDX<count;outputIDX++) {
      synonyms.words.get(bytesReader.readVInt(), scratchBytes);
      UnicodeUtil.UTF8toUTF16(scratchBytes, scratchChars);
      assert scratchChars.offset == 0;

      int lastStart = 0;
      int lastNode = match.fromNode;

      for (int chIDX=0;chIDX<=scratchChars.length;chIDX++) {
        if (chIDX == scratchChars.length || scratchChars.chars[chIDX] == SynonymMap.WORD_SEPARATOR) {
          final int outputLen = chIDX - lastStart;

          // Caller is not allowed to have empty string in
          // the output:
          assert outputLen > 0: "output contains empty string: " + scratchChars;

          int toNode;
          if (chIDX == scratchChars.length) {
            toNode = arcAttIn.to();
          } else {
            toNode = nodes.newNode();
            lastNode = toNode;
            lastStart = 1+chIDX;
          }

          // These offsets make sense for "domain name service ->
          // DNS", but for "DNS -> domain name service" it's a
          // little weird because each of the 3 tokens will have
          // the same offsets ... but I don't see how we can do
          // any better?
          //System.out.println("  add output=" + new String(scratchChars.chars, lastStart, outputLen));
          pendingOutputs.add(new OutputToken(new String(scratchChars.chars, lastStart, outputLen),
                                             match.startOffset, offsetAttIn.endOffset(),
                                             lastNode, toNode));
        }
      }
    }
  }

  /** Update all matches for the current token. */
  private void match() throws IOException {
    int from = arcAttIn.from();

    // First extend any existing partial matches:
    int end = pending.size();
    for(int i=0;i<end;i++) {
      PartialMatch match = pending.get(i);
      //System.out.println("  try to extend match ending @ node=" + match.toNode + " vs from=" + from);
      if (match.toNode == from) {
        matchOne(match);
      }
    }

    // Then start any new matches:
    matchOne(null);

    // Prune any dead partial matches:
    int upto = 0;
    for(int i=0;i<pending.size();i++) {
      PartialMatch match = pending.get(i);
      int to = match.toNode;
      if (to != arcAttIn.from() && to != arcAttIn.to() && !nodes.getCanChange(to)) {
        // Prune this path
        //System.out.println("  prune path @ node=" + match.toNode);
        //System.out.println("  decr node=" + match.fromNode);
        nodes.wontChange(match.fromNode);
      } else {
        if (upto < i) {
          pending.set(upto, pending.get(i));
        }
        upto++;
      }
    }

    pending.subList(upto, pending.size()).clear();
  }

  private boolean insertOutputToken() {
    OutputToken token = pendingOutputs.pollFirst();
    if (token != null) {
      //System.out.println("  insert output token! text=" + token.text + " offset=" + token.startOffset + "/" + token.endOffset);
      // We still have outputs pending from previous match:
      termAttOut.set(token.text);
      offsetAttOut.setOffset(token.startOffset, token.endOffset);
      arcAttOut.set(token.fromNode, token.toNode);
      return true;
    } else {
      return false;
    }
  }

  public boolean next() throws IOException {
    //System.out.println("\nSYN.next");
    if (insertOutputToken()) {
      return true;
    }
    //System.out.println("  input.next()");
    if (prevStage.next()) {
      // Extend matches for this new token:
      //System.out.println("  got token=" + termAttIn + " from=" + arcAttIn.from() + " to=" + arcAttIn.to());
      match();
      
      // nocommit copy?
      termAttOut.set(termAttIn.get());

      offsetAttOut.setOffset(offsetAttIn.startOffset(),
                             offsetAttIn.endOffset());
      arcAttOut.set(arcAttIn.from(),
                    arcAttIn.to());
      return true;
    } else {
      // Prune any remaining partial matches:
      for(int i=0;i<pending.size();i++) {
        nodes.wontChange(pending.get(i).fromNode);
      }
      pending.clear();
      return false;
    }
  }
}
