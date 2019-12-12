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

package org.apache.lucene.analysis.synonym;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.FlattenGraphFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.fst.FST;

// TODO: maybe we should resolve token -> wordID then run
// FST on wordIDs, for better perf?
 
// TODO: a more efficient approach would be Aho/Corasick's
// algorithm
// http://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_string_matching_algorithm
// It improves over the current approach here
// because it does not fully re-start matching at every
// token.  For example if one pattern is "a b c x"
// and another is "b c d" and the input is "a b c d", on
// trying to parse "a b c x" but failing when you got to x,
// rather than starting over again your really should
// immediately recognize that "b c d" matches at the next
// input.  I suspect this won't matter that much in
// practice, but it's possible on some set of synonyms it
// will.  We'd have to modify Aho/Corasick to enforce our
// conflict resolving (eg greedy matching) because that algo
// finds all matches.  This really amounts to adding a .*
// closure to the FST and then determinizing it.
//
// Another possible solution is described at http://www.cis.uni-muenchen.de/people/Schulz/Pub/dictle5.ps

/** Applies single- or multi-token synonyms from a {@link SynonymMap}
 *  to an incoming {@link TokenStream}, producing a fully correct graph
 *  output.  This is a replacement for {@link SynonymFilter}, which produces
 *  incorrect graphs for multi-token synonyms.
 *
 *  <p>However, if you use this during indexing, you must follow it with
 *  {@link FlattenGraphFilter} to squash tokens on top of one another
 *  like {@link SynonymFilter}, because the indexer can't directly
 *  consume a graph.  To get fully correct positional queries when your
 *  synonym replacements are multiple tokens, you should instead apply
 *  synonyms using this {@code TokenFilter} at query time and translate
 *  the resulting graph to a {@code TermAutomatonQuery} e.g. using
 *  {@code TokenStreamToTermAutomatonQuery}.
 *
 *  <p><b>NOTE</b>: this cannot consume an incoming graph; results will
 *  be undefined.
 *
 *  @lucene.experimental */

public final class SynonymGraphFilter extends TokenFilter {

  public static final String TYPE_SYNONYM = "SYNONYM";

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  private final SynonymMap synonyms;
  private final boolean ignoreCase;

  private final FST<BytesRef> fst;

  private final FST.BytesReader fstReader;
  private final FST.Arc<BytesRef> scratchArc;
  private final ByteArrayDataInput bytesReader = new ByteArrayDataInput();
  private final BytesRef scratchBytes = new BytesRef();
  private final CharsRefBuilder scratchChars = new CharsRefBuilder();
  private final Queue<BufferedToken> tokenQueue;

  // For testing:
  private int captureCount;


  static class BufferedToken {
    final char[] term;
    final State state;
    int startNode;
    int endNode;
    int startOffset;
    int endOffset;
    int captureIndex = -1;// for debug

    BufferedToken(char[] term, State state, int startNode, int endNode) {
      this.term = term;
      this.state = state;
      this.startNode = startNode;
      this.endNode = endNode;
    }


    @Override
    public String toString(){
      return new String(term);
    }
  }

  private int prevInputStartNode;
  private int prevOutputStartNode;
  private int deltaStartNode;

  // True once the input TokenStream is exhausted:
  private boolean inputFinished;

  private Queue<BufferedToken> lookahead;
  private BufferedToken lastInputToken;

  /**
   * Apply previously built synonyms to incoming tokens.
   * @param input input tokenstream
   * @param synonyms synonym map
   * @param ignoreCase case-folds input for matching with {@link Character#toLowerCase(int)}.
   *                   Note, if you set this to true, it's your responsibility to lowercase
   *                   the input entries when you create the {@link SynonymMap}
   */
  public SynonymGraphFilter(TokenStream input, SynonymMap synonyms, boolean ignoreCase) {
    super(input);
    this.synonyms = synonyms;
    this.fst = synonyms.fst;
    if (fst == null) {
      throw new IllegalArgumentException("fst must be non-null");
    }
    this.fstReader = fst.getBytesReader();
    scratchArc = new FST.Arc<>();
    this.ignoreCase = ignoreCase;
    this.tokenQueue = new ArrayDeque<>();
    this.lookahead = new ArrayDeque<>();
  }

  @Override
  public boolean incrementToken() throws IOException {
      if (!tokenQueue.isEmpty()) {
        BufferedToken token = tokenQueue.poll();
        releaseBufferedToken(token);
        return true;
      }
      if (inputFinished && lookahead.isEmpty()) {
        return false;
      }
      if (parse()) {
        releaseCurrentToken();
        return true;
      }
      return releaseBufferedToken();
  }

  private boolean releaseBufferedToken() {
    if (tokenQueue.isEmpty()) {
      return false;
    }
    BufferedToken token = tokenQueue.poll();
    releaseBufferedToken(token);
    return true;
  }

  private void releaseCurrentToken() {
    prevInputStartNode += posIncrAtt.getPositionIncrement();
    int startNode = prevInputStartNode + deltaStartNode;
    posIncrAtt.setPositionIncrement(startNode - prevOutputStartNode);
    prevOutputStartNode = startNode;
  }

  private void releaseBufferedToken(BufferedToken token) {
    if (token.state != null) {
      restoreState(token.state);
    } else {
      clearAttributes();
      termAtt.copyBuffer(token.term, 0, token.term.length);
      // We better have a match already:
      offsetAtt.setOffset(token.startOffset, token.endOffset);
      typeAtt.setType(TYPE_SYNONYM);
    }

    posIncrAtt.setPositionIncrement(token.startNode - prevOutputStartNode);
    prevOutputStartNode = token.startNode;
    posLenAtt.setPositionLength(token.endNode - token.startNode);
    // System.out.println("releaseBufferedToken term: " + termAtt.toString() + ",posIncrAtt:" + posIncrAtt.getPositionIncrement() + ", token.startNode:" + token.startNode + ", token.endNode:" + token.endNode + ", tokenInQueue:" + tokenQueue.size());
  }

  // return new pendingOutput, return null if doesn't match.
  private BytesRef termMatch(char[] term, BytesRef pendingOutput, int bufferLen) throws  IOException{
    int bufUpto = 0;
    while (bufUpto < bufferLen) {
      final int codePoint = Character.codePointAt(term, bufUpto, bufferLen);
      if (fst.findTargetArc(ignoreCase ? Character.toLowerCase(codePoint) : codePoint, scratchArc, scratchArc, fstReader) == null) {
        return null;
      }
      // Accum the output
      pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output());
      bufUpto += Character.charCount(codePoint);
    }
    return pendingOutput;
  }

  /** Scans the next input token(s) to see if a synonym matches. returns true if the token is not captured in Queue.*/
  private boolean parse() throws  IOException {
    BytesRef matchOutput = null;
    BytesRef pendingOutput = fst.outputs.getNoOutput();
    fst.getFirstArc(scratchArc);
    assert scratchArc.output() == fst.outputs.getNoOutput();
    int lookaheadIndex = 0;
    int matchedLength = 0;
    Iterator<BufferedToken> it = lookahead.iterator();
    int lastEndNode = -1;
    int lastEndOffset = 0;
    int lastEndNode2 = 0;
    boolean restored = false;
    while(true){
      BufferedToken token = null;
      if (it.hasNext()){
        token = it.next();
        if (lastEndNode != -1 && lastEndNode != token.startNode){
          // it's not consecutive
          break;
        }
        pendingOutput = termMatch(token.term, pendingOutput, token.term.length);
      } else {
        if (inputFinished) {
          break;
        }
        if (!restored && lastInputToken != null) {
          // restore state before increase
          restoreState(lastInputToken.state);
        }
        restored = true;
        if (!input.incrementToken()) {
          inputFinished = true;
          break;
        }
        char[] termBuffer = termAtt.buffer();
        pendingOutput = termMatch(termBuffer, pendingOutput, termAtt.length());
        if (pendingOutput == null && lookahead.isEmpty()){
          // no match at the beginning, we can release current token without capture.
          return true;
        }

        token = capture();
        lastInputToken = token;
        lookahead.offer(token);
        if (lastEndNode != -1 && lastEndNode != token.startNode){
          // it's not consecutive
          break;
        }
      }
      lastEndNode = token.endNode;
      if (pendingOutput == null){
        break;
      }

      lookaheadIndex += 1;
      if (scratchArc.isFinal()){
        matchOutput = fst.outputs.add(pendingOutput, scratchArc.nextFinalOutput());
        matchedLength = lookaheadIndex;
        lastEndOffset = token.endOffset;
        lastEndNode2 = token.endNode;
      }

      // See if the FST can continue matching (ie, needs to
      // see the next input token):
      if (fst.findTargetArc(SynonymMap.WORD_SEPARATOR, scratchArc, scratchArc, fstReader) == null) {
        // No further rules can match here; we're done
        // searching for matching rules starting at the
        // current input position.
        break;
      } else {
        // More matching is possible -- accum the output (if
        // any) of the WORD_SEP arc:
        pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output());
      }
    }
    if (lookahead.isEmpty()){
      // input is empty
      return false;
    }
    if (matchOutput != null){
      // There is a match!
      bufferSynonym(lookahead, matchedLength, matchOutput, lastEndOffset, lastEndNode2);
    } else {
      BufferedToken t = lookahead.poll();
      tokenQueue.offer(t);
    }
    return false;
  }

  /** Expands the output graph into the necessary tokens, adding
   *  synonyms as side paths parallel to the input tokens, and
   *  buffers them in the output token buffer. */
  private void bufferSynonym(Queue<BufferedToken> lookahead, int consumed, BytesRef bytes, int lastEndOffset, int lastEndNode){
    assert tokenQueue.isEmpty();
    bytesReader.reset(bytes.bytes, bytes.offset, bytes.length);
    final int code = bytesReader.readVInt();
    final boolean keepOrig = (code & 0x1) == 0;
    // How many synonyms we will insert over this match:
    final int count = code >>> 1;

    BufferedToken firstToken = lookahead.peek();
    int oriLength = lastEndNode- firstToken.startNode;

    List<List<BufferedToken>> paths = new ArrayList<>();
    int lengthExceptHead = 0;
    for(int outputIDX=0;outputIDX<count;outputIDX++) {
      int wordID = bytesReader.readVInt();
      synonyms.words.get(wordID, scratchBytes);
      scratchChars.copyUTF8Bytes(scratchBytes);
      int lastStart = 0;

      List<BufferedToken> path = new ArrayList<>();
      paths.add(path);
      int chEnd = scratchChars.length();
      int curTokenNum = 0;
      for(int chUpto=0; chUpto<=chEnd; chUpto++) {
        if (chUpto == chEnd || scratchChars.charAt(chUpto) == SynonymMap.WORD_SEPARATOR) {
          char[] term = ArrayUtil.copyOfSubArray(scratchChars.chars(), lastStart, chUpto);
          lastStart = 1 + chUpto;

          int startNode;
          int endNode;
          if (curTokenNum != 0) {
            startNode = firstToken.startNode + lengthExceptHead + 1;
            endNode = startNode + 1;
            lengthExceptHead += 1;
          } else {
            startNode = firstToken.startNode;
            endNode = startNode + lengthExceptHead + 1;
          }
          BufferedToken t = new BufferedToken(term, null, startNode, endNode);
          t.endOffset = lastEndOffset;
          t.startOffset = firstToken.startOffset;
          path.add(t);
          curTokenNum += 1;
        }
      }
    }

    List<BufferedToken> path = null;
    if (keepOrig) {
      path =new ArrayList<>();
    }
    BufferedToken token = null;
    for(int i = 0; i < consumed; i++){
      token = lookahead.poll();
      if (keepOrig){
        if (i != 0) {
          // move original tokens to the end
          token.startNode += lengthExceptHead;
        }
        token.endNode += lengthExceptHead;
        path.add(token);
      }
    }
    final BufferedToken lastToken = token;
    if (keepOrig) {
      lengthExceptHead += oriLength - 1;
      paths.add(path);
    }
    int deltaDelta = lengthExceptHead - (oriLength - 1);
    deltaStartNode += deltaDelta;
    if (deltaDelta != 0){
      lookahead.forEach((BufferedToken t) -> {
        t.startNode += deltaDelta;
        t.endNode += deltaDelta;
      });
    }

    int curEndNode = firstToken.startNode + lengthExceptHead + 1;
    paths.forEach((List<BufferedToken> p) -> {
      int size = p.size();
      p.get(size - 1).endNode = curEndNode;

      tokenQueue.offer(p.get(0));
    });
    paths.forEach((List<BufferedToken> p) ->{
      int size = p.size();
      for (int i = 1; i < size; i++) {
        tokenQueue.offer(p.get(i));
      }
    });

  }

  /** Buffers the current input token into lookahead buffer. */
  private BufferedToken capture() {
    prevInputStartNode += posIncrAtt.getPositionIncrement();
    int startNode = prevInputStartNode + deltaStartNode;
    int endNode = startNode + posLenAtt.getPositionLength();
    AttributeSource.State state = captureState();

    char[] term = ArrayUtil.copyOfSubArray(termAtt.buffer(), 0, termAtt.length());
    BufferedToken token = new BufferedToken(
        term,
        state,
        startNode,
        endNode
    );
    token.startOffset = offsetAtt.startOffset();
    token.endOffset = offsetAtt.endOffset();
    token.captureIndex = captureCount;
    captureCount++;
    // System.out.println("capture: " + token.captureIndex + ", term: " + termAtt.toString() + ", term.size: " + termAtt + ", startOffset: " + token.startOffset + ", endOffset: " + token.endOffset + ", startNode: " + token.startNode + ", endNode:" + token.endNode);
    return token;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    prevOutputStartNode = 0;
    prevInputStartNode = 0;
    deltaStartNode = 0;
    captureCount = 0;
    inputFinished = false;
    lastInputToken = null;
    this.tokenQueue.clear();
    this.lookahead.clear();
  }

  public int getCaptureCount(){
    return captureCount;
  }

  int getMaxLookaheadUsed(){
    return 0;
  }
}
