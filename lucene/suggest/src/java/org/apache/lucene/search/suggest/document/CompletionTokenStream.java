package org.apache.lucene.search.suggest.document;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Util;

import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.DEFAULT_MAX_GRAPH_EXPANSIONS;
import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.DEFAULT_PRESERVE_POSITION_INCREMENTS;
import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.DEFAULT_PRESERVE_SEP;
import static org.apache.lucene.search.suggest.document.CompletionAnalyzer.SEP_LABEL;

/**
 * Token stream which converts a provided token stream to an automaton.
 * The accepted strings enumeration from the automaton are available through the
 * {@link org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute} attribute
 * The token stream uses a {@link org.apache.lucene.analysis.tokenattributes.PayloadAttribute} to store
 * a completion's payload (see {@link CompletionTokenStream#setPayload(org.apache.lucene.util.BytesRef)})
 *
 */
final class CompletionTokenStream extends TokenStream {

  private final PayloadAttribute payloadAttr = addAttribute(PayloadAttribute.class);
  private final PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
  private final ByteTermAttribute bytesAtt = addAttribute(ByteTermAttribute.class);

  private final TokenStream input;
  private final boolean preserveSep;
  private final boolean preservePositionIncrements;
  private final int sepLabel;
  private final int maxGraphExpansions;

  private BytesRef payload;
  private Iterator<IntsRef> finiteStrings;
  private int posInc = -1;
  private CharTermAttribute charTermAttribute;

  /**
   * Creates a token stream to convert <code>input</code> to a token stream
   * of accepted strings by its automaton.
   * <p>
   * The token stream <code>input</code> is converted to an automaton
   * with the default settings of {@link org.apache.lucene.search.suggest.document.CompletionAnalyzer}
   */
  public CompletionTokenStream(TokenStream input) {
    this(input, DEFAULT_PRESERVE_SEP, DEFAULT_PRESERVE_POSITION_INCREMENTS, SEP_LABEL, DEFAULT_MAX_GRAPH_EXPANSIONS);
  }

  CompletionTokenStream(TokenStream input, boolean preserveSep, boolean preservePositionIncrements, int sepLabel, int maxGraphExpansions) {
    // Don't call the super(input) ctor - this is a true delegate and has a new attribute source since we consume
    // the input stream entirely in toFiniteStrings(input)
    this.input = input;
    this.preserveSep = preserveSep;
    this.preservePositionIncrements = preservePositionIncrements;
    this.sepLabel = sepLabel;
    this.maxGraphExpansions = maxGraphExpansions;
  }

  /**
   * Returns a separator label that is reserved for the payload
   * in {@link CompletionTokenStream#setPayload(org.apache.lucene.util.BytesRef)}
   */
  public int sepLabel() {
    return sepLabel;
  }

  /**
   * Sets a payload available throughout successive token stream enumeration
   */
  public void setPayload(BytesRef payload) {
    this.payload = payload;
  }

  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    if (finiteStrings == null) {
      //TODO: make this return a Iterator<IntsRef> instead?
      Automaton automaton = toAutomaton(input);
      Set<IntsRef> strings = Operations.getFiniteStrings(automaton, maxGraphExpansions);

      posInc = strings.size();
      finiteStrings = strings.iterator();
    }
    if (finiteStrings.hasNext()) {
      posAttr.setPositionIncrement(posInc);
      /*
       * this posInc encodes the number of paths that this surface form
       * produced. Multi Fields have the same surface form and therefore sum up
       */
      posInc = 0;
      Util.toBytesRef(finiteStrings.next(), bytesAtt.builder()); // now we have UTF-8
      if (charTermAttribute != null) {
        charTermAttribute.setLength(0);
        charTermAttribute.append(bytesAtt.toUTF16());
      }
      if (payload != null) {
        payloadAttr.setPayload(this.payload);
      }
      return true;
    }

    return false;
  }

  @Override
  public void end() throws IOException {
    super.end();
    if (posInc == -1) {
      input.end();
    }
  }

  @Override
  public void close() throws IOException {
    if (posInc == -1) {
      input.close();
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    if (hasAttribute(CharTermAttribute.class)) {
      // we only create this if we really need it to safe the UTF-8 to UTF-16 conversion
      charTermAttribute = getAttribute(CharTermAttribute.class);
    }
    finiteStrings = null;
    posInc = -1;
  }

  /**
   * Converts <code>tokenStream</code> to an automaton
   */
  public Automaton toAutomaton(TokenStream tokenStream) throws IOException {
    // TODO refactor this
    // maybe we could hook up a modified automaton from TermAutomatonQuery here?
    Automaton automaton = null;
    try {
      // Create corresponding automaton: labels are bytes
      // from each analyzed token, with byte 0 used as
      // separator between tokens:
      final TokenStreamToAutomaton tsta;
      if (preserveSep) {
        tsta = new EscapingTokenStreamToAutomaton((char) SEP_LABEL);
      } else {
        // When we're not preserving sep, we don't steal 0xff
        // byte, so we don't need to do any escaping:
        tsta = new TokenStreamToAutomaton();
      }
      tsta.setPreservePositionIncrements(preservePositionIncrements);

      automaton = tsta.toAutomaton(tokenStream);
    } finally {
      IOUtils.closeWhileHandlingException(tokenStream);
    }

    // TODO: we can optimize this somewhat by determinizing
    // while we convert
    automaton = replaceSep(automaton, preserveSep, SEP_LABEL);
    // This automaton should not blow up during determinize:
    return Operations.determinize(automaton, maxGraphExpansions);
  }

  /**
   * Just escapes the 0xff byte (which we still for SEP).
   */
  private static final class EscapingTokenStreamToAutomaton extends TokenStreamToAutomaton {

    final BytesRefBuilder spare = new BytesRefBuilder();
    private char sepLabel;

    public EscapingTokenStreamToAutomaton(char sepLabel) {
      this.sepLabel = sepLabel;
    }

    @Override
    protected BytesRef changeToken(BytesRef in) {
      int upto = 0;
      for (int i = 0; i < in.length; i++) {
        byte b = in.bytes[in.offset + i];
        if (b == (byte) sepLabel) {
          spare.grow(upto + 2);
          spare.setByteAt(upto++, (byte) sepLabel);
          spare.setByteAt(upto++, b);
        } else {
          spare.grow(upto + 1);
          spare.setByteAt(upto++, b);
        }
      }
      spare.setLength(upto);
      return spare.get();
    }
  }

  // Replaces SEP with epsilon or remaps them if
  // we were asked to preserve them:
  private static Automaton replaceSep(Automaton a, boolean preserveSep, int sepLabel) {

    Automaton result = new Automaton();

    // Copy all states over
    int numStates = a.getNumStates();
    for (int s = 0; s < numStates; s++) {
      result.createState();
      result.setAccept(s, a.isAccept(s));
    }

    // Go in reverse topo sort so we know we only have to
    // make one pass:
    Transition t = new Transition();
    int[] topoSortStates = topoSortStates(a);
    for (int i = 0; i < topoSortStates.length; i++) {
      int state = topoSortStates[topoSortStates.length - 1 - i];
      int count = a.initTransition(state, t);
      for (int j = 0; j < count; j++) {
        a.getNextTransition(t);
        if (t.min == TokenStreamToAutomaton.POS_SEP) {
          assert t.max == TokenStreamToAutomaton.POS_SEP;
          if (preserveSep) {
            // Remap to SEP_LABEL:
            result.addTransition(state, t.dest, sepLabel);
          } else {
            result.addEpsilon(state, t.dest);
          }
        } else if (t.min == TokenStreamToAutomaton.HOLE) {
          assert t.max == TokenStreamToAutomaton.HOLE;

          // Just remove the hole: there will then be two
          // SEP tokens next to each other, which will only
          // match another hole at search time.  Note that
          // it will also match an empty-string token ... if
          // that's somehow a problem we can always map HOLE
          // to a dedicated byte (and escape it in the
          // input).
          result.addEpsilon(state, t.dest);
        } else {
          result.addTransition(state, t.dest, t.min, t.max);
        }
      }
    }

    result.finishState();

    return result;
  }

  private static int[] topoSortStates(Automaton a) {
    int[] states = new int[a.getNumStates()];
    final Set<Integer> visited = new HashSet<>();
    final LinkedList<Integer> worklist = new LinkedList<>();
    worklist.add(0);
    visited.add(0);
    int upto = 0;
    states[upto] = 0;
    upto++;
    Transition t = new Transition();
    while (worklist.size() > 0) {
      int s = worklist.removeFirst();
      int count = a.initTransition(s, t);
      for (int i = 0; i < count; i++) {
        a.getNextTransition(t);
        if (!visited.contains(t.dest)) {
          visited.add(t.dest);
          worklist.add(t.dest);
          states[upto++] = t.dest;
        }
      }
    }
    return states;
  }

  public interface ByteTermAttribute extends TermToBytesRefAttribute {
    // marker interface

    /**
     * Return the builder from which the term is derived.
     */
    public BytesRefBuilder builder();

    public CharSequence toUTF16();
  }

  public static final class ByteTermAttributeImpl extends AttributeImpl implements ByteTermAttribute, TermToBytesRefAttribute {
    private final BytesRefBuilder bytes = new BytesRefBuilder();
    private CharsRefBuilder charsRef;

    @Override
    public void fillBytesRef() {
      // does nothing - we change in place
    }

    @Override
    public BytesRefBuilder builder() {
      return bytes;
    }

    @Override
    public BytesRef getBytesRef() {
      return bytes.get();
    }

    @Override
    public void clear() {
      bytes.clear();
    }

    @Override
    public void copyTo(AttributeImpl target) {
      ByteTermAttributeImpl other = (ByteTermAttributeImpl) target;
      other.bytes.copyBytes(bytes);
    }

    @Override
    public CharSequence toUTF16() {
      if (charsRef == null) {
        charsRef = new CharsRefBuilder();
      }
      charsRef.copyUTF8Bytes(getBytesRef());
      return charsRef.get();
    }
  }
}
