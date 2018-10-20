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
package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.LimitedFiniteStringsIterator;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Util;

/**
 * Concatenates/Joins every incoming token with a separator into one output token for every path through the
 * token stream (which is a graph).  In simple cases this yields one token, but in the presence of any tokens with
 * a zero positionIncrmeent (e.g. synonyms) it will be more.  This filter uses the token bytes, position increment,
 * and position length of the incoming stream.  Other attributes are not used or manipulated.
 *
 * @lucene.experimental
 */
public final class ConcatenateGraphFilter extends TokenStream {

  /*
   * Token stream which converts a provided token stream to an automaton.
   * The accepted strings enumeration from the automaton are available through the
   * {@link org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute} attribute
   * The token stream uses a {@link org.apache.lucene.analysis.tokenattributes.PayloadAttribute} to store
   * a completion's payload (see {@link ConcatenateGraphFilter#setPayload(org.apache.lucene.util.BytesRef)})
   */

  /**
   * Represents the separation between tokens, if
   * <code>preserveSep</code> is <code>true</code>.
   */
  public final static int SEP_LABEL = TokenStreamToAutomaton.POS_SEP;
  public final static int DEFAULT_MAX_GRAPH_EXPANSIONS = Operations.DEFAULT_MAX_DETERMINIZED_STATES;
  public final static boolean DEFAULT_PRESERVE_SEP = true;
  public final static boolean DEFAULT_PRESERVE_POSITION_INCREMENTS = true;

  private final BytesRefBuilderTermAttribute bytesAtt = addAttribute(BytesRefBuilderTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  private final TokenStream inputTokenStream;
  private final boolean preserveSep;
  private final boolean preservePositionIncrements;
  private final int maxGraphExpansions;

  private LimitedFiniteStringsIterator finiteStrings;
  private CharTermAttribute charTermAttribute;
  private boolean wasReset = false;
  private int endOffset;

  /**
   * Creates a token stream to convert <code>input</code> to a token stream
   * of accepted strings by its token stream graph.
   * <p>
   * This constructor uses the default settings of the constants in this class.
   */
  public ConcatenateGraphFilter(TokenStream inputTokenStream) {
    this(inputTokenStream, DEFAULT_PRESERVE_SEP, DEFAULT_PRESERVE_POSITION_INCREMENTS, DEFAULT_MAX_GRAPH_EXPANSIONS);
  }

  /**
   * Creates a token stream to convert <code>input</code> to a token stream
   * of accepted strings by its token stream graph.
   *
   * @param inputTokenStream The input/incoming TokenStream
   * @param preserveSep Whether {@link #SEP_LABEL} should separate the input tokens in the concatenated token
   * @param preservePositionIncrements Whether to add an empty token for missing positions.
   *                                   The effect is a consecutive {@link #SEP_LABEL}.
   *                                   When false, it's as if there were no missing positions
   *                                     (we pretend the surrounding tokens were adjacent).
   * @param maxGraphExpansions If the tokenStream graph has more than this many possible paths through, then we'll throw
   *                           {@link TooComplexToDeterminizeException} to preserve the stability and memory of the
   *                           machine.
   * @throws TooComplexToDeterminizeException if the tokenStream graph has more than {@code maxGraphExpansions}
   *         expansions
   *
   */
  public ConcatenateGraphFilter(TokenStream inputTokenStream, boolean preserveSep, boolean preservePositionIncrements, int maxGraphExpansions) {
    // Don't call the super(input) ctor - this is a true delegate and has a new attribute source since we consume
    // the input stream entirely in the first call to incrementToken
    this.inputTokenStream = inputTokenStream;
    this.preserveSep = preserveSep;
    this.preservePositionIncrements = preservePositionIncrements;
    this.maxGraphExpansions = maxGraphExpansions;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    // we only capture this if we really need it to save the UTF-8 to UTF-16 conversion
    charTermAttribute = getAttribute(CharTermAttribute.class); // may return null
    wasReset = true;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (finiteStrings == null) {
      if (wasReset == false) {
        throw new IllegalStateException("reset() missing before incrementToken");
      }
      // lazy init/consume
      Automaton automaton = toAutomaton(); // calls reset(), incrementToken() repeatedly, and end() on inputTokenStream
      finiteStrings = new LimitedFiniteStringsIterator(automaton, maxGraphExpansions);
      //note: would be nice to know the startOffset but toAutomaton doesn't capture it.  We'll assume 0
      endOffset = inputTokenStream.getAttribute(OffsetAttribute.class).endOffset();
    }

    IntsRef string = finiteStrings.next();
    if (string == null) {
      return false;
    }

    clearAttributes();

    if (finiteStrings.size() > 1) { // if number of iterated strings so far is more than one...
      posIncrAtt.setPositionIncrement(0); // stacked
    }

    offsetAtt.setOffset(0, endOffset);

    Util.toBytesRef(string, bytesAtt.builder()); // now we have UTF-8
    if (charTermAttribute != null) {
      charTermAttribute.setLength(0);
      charTermAttribute.append(bytesAtt.toUTF16());
    }

    return true;
  }

  @Override
  public void end() throws IOException {
    super.end();
    if (finiteStrings == null) { // thus inputTokenStream hasn't yet received end()
      inputTokenStream.end(); // the input TS may really want to see "end()" called even if incrementToken hasn't.
    } // else we already eagerly consumed inputTokenStream including end()
    if (endOffset != -1) {
      offsetAtt.setOffset(0, endOffset);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    //delegate lifecycle.  Note toAutomaton does not close the stream
    inputTokenStream.close();
    finiteStrings = null;
    wasReset = false;//reset
    endOffset = -1;//reset
  }

  /**
   * Converts the tokenStream to an automaton, treating the transition labels as utf-8.  Does *not* close it.
   */
  public Automaton toAutomaton() throws IOException {
    return toAutomaton(false);
  }

  /**
   * Converts the tokenStream to an automaton.  Does *not* close it.
   */
  public Automaton toAutomaton(boolean unicodeAware) throws IOException {
    // TODO refactor this
    // maybe we could hook up a modified automaton from TermAutomatonQuery here?

    // Create corresponding automaton: labels are bytes
    // from each analyzed token, with byte 0 used as
    // separator between tokens:
    final TokenStreamToAutomaton tsta;
    if (preserveSep) {
      tsta = new EscapingTokenStreamToAutomaton(SEP_LABEL);
    } else {
      // When we're not preserving sep, we don't steal 0xff
      // byte, so we don't need to do any escaping:
      tsta = new TokenStreamToAutomaton();
    }
    tsta.setPreservePositionIncrements(preservePositionIncrements);
    tsta.setUnicodeArcs(unicodeAware);

    Automaton automaton = tsta.toAutomaton(inputTokenStream);

    // TODO: we can optimize this somewhat by determinizing
    // while we convert
    automaton = replaceSep(automaton, preserveSep, SEP_LABEL);
    // This automaton should not blow up during determinize:
    return Operations.determinize(automaton, maxGraphExpansions);
  }

  /**
   * Just escapes the {@link #SEP_LABEL} byte with an extra.
   */
  private static final class EscapingTokenStreamToAutomaton extends TokenStreamToAutomaton {

    final BytesRefBuilder spare = new BytesRefBuilder();
    final byte sepLabel;

    public EscapingTokenStreamToAutomaton(int sepLabel) {
      assert sepLabel <= Byte.MAX_VALUE;
      this.sepLabel = (byte) sepLabel;
    }

    @Override
    protected BytesRef changeToken(BytesRef in) {
      int upto = 0;
      for (int i = 0; i < in.length; i++) {
        byte b = in.bytes[in.offset + i];
        if (b == sepLabel) {
          spare.grow(upto + 2);
          spare.setByteAt(upto++, sepLabel);
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
    int[] topoSortStates = Operations.topoSortStates(a);
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

  /**
   * Attribute providing access to the term builder and UTF-16 conversion
   * @lucene.internal
   */
  public interface BytesRefBuilderTermAttribute extends TermToBytesRefAttribute {
    /**
     * Returns the builder from which the term is derived.
     */
    BytesRefBuilder builder();

    /**
     * Returns the term represented as UTF-16
     */
    CharSequence toUTF16();
  }

  /**
   * Implementation of {@link BytesRefBuilderTermAttribute}
   * @lucene.internal
   */
  public static final class BytesRefBuilderTermAttributeImpl extends AttributeImpl implements BytesRefBuilderTermAttribute, TermToBytesRefAttribute {
    private final BytesRefBuilder bytes = new BytesRefBuilder();
    private transient CharsRefBuilder charsRef;

    /**
     * Sole constructor
     * no-op
     */
    public BytesRefBuilderTermAttributeImpl() {
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
      BytesRefBuilderTermAttributeImpl other = (BytesRefBuilderTermAttributeImpl) target;
      other.bytes.copyBytes(bytes);
    }

    @Override
    public AttributeImpl clone() {
      BytesRefBuilderTermAttributeImpl other = new BytesRefBuilderTermAttributeImpl();
      copyTo(other);
      return other;
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(TermToBytesRefAttribute.class, "bytes", getBytesRef());
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
