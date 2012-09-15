package org.apache.lucene.search.suggest.analyzing;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.fst.Sort;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.SpecialOperations;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util.MinResult;
import org.apache.lucene.util.fst.Util;

/**
 * Suggester that first analyzes the surface form, adds the
 * analyzed from to a weighted FST, and then does the same
 * thing at lookup time.  This means lookup is based on the
 * analyzed form while suggestions are still the surface
 * form(s).

 * <p>
 * This can result in powerful suggestor functionality.  For
 * example, if you use an analyzer removing stop words, 
 * then the partial text "ghost chr..." could see the
 * suggestion "The Ghost of Christmas Past".  If
 * SynonymFilter is used to map wifi and wireless network to
 * hotspot then the partial text "wirele..." could suggest
 * "wifi router".  Token normalization like stemmers, accent
 * removal, etc., would allow suggestions to ignore such
 * variations.
 *
 * <p>
 * <b>NOTE</b>: Although the {@link TermFreqIterator} API specifies
 * floating point weights, input weights should be whole numbers.
 * Input weights will be cast to a java integer, and any
 * negative, infinite, or NaN values will be rejected.
 * 
 * @lucene.experimental
 */
public class AnalyzingCompletionLookup extends Lookup {
 
  /**
   * FST<Weight,Surface>: 
   *  input is the analyzed form, with a null byte between terms
   *  weights are encoded as costs: (Integer.MAX_VALUE-weight)
   *  surface is the original, unanalyzed form.
   */
  private FST<Pair<Long,BytesRef>> fst = null;
  
  /** 
   * Analyzer that will be used for analyzing suggestions 
   */
  private final Analyzer analyzer;
  
  /** 
   * True if exact match suggestions should always be returned first.
   */
  private final boolean exactFirst;
  
  /** 
   * True if separator between tokens should be preservered.
   */
  private final boolean preserveSep;

  /**
   * Calls {@link #AnalyzingCompletionLookup(Analyzer,int) AnalyzingCompletionLookup(analyzer, EXACT_FIRST | PRESERVE_SEP)}
   */
  public AnalyzingCompletionLookup(Analyzer analyzer) {
    this(analyzer, EXACT_FIRST | PRESERVE_SEP);
  }

  /** Include this flag in the options parameter {@link
   * #AnalyzingCompletionLookup(Analyzer,int)} to always
   * return exact matches first regardless of score.  This
   * has no performance impact but could result in
   * low-quality suggestions. */
  public static final int EXACT_FIRST = 1;

  /** Include this flag in the options parameter {@link
   * #AnalyzingCompletionLookup(Analyzer,int)} to preserve
   * token separators when matching. */
  public static final int PRESERVE_SEP = 2;

  /** Represents the separation between tokens, if
   *  PRESERVE_SEP was specified */
  private static final byte SEP_BYTE = 0;

  /**
   * Creates a new suggester.
   * 
   * @param analyzer Analyzer that will be used for analyzing suggestions.
   * @param options see {@link #EXACT_FIRST}, {@link #PRESERVE_SEP}
   */
  public AnalyzingCompletionLookup(Analyzer analyzer, int options) {
    this.analyzer = analyzer;
    if ((options & ~(EXACT_FIRST | PRESERVE_SEP)) != 0) {
      throw new IllegalArgumentException("options should only contain EXACT_FIRST and PRESERVE_SEP; got " + options);
    }
    this.exactFirst = (options & EXACT_FIRST) != 0;
    this.preserveSep = (options & PRESERVE_SEP) != 0;
  }

  // Replaces SEP with epsilon or remaps them if
  // we were asked to preserve them:
  private void replaceSep(Automaton a) {

    State[] states = a.getNumberedStates();

    // Go in reverse topo sort so we know we only have to
    // make one pass:
    for(int stateNumber=states.length-1;stateNumber >=0;stateNumber--) {
      final State state = states[stateNumber];
      List<Transition> newTransitions = new ArrayList<Transition>();
      for(Transition t : state.getTransitions()) {
        assert t.getMin() == t.getMax();
        if (t.getMin() == TokenStreamToAutomaton.POS_SEP) {
          if (preserveSep) {
            // Remap to SEP_BYTE:
            t = new Transition(SEP_BYTE, t.getDest());
          } else {
            // NOTE: sort of weird because this will grow
            // the transition array we are iterating over,
            // but because we are going in reverse topo sort
            // it will not add any SEP/HOLE transitions:
            state.addEpsilon(t.getDest());
            t = null;
          }
        }
        if (t != null) {
          newTransitions.add(t);
        }
      }
      state.resetTransitions();
      state.setTransitions(newTransitions.toArray(new Transition[newTransitions.size()]));
    }
  }
  
  @Override
  public void build(TermFreqIterator iterator) throws IOException {
    String prefix = getClass().getSimpleName();
    File directory = Sort.defaultTempDir();
    File tempInput = File.createTempFile(prefix, ".input", directory);
    File tempSorted = File.createTempFile(prefix, ".sorted", directory);
    
    Sort.ByteSequencesWriter writer = new Sort.ByteSequencesWriter(tempInput);
    Sort.ByteSequencesReader reader = null;
    BytesRef scratch = new BytesRef();

    BytesRef separator = new BytesRef(new byte[] { (byte)TokenStreamToAutomaton.POS_SEP });
    
    // analyzed sequence + 0(byte) + weight(int) + surface + analyzedLength(short) 
    boolean success = false;
    byte buffer[] = new byte[8];
    try {
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
      BytesRef surfaceForm;
      while ((surfaceForm = iterator.next()) != null) {

        // Analyze surface form:
        TokenStream ts = analyzer.tokenStream("", new StringReader(surfaceForm.utf8ToString()));

        // Create corresponding automaton: labels are bytes
        // from each analyzed token, with byte 0 used as
        // separator between tokens:
        Automaton automaton = TokenStreamToAutomaton.toAutomaton(ts);
        ts.end();
        ts.close();

        replaceSep(automaton);

        assert SpecialOperations.isFinite(automaton);

        // Get all paths from the automaton (there can be
        // more than one path, eg if the analyzer created a
        // graph using SynFilter or WDF):

        // nocommit: we should probably not wire this param to -1 but have a reasonable limit?!
        Set<IntsRef> paths = SpecialOperations.getFiniteStrings(automaton, -1);
        for (IntsRef path : paths) {

          Util.toBytesRef(path, scratch);
          
          // length of the analyzed text (FST input)
          short analyzedLength = (short) scratch.length;
          // compute the required length:
          // analyzed sequence + 12 (separator) + weight (4) + surface + analyzedLength (short)
          int requiredLength = analyzedLength + 2 + 4 + surfaceForm.length + 2;
          
          buffer = ArrayUtil.grow(buffer, requiredLength);
          
          output.reset(buffer);
          output.writeBytes(scratch.bytes, scratch.offset, scratch.length);
          output.writeByte((byte)0); // separator: not used, just for sort order
          output.writeByte((byte)0); // separator: not used, just for sort order
          output.writeInt(encodeWeight(iterator.weight()));
          output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
          output.writeShort(analyzedLength);
          writer.write(buffer, 0, output.getPosition());
        }
      }
      writer.close();

      // Sort all input/output pairs (required by FST.Builder):
      new Sort().sort(tempInput, tempSorted);
      reader = new Sort.ByteSequencesReader(tempSorted);
      
      PairOutputs<Long,BytesRef> outputs = new PairOutputs<Long,BytesRef>(PositiveIntOutputs.getSingleton(true), ByteSequenceOutputs.getSingleton());
      Builder<Pair<Long,BytesRef>> builder = new Builder<Pair<Long,BytesRef>>(FST.INPUT_TYPE.BYTE1, outputs);

      // Build FST:
      BytesRef previous = null;
      BytesRef analyzed = new BytesRef();
      BytesRef surface = new BytesRef();
      IntsRef scratchInts = new IntsRef();
      ByteArrayDataInput input = new ByteArrayDataInput();
      int dedup = 0;
      while (reader.read(scratch)) {
        input.reset(scratch.bytes, scratch.offset, scratch.length);
        input.setPosition(input.length()-2);
        short analyzedLength = input.readShort();

        analyzed.bytes = scratch.bytes;
        analyzed.offset = scratch.offset;
        analyzed.length = analyzedLength;
        
        input.setPosition(analyzedLength + 2); // analyzed sequence + separator
        long cost = input.readInt();
   
        surface.bytes = scratch.bytes;
        surface.offset = input.getPosition();
        surface.length = input.length() - input.getPosition() - 2;
        
        if (previous == null) {
          previous = new BytesRef();
        } else if (analyzed.equals(previous)) {
          // nocommit: "extend" duplicates with useless
          // increasing bytes (it wont matter) ... or we
          // could use multiple outputs for a single input?
          // this would be more efficient?
          if (dedup < 256) {
            analyzed.grow(analyzed.length+1);
            analyzed.bytes[analyzed.length] = (byte) dedup;
            dedup++;
            analyzed.length++;
          } else {
            // nocommit add param to limit "dups"???

            // More than 256 dups: skip the rest:
            continue;
          }
        } else {
          dedup = 0;
        }

        Util.toIntsRef(analyzed, scratchInts);
        // nocommit (why?)
        builder.add(scratchInts, outputs.newPair(cost, BytesRef.deepCopyOf(surface)));
        previous.copyBytes(analyzed);
      }
      fst = builder.finish();

      //Util.dotToFile(fst, "/tmp/suggest.dot");
      
      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader, writer);
      } else {
        IOUtils.closeWhileHandlingException(reader, writer);
      }
      
      tempInput.delete();
      tempSorted.delete();
    }
  }

  @Override
  public boolean store(OutputStream output) throws IOException {
    try {
      fst.save(new OutputStreamDataOutput(output));
    } finally {
      IOUtils.close(output);
    }
    return true;
  }

  @Override
  public boolean load(InputStream input) throws IOException {
    try {
      this.fst = new FST<Pair<Long,BytesRef>>(new InputStreamDataInput(input), new PairOutputs<Long,BytesRef>(PositiveIntOutputs.getSingleton(true), ByteSequenceOutputs.getSingleton()));
    } finally {
      IOUtils.close(input);
    }
    return true;
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, boolean onlyMorePopular, int num) {
    assert num > 0;
    Arc<Pair<Long,BytesRef>> arc = new Arc<Pair<Long,BytesRef>>();

    //System.out.println("lookup");

    // TODO: is there a Reader from a CharSequence?
    // Turn tokenstream into automaton:
    Automaton automaton;
    try {
      TokenStream ts = analyzer.tokenStream("", new StringReader(key.toString()));
      automaton = TokenStreamToAutomaton.toAutomaton(ts);
      ts.end();
      ts.close();
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }

    replaceSep(automaton);

    // TODO: we can optimize this somewhat by determinizing
    // while we convert
    automaton = Automaton.minimize(automaton);

    List<LookupResult> results = new ArrayList<LookupResult>(num);
    CharsRef spare = new CharsRef();

    //System.out.println("  now intersect exactFirst=" + exactFirst);
    
    // Intersect automaton w/ suggest wFST and get all
    // prefix starting nodes & their outputs:
    final List<FSTUtil.Path<Pair<Long,BytesRef>>> prefixPaths;
    try {
      prefixPaths = FSTUtil.intersectPrefixPaths(automaton, fst);
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }

    // nocommit maybe nuke exactFirst...? but... it's
    // useful?
    // nocommit: exactFirst is not well defined here ... the
    // "exactness" refers to the analyzed form not the
    // surface form
    if (exactFirst) {
      for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
        if (path.fstNode.isFinal()) {
          BytesRef prefix = BytesRef.deepCopyOf(path.output.output2);
          prefix.append(path.fstNode.nextFinalOutput.output2);
          spare.grow(prefix.length);
          UnicodeUtil.UTF8toUTF16(prefix, spare);
          results.add(new LookupResult(spare.toString(), decodeWeight(path.output.output1 + path.fstNode.nextFinalOutput.output1)));
          if (--num == 0) {
            // nocommit hmm should we order all "exact"
            // matches by their .output1s, then return those
            // top n...?
            return results; // that was quick
          }
        }
      }
    }

    Util.TopNSearcher<Pair<Long,BytesRef>> searcher = new Util.TopNSearcher<Pair<Long,BytesRef>>(fst, num, weightComparator);
    for (FSTUtil.Path<Pair<Long,BytesRef>> path : prefixPaths) {
      try {
        searcher.addStartPaths(path.fstNode, path.output, !exactFirst, path.input);
      } catch (IOException bogus) {
        throw new RuntimeException(bogus);
      }
    }

    MinResult<Pair<Long,BytesRef>> completions[] = null;
    try {
      completions = searcher.search();
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }

    for (MinResult<Pair<Long,BytesRef>> completion : completions) {
      spare.grow(completion.output.output2.length);
      UnicodeUtil.UTF8toUTF16(completion.output.output2, spare);
      results.add(new LookupResult(spare.toString(), decodeWeight(completion.output.output1)));
    }

    return results;
  }
  
  /**
   * Returns the weight associated with an input string,
   * or null if it does not exist.
   */
  public Object get(CharSequence key) {
    throw new UnsupportedOperationException();
  }
  
  /** cost -> weight */
  private static int decodeWeight(long encoded) {
    return (int)(Integer.MAX_VALUE - encoded);
  }
  
  /** weight -> cost */
  private static int encodeWeight(long value) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return Integer.MAX_VALUE - (int)value;
  }
   
  static final Comparator<Pair<Long,BytesRef>> weightComparator = new Comparator<Pair<Long,BytesRef>> () {
    public int compare(Pair<Long,BytesRef> left, Pair<Long,BytesRef> right) {
      return left.output1.compareTo(right.output1);
    }  
  };
}
