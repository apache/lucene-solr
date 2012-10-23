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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute; // javadocs
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.SpecialOperations;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs.Pair;

/**
 * Implements a fuzzy {@link AnalyzingSuggester}. The similarity measurement is
 * based on the Damerau-Levenshtein (optimal string alignment) algorithm, though
 * you can explicitly choose classic Levenshtein by passing <code>false</code>
 * to the <code>transpositions</code> parameter.
 * <p>
 * At most, this query will match terms up to
 * {@value org.apache.lucene.util.automaton.LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}
 * edits. Higher distances (especially with transpositions enabled), are not
 * supported.  Note that the fuzzy distance is byte-by-byte
 * as returned by the {@link TokenStream}'s {@link
 * TermToBytesRefAttribute}, usually UTF8.  By default
 * the first 2 (@link #DEFAULT_MIN_PREFIX) bytes must match,
 * and by default we allow up to 1 (@link
 * #DEFAULT_MAX_EDITS} edit.
 * <p>
 * Note: complex query analyzers can have a significant impact on the lookup
 * performance. It's recommended to not use analyzers that drop or inject terms
 * like synonyms to keep the complexity of the prefix intersection low for good
 * lookup performance. At index time, complex analyzers can safely be used.
 * </p>
 */
public final class FuzzySuggester extends AnalyzingSuggester {
  private final int maxEdits;
  private final boolean transpositions;
  private final int minPrefix;

  // nocommit separate param for "min length before we
  // enable fuzzy"?  eg type "nusglasses" into google...
  
  /**
   * The default minimum shared (non-fuzzy) prefix. Set to <tt>2</tt>
   */
  // nocommit should we do 1...?
  public static final int DEFAULT_MIN_PREFIX = 2;
  
  /**
   * The default maximum number of edits for fuzzy suggestions. Set to <tt>1</tt>
   */
  public static final int DEFAULT_MAX_EDITS = 1;
  
  /**
   * Creates a {@link FuzzySuggester} instance initialized with default values.
   * Calls
   * {@link FuzzySuggester#FuzzySuggester(Analyzer, Analyzer, int, int, int, int, boolean, int)}
   * FuzzySuggester(analyzer, analyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1,
   * DEFAULT_MAX_EDITS, true, DEFAULT_MIN_PREFIX)
   * 
   * @param analyzer
   *          the analyzer used for this suggester
   */
  public FuzzySuggester(Analyzer analyzer) {
    this(analyzer, analyzer);
  }
  
  /**
   * Creates a {@link FuzzySuggester} instance with an index & a query analyzer initialized with default values.
   * Calls
   * {@link FuzzySuggester#FuzzySuggester(Analyzer, Analyzer, int, int, int, int, boolean, int)}
   * FuzzySuggester(indexAnalyzer, queryAnalyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1,
   * DEFAULT_MAX_EDITS, true, DEFAULT_MIN_PREFIX)
   * 
   * @param indexAnalyzer
   *           Analyzer that will be used for analyzing suggestions while building the index.
   * @param queryAnalyzer
   *           Analyzer that will be used for analyzing query text during lookup
   */
  public FuzzySuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
    this(indexAnalyzer, queryAnalyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1, DEFAULT_MAX_EDITS, true, DEFAULT_MIN_PREFIX);
  }

  /**
   * Creates a {@link FuzzySuggester} instance.
   * 
   * @param indexAnalyzer Analyzer that will be used for
   *        analyzing suggestions while building the index.
   * @param queryAnalyzer Analyzer that will be used for
   *        analyzing query text during lookup
   * @param options see {@link #EXACT_FIRST}, {@link #PRESERVE_SEP}
   * @param maxSurfaceFormsPerAnalyzedForm Maximum number of
   *        surface forms to keep for a single analyzed form.
   *        When there are too many surface forms we discard the
   *        lowest weighted ones.
   * @param maxGraphExpansions Maximum number of graph paths
   *        to expand from the analyzed form.  Set this to -1 for
   *        no limit.
   *   
   * @param maxEdits must be >= 0 and <= {@link LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}.
   * @param transpositions <code>true</code> if transpositions should be treated as a primitive 
   *        edit operation. If this is false, comparisons will implement the classic
   *        Levenshtein algorithm.
   * @param minPrefix length of common (non-fuzzy) prefix
   *          
   */
  public FuzzySuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer,
      int options, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions, int maxEdits, boolean transpositions, int minPrefix) {
    super(indexAnalyzer, queryAnalyzer, options, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions);
    if (maxEdits < 0 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
      throw new IllegalArgumentException("maxEdits must be between 0 and " + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE);
    }
    if (minPrefix < 0) {
      throw new IllegalArgumentException("minPrefix must not be < 0");
    }
    this.maxEdits = maxEdits;
    this.transpositions = transpositions;
    this.minPrefix = minPrefix;
  }
  
  @Override
  protected List<FSTUtil.Path<Pair<Long,BytesRef>>> getFullPrefixPaths(List<FSTUtil.Path<Pair<Long,BytesRef>>> prefixPaths,
                                                                       Automaton lookupAutomaton,
                                                                       FST<Pair<Long,BytesRef>> fst)
    throws IOException {
    // nocommit we don't "penalize" for edits
    // ... shouldn't we?  ie, ed=0 completions should have
    // higher rank than ed=1, at the same "weight"?  maybe
    // we can punt on this for starters ... or maybe we
    // can re-run each prefix path through lev0, lev1,
    // lev2 to figure out the number of edits?
    Automaton levA = toLevenshteinAutomata(lookupAutomaton);
    /*
      Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"), "UTF-8");
      w.write(levA.toDot());
      w.close();
      System.out.println("Wrote LevA to out.dot");
    */
    return FSTUtil.intersectPrefixPaths(levA, fst);
  }

  Automaton toLevenshteinAutomata(Automaton automaton) {
    final Set<IntsRef> ref = SpecialOperations.getFiniteStrings(automaton, -1);
    Automaton subs[] = new Automaton[ref.size()];
    int upto = 0;
    for (IntsRef path : ref) {
      if (path.length <= minPrefix) {
        subs[upto] = BasicAutomata.makeString(path.ints, path.offset, path.length);
        upto++;
      } else {
        Automaton prefix = BasicAutomata.makeString(path.ints, path.offset, minPrefix);
        int ints[] = new int[path.length-minPrefix];
        System.arraycopy(path.ints, path.offset+minPrefix, ints, 0, ints.length);
        // nocommit i think we should pass 254 max?  ie
        // exclude 0xff ... this way we can't 'edit away'
        // the sep?  or ... maybe we want to allow that to
        // be edited away?
        // nocommit also the 0 byte ... we use that as
        // trailer ... we probably shouldn't allow that byte
        // to be edited (we could add alphaMin?)
        LevenshteinAutomata lev = new LevenshteinAutomata(ints, 255, transpositions);
        Automaton levAutomaton = lev.toAutomaton(maxEdits);
        Automaton combined = BasicOperations.concatenate(Arrays.asList(prefix, levAutomaton));
        combined.setDeterministic(true); // its like the special case in concatenate itself, except we cloneExpanded already
        subs[upto] = combined;
        upto++;
      }
    }

    // nocommit maybe we should reduce the LevN?  the added
    // arcs add cost during intersect (extra FST arc
    // lookups...).  could be net win...

    if (subs.length == 0) {
      return BasicAutomata.makeEmpty(); // matches nothing
    } else if (subs.length == 1) {
      return subs[0];
    } else {
      Automaton a = BasicOperations.union(Arrays.asList(subs));
      // TODO: we could call toLevenshteinAutomata() before det? 
      // this only happens if you have multiple paths anyway (e.g. synonyms)
      BasicOperations.determinize(a);
      return a;
    }
  }
}
