package org.apache.lucene.search.suggest.analyzing;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester.PathIntersector;
import org.apache.lucene.search.suggest.analyzing.FSTUtil.Path;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.SpecialOperations;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs.Pair;

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

public class FuzzySuggester extends AnalyzingSuggester {
  private final int maxEdits;
  private final boolean transpositions;
  private final int minPrefix;
  
  public FuzzySuggester(Analyzer analyzer) {
    this(analyzer, analyzer);
  }
  
  public FuzzySuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
    this(indexAnalyzer, queryAnalyzer, EXACT_FIRST | PRESERVE_SEP, 256, -1, 1, true, 1);
  }

  // nocommit: probably want an option to like, require the first character or something :)
  public FuzzySuggester(Analyzer indexAnalyzer, Analyzer queryAnalyzer,
      int options, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions, int maxEdits, boolean transpositions, int minPrefix) {
    super(indexAnalyzer, queryAnalyzer, options, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions);
    this.maxEdits = maxEdits;
    this.transpositions = transpositions;
    this.minPrefix = minPrefix;
  }
  
  

  @Override
  protected PathIntersector getPathIntersector(Automaton automaton,
      FST<Pair<Long,BytesRef>> fst) {
    return new FuzzyPathIntersector(automaton, fst);
  }

  final Automaton toLevenshteinAutomata(Automaton automaton) {
    // nocommit: how slow can this be :)
    Set<IntsRef> ref = SpecialOperations.getFiniteStrings(automaton, -1);
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
        LevenshteinAutomata lev = new LevenshteinAutomata(ints, 256, transpositions);
        Automaton levAutomaton = lev.toAutomaton(maxEdits);
        Automaton combined = BasicOperations.concatenate(Arrays.asList(prefix, levAutomaton));
        combined.setDeterministic(true); // its like the special case in concatenate itself, except we cloneExpanded already
        subs[upto] = combined;
        upto++;
      }
    }
    if (subs.length == 0) {
      return BasicAutomata.makeEmpty(); // matches nothing
    } else if (subs.length == 1) {
      return subs[0];
    } else {
      Automaton a = BasicOperations.union(Arrays.asList(subs));
      // nocommit: we could call toLevenshteinAutomata() before det? 
      // this only happens if you have multiple paths anyway (e.g. synonyms)
      BasicOperations.determinize(a);
      return a;
    }
  }
  
  private final class FuzzyPathIntersector extends PathIntersector {

    public FuzzyPathIntersector(Automaton automaton,
        FST<Pair<Long,BytesRef>> fst) {
      super(automaton, fst);
    }

    @Override
    public List<Path<Pair<Long,BytesRef>>> intersectAll() throws IOException {
      return  FSTUtil.intersectPrefixPaths(toLevenshteinAutomata(automaton),fst);
    }
    
  }
}
