package org.apache.lucene.search;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

// TODO
//    - compare perf to PhraseQuery exact and sloppy
//    - optimize: find terms that are in fact MUST (because all paths
//      through the A include that term)
//    - if we ever store posLength in the index, it would be easy[ish]
//      to take it into account here

/** A proximity query that lets you express an automaton, whose
 *  transitions are terms, to match documents.  This is a generalization
 *  of other proximity queries like  {@link PhraseQuery}, {@link
 *  MultiPhraseQuery} and {@link SpanNearQuery}.  It is likely
 *  slow, since it visits any document having any of the terms (i.e. it
 *  acts like a disjunction, not a conjunction like {@link
 *  PhraseQuery}), and then it must merge-sort all positions within each
 *  document to test whether/how many times the automaton matches.
 *
 *  <p>After creating the query, use {@link #createState}, {@link
 *  #setAccept}, {@link #addTransition} and {@link #addAnyTransition} to
 *  build up the automaton.  Once you are done, call {@link #finish} and
 *  then execute the query.
 *
 *  <p>This code is very new and likely has exciting bugs!
 *
 *  @lucene.experimental */

public class TermAutomatonQuery extends Query {
  private final String field;
  private final Automaton.Builder builder;
  Automaton det;
  private final Map<BytesRef,Integer> termToID = new HashMap<>();
  private final Map<Integer,BytesRef> idToTerm = new HashMap<>();
  private int anyTermID = -1;

  public TermAutomatonQuery(String field) {
    this.field = field;
    this.builder = new Automaton.Builder();
  }

  /** Returns a new state; state 0 is always the initial state. */
  public int createState() {
    return builder.createState();
  }

  /** Marks the specified state as accept or not. */
  public void setAccept(int state, boolean accept) {
    builder.setAccept(state, accept);
  }

  /** Adds a transition to the automaton. */
  public void addTransition(int source, int dest, String term) {
    addTransition(source, dest, new BytesRef(term));
  }

  /** Adds a transition to the automaton. */
  public void addTransition(int source, int dest, BytesRef term) {
    if (term == null) {
      throw new NullPointerException("term should not be null");
    }
    builder.addTransition(source, dest, getTermID(term));
  }

  /** Adds a transition matching any term. */
  public void addAnyTransition(int source, int dest) {
    builder.addTransition(source, dest, getTermID(null));
  }

  /** Call this once you are done adding states/transitions. */
  public void finish() {
    finish(DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /**
   * Call this once you are done adding states/transitions.
   * @param maxDeterminizedStates Maximum number of states created when
   *   determinizing the automaton.  Higher numbers allow this operation to
   *   consume more memory but allow more complex automatons.
   */
  public void finish(int maxDeterminizedStates) {
    Automaton automaton = builder.finish();

    // System.out.println("before det:\n" + automaton.toDot());

    Transition t = new Transition();

    // TODO: should we add "eps back to initial node" for all states,
    // and det that?  then we don't need to revisit initial node at
    // every position?  but automaton could blow up?  And, this makes it
    // harder to skip useless positions at search time?

    if (anyTermID != -1) {

      // Make sure there are no leading or trailing ANY:
      int count = automaton.initTransition(0, t);
      for(int i=0;i<count;i++) {
        automaton.getNextTransition(t);
        if (anyTermID >= t.min && anyTermID <= t.max) {
          throw new IllegalStateException("automaton cannot lead with an ANY transition");
        }
      }

      int numStates = automaton.getNumStates();
      for(int i=0;i<numStates;i++) {
        count = automaton.initTransition(i, t);
        for(int j=0;j<count;j++) {
          automaton.getNextTransition(t);
          if (automaton.isAccept(t.dest) && anyTermID >= t.min && anyTermID <= t.max) {
            throw new IllegalStateException("automaton cannot end with an ANY transition");
          }
        }
      }

      int termCount = termToID.size();

      // We have to carefully translate these transitions so automaton
      // realizes they also match all other terms:
      Automaton newAutomaton = new Automaton();
      for(int i=0;i<numStates;i++) {
        newAutomaton.createState();
        newAutomaton.setAccept(i, automaton.isAccept(i));
      }

      for(int i=0;i<numStates;i++) {
        count = automaton.initTransition(i, t);
        for(int j=0;j<count;j++) {
          automaton.getNextTransition(t);
          int min, max;
          if (t.min <= anyTermID && anyTermID <= t.max) {
            // Match any term
            min = 0;
            max = termCount-1;
          } else {
            min = t.min;
            max = t.max;
          }
          newAutomaton.addTransition(t.source, t.dest, min, max);
        }
      }
      newAutomaton.finishState();
      automaton = newAutomaton;
    }

    det = Operations.removeDeadStates(Operations.determinize(automaton,
      maxDeterminizedStates));
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    IndexReaderContext context = searcher.getTopReaderContext();
    Map<Integer,TermContext> termStates = new HashMap<>();

    for (Map.Entry<BytesRef,Integer> ent : termToID.entrySet()) {
      if (ent.getKey() != null) {
        termStates.put(ent.getValue(), TermContext.build(context, new Term(field, ent.getKey())));
      }
    }

    return new TermAutomatonWeight(det, searcher, termStates);
  }

  @Override
  public String toString(String field) {
    // TODO: what really am I supposed to do with the incoming field...
    StringBuilder sb = new StringBuilder();
    sb.append("TermAutomatonQuery(field=");
    sb.append(this.field);
    if (det != null) {
      sb.append(" numStates=");
      sb.append(det.getNumStates());
    }
    sb.append(')');
    return sb.toString();
  }

  private int getTermID(BytesRef term) {
    Integer id = termToID.get(term);
    if (id == null) {
      id = termToID.size();
      if (term != null) {
        term = BytesRef.deepCopyOf(term);
      }
      termToID.put(term, id);
      idToTerm.put(id, term);
      if (term == null) {
        anyTermID = id;
      }
    }

    return id;
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TermAutomatonQuery)) {
      return false;
    }
    TermAutomatonQuery other = (TermAutomatonQuery) o;

    if (det == null) {
      throw new IllegalStateException("please call finish first");
    }
    if (other.det == null) {
      throw new IllegalStateException("please call other.finish first");
    }

    // NOTE: not quite correct, because if terms were added in different
    // order in each query but the language is the same, we return false:
    return super.equals(o)
      && this.termToID.equals(other.termToID) &&
      Operations.sameLanguage(det, other.det);
  }

  /** Returns a hash code value for this object.  This is very costly! */
  @Override
  public int hashCode() {
    if (det == null) {
      throw new IllegalStateException("please call finish first");
    }
    return super.hashCode() ^ termToID.hashCode() + det.toDot().hashCode();
  }

  /** Returns the dot (graphviz) representation of this automaton.
   *  This is extremely useful for visualizing the automaton. */
  public String toDot() {

    // TODO: refactor & share with Automaton.toDot!

    StringBuilder b = new StringBuilder();
    b.append("digraph Automaton {\n");
    b.append("  rankdir = LR\n");
    final int numStates = det.getNumStates();
    if (numStates > 0) {
      b.append("  initial [shape=plaintext,label=\"0\"]\n");
      b.append("  initial -> 0\n");
    }

    Transition t = new Transition();
    for(int state=0;state<numStates;state++) {
      b.append("  ");
      b.append(state);
      if (det.isAccept(state)) {
        b.append(" [shape=doublecircle,label=\"" + state + "\"]\n");
      } else {
        b.append(" [shape=circle,label=\"" + state + "\"]\n");
      }
      int numTransitions = det.initTransition(state, t);
      for(int i=0;i<numTransitions;i++) {
        det.getNextTransition(t);
        assert t.max >= t.min;
        for(int j=t.min;j<=t.max;j++) {
          b.append("  ");
          b.append(state);
          b.append(" -> ");
          b.append(t.dest);
          b.append(" [label=\"");
          if (j == anyTermID) {
            b.append('*');
          } else {
            b.append(idToTerm.get(j).utf8ToString());
          }
          b.append("\"]\n");
        }
      }
    }
    b.append('}');
    return b.toString();
  }

  // TODO: should we impl rewrite to return BooleanQuery of PhraseQuery,
  // when 1) automaton is finite, 2) doesn't use ANY transition, 3) is
  // "small enough"?

  static class EnumAndScorer {
    public final int termID;
    public final PostingsEnum posEnum;

    // How many positions left in the current document:
    public int posLeft;

    // Current position
    public int pos;

    public EnumAndScorer(int termID, PostingsEnum posEnum) {
      this.termID = termID;
      this.posEnum = posEnum;
    }
  }

  final class TermAutomatonWeight extends Weight {
    private final IndexSearcher searcher;
    final Automaton automaton;
    private final Map<Integer,TermContext> termStates;
    private final Similarity.SimWeight stats;
    private final Similarity similarity;

    public TermAutomatonWeight(Automaton automaton, IndexSearcher searcher, Map<Integer,TermContext> termStates) throws IOException {
      super(TermAutomatonQuery.this);
      this.automaton = automaton;
      this.searcher = searcher;
      this.termStates = termStates;
      this.similarity = searcher.getSimilarity(true);
      List<TermStatistics> allTermStats = new ArrayList<>();
      for(Map.Entry<Integer,BytesRef> ent : idToTerm.entrySet()) {
        Integer termID = ent.getKey();
        if (ent.getValue() != null) {
          allTermStats.add(searcher.termStatistics(new Term(field, ent.getValue()), termStates.get(termID)));
        }
      }

      stats = similarity.computeWeight(searcher.collectionStatistics(field),
                                       allTermStats.toArray(new TermStatistics[allTermStats.size()]));
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for(BytesRef text : termToID.keySet()) {
        if (text != null) {
          terms.add(new Term(field, text));
        }
      }
    }

    @Override
    public String toString() {
      return "weight(" + TermAutomatonQuery.this + ")";
    }

    @Override
    public float getValueForNormalization() {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float boost) {
      stats.normalize(queryNorm, boost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {

      // Initialize the enums; null for a given slot means that term didn't appear in this reader
      EnumAndScorer[] enums = new EnumAndScorer[idToTerm.size()];

      boolean any = false;
      for(Map.Entry<Integer,TermContext> ent : termStates.entrySet()) {
        TermContext termContext = ent.getValue();
        assert termContext.topReaderContext == ReaderUtil.getTopLevelContext(context) : "The top-reader used to create Weight (" + termContext.topReaderContext + ") is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);
        BytesRef term = idToTerm.get(ent.getKey());
        TermState state = termContext.get(context.ord);
        if (state != null) {
          TermsEnum termsEnum = context.reader().terms(field).iterator();
          termsEnum.seekExact(term, state);
          enums[ent.getKey()] = new EnumAndScorer(ent.getKey(), termsEnum.postings(null, PostingsEnum.POSITIONS));
          any = true;
        }
      }

      if (any) {
        return new TermAutomatonScorer(this, enums, anyTermID, idToTerm, similarity.simScorer(stats, context));
      } else {
        return null;
      }
    }
    
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      // TODO
      return null;
    }
  }
}
