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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.fst.Util;

/**
 * A {@link CompletionQuery} that match documents specified by
 * a wrapped {@link CompletionQuery} supporting boosting and/or filtering
 * by specified contexts.
 * <p>
 * Use this query against {@link ContextSuggestField}
 * <p>
 * Example of using a {@link CompletionQuery} with boosted
 * contexts:
 * <pre class="prettyprint">
 *  CompletionQuery completionQuery = ...;
 *  ContextQuery query = new ContextQuery(completionQuery);
 *  query.addContext("context1", 2);
 *  query.addContext("context2", 1);
 * </pre>
 * <p>
 * NOTE:
 * <ul>
 *   <li>
 *    This query can be constructed with
 *    {@link PrefixCompletionQuery}, {@link RegexCompletionQuery}
 *    or {@link FuzzyCompletionQuery} query.
 *   </li>
 *   <li>
 *     To suggest across all contexts with the same boost,
 *     use '*' as the context in {@link #addContext(CharSequence)})}.
 *     This can be combined with specific contexts with different boosts.
 *   </li>
 *   <li>
 *     To apply the same boost to multiple contexts sharing the same prefix,
 *     Use {@link #addContext(CharSequence, float, boolean)} with the common
 *     context prefix, boost and set <code>exact</code> to false.
 *   <li>
 *     Using this query against a {@link SuggestField} (not context enabled),
 *     would yield results ignoring any context filtering/boosting
 *   </li>
 * </ul>
 *
 * @lucene.experimental
 */
public class ContextQuery extends CompletionQuery {
  private Map<CharSequence, ContextMetaData> contexts;
  /** Inner completion query */
  protected CompletionQuery query;

  /**
   * Constructs a context completion query that matches
   * documents specified by <code>query</code>.
   * <p>
   * Use {@link #addContext(CharSequence, float, boolean)}
   * to add context(s) with boost
   */
  public ContextQuery(CompletionQuery query) {
    super(query.getTerm(), query.getFilter());
    if (query instanceof ContextQuery) {
      throw new IllegalArgumentException("'query' parameter must not be of type "
              + this.getClass().getSimpleName());
    }
    this.query = query;
    contexts = new HashMap<>();
  }

  /**
   * Adds an exact context with default boost of 1
   */
  public void addContext(CharSequence context) {
    addContext(context, 1f, true);
  }

  /**
   * Adds an exact context with boost
   */
  public void addContext(CharSequence context, float boost) {
    addContext(context, boost, true);
  }

  /**
   * Adds a context with boost, set <code>exact</code> to false
   * if the context is a prefix of any indexed contexts
   */
  public void addContext(CharSequence context, float boost, boolean exact) {
    if (boost < 0f) {
      throw new IllegalArgumentException("'boost' must be >= 0");
    }
    for (int i = 0; i < context.length(); i++) {
      if (ContextSuggestField.CONTEXT_SEPARATOR == context.charAt(i)) {
        throw new IllegalArgumentException("Illegal value [" + context + "] UTF-16 codepoint [0x"
            + Integer.toHexString((int) context.charAt(i))+ "] at position " + i + " is a reserved character");
      }
    }
    contexts.put(context, new ContextMetaData(boost, exact));
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    for (CharSequence context : contexts.keySet()) {
      if (buffer.length() != 0) {
        buffer.append(",");
      } else {
        buffer.append("contexts");
        buffer.append(":[");
      }
      buffer.append(context);
      ContextMetaData metaData = contexts.get(context);
      if (metaData.exact == false) {
        buffer.append("*");
      }
      if (metaData.boost != 0) {
        buffer.append("^");
        buffer.append(Float.toString(metaData.boost));
      }
    }
    if (buffer.length() != 0) {
      buffer.append("]");
      buffer.append(",");
    }
    return buffer.toString() + query.toString(field);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    IntsRefBuilder scratch = new IntsRefBuilder();
    final Map<IntsRef, Float> contextMap = new HashMap<>(contexts.size());
    final TreeSet<Integer> contextLengths = new TreeSet<>();
    final CompletionWeight innerWeight = ((CompletionWeight) query.createWeight(searcher, needsScores));
    Automaton contextsAutomaton = null;
    Automaton gap = Automata.makeChar(ContextSuggestField.CONTEXT_SEPARATOR);
    // if separators are preserved the fst contains a SEP_LABEL
    // behind each gap. To have a matching automaton, we need to
    // include the SEP_LABEL in the query as well
    gap = Operations.concatenate(gap, Operations.optional(Automata.makeChar(CompletionAnalyzer.SEP_LABEL)));
    final Automaton prefixAutomaton = Operations.concatenate(gap, innerWeight.getAutomaton());
    final Automaton matchAllAutomaton = new RegExp(".*").toAutomaton();
    for (Map.Entry<CharSequence, ContextMetaData> entry : contexts.entrySet()) {
      Automaton contextAutomaton;
      if (entry.getKey().equals("*")) {
        contextAutomaton = Operations.concatenate(matchAllAutomaton, prefixAutomaton);
      } else {
        BytesRef ref = new BytesRef(entry.getKey());
        ContextMetaData contextMetaData = entry.getValue();
        contextMap.put(IntsRef.deepCopyOf(Util.toIntsRef(ref, scratch)), contextMetaData.boost);
        contextLengths.add(scratch.length());
        contextAutomaton = Automata.makeString(entry.getKey().toString());
        if (contextMetaData.exact) {
          contextAutomaton = Operations.concatenate(contextAutomaton, prefixAutomaton);
        } else {
          contextAutomaton = Operations.concatenate(Arrays.asList(contextAutomaton,
              matchAllAutomaton,
              prefixAutomaton));
        }
      }
      if (contextsAutomaton == null) {
        contextsAutomaton = contextAutomaton;
      } else {
        contextsAutomaton = Operations.union(contextsAutomaton, contextAutomaton);
      }
    }
    if (contexts.size() == 0) {
      addContext("*");
      contextsAutomaton = Operations.concatenate(matchAllAutomaton, prefixAutomaton);
    }
    contextsAutomaton = Operations.determinize(contextsAutomaton, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
    int[] contextLengthArray = new int[contextLengths.size()];
    final Iterator<Integer> iterator = contextLengths.descendingIterator();
    for (int i = 0; iterator.hasNext(); i++) {
      contextLengthArray[i] = iterator.next();
    }
    return new ContextCompletionWeight(searcher.getIndexReader(), this, contextsAutomaton,
        innerWeight, contextMap, contextLengthArray);
  }

  private static class ContextMetaData {
    private final float boost;
    private final boolean exact;

    private ContextMetaData(float boost, boolean exact) {
      this.boost = boost;
      this.exact = exact;
    }
  }

  private class ContextCompletionWeight extends CompletionWeight {

    private final Map<IntsRef, Float> contextMap;
    private final int[] contextLengths;
    private final CompletionWeight innerWeight;
    private final BytesRefBuilder scratch = new BytesRefBuilder();

    private float currentBoost;
    private CharSequence currentContext;

    public ContextCompletionWeight(IndexReader reader, CompletionQuery query,
                                   Automaton automaton, CompletionWeight innerWeight,
                                   Map<IntsRef, Float> contextMap,
                                   int[] contextLengths) throws IOException {
      super(reader, query, automaton);
      this.contextMap = contextMap;
      this.contextLengths = contextLengths;
      this.innerWeight = innerWeight;
    }

    @Override
    protected void setNextMatch(IntsRef pathPrefix) {
      IntsRef ref = pathPrefix.clone();

      // check if the pathPrefix matches any
      // defined context, longer context first
      for (int contextLength : contextLengths) {
        if (contextLength > pathPrefix.length) {
          continue;
        }
        ref.length = contextLength;
        if (contextMap.containsKey(ref)) {
          currentBoost = contextMap.get(ref);
          ref.length = pathPrefix.length;
          ref.offset = contextLength;
          while (ref.ints[ref.offset] != ContextSuggestField.CONTEXT_SEPARATOR) {
            ref.offset++;
            assert ref.offset < ref.length;
          }
          assert ref.ints[ref.offset] == ContextSuggestField.CONTEXT_SEPARATOR : "expected CONTEXT_SEPARATOR at offset=" + ref.offset;
          if (ref.offset > pathPrefix.offset) {
            currentContext = Util.toBytesRef(new IntsRef(pathPrefix.ints, pathPrefix.offset, ref.offset), scratch).utf8ToString();
          } else {
            currentContext = null;
          }
          ref.offset++;
          if (ref.ints[ref.offset] == CompletionAnalyzer.SEP_LABEL) {
            ref.offset++;
          }
          innerWeight.setNextMatch(ref);
          return;
        }
      }
      // unknown context
      ref.length = pathPrefix.length;
      currentBoost = contexts.get("*").boost;
      for (int i = pathPrefix.offset; i < pathPrefix.length; i++) {
        if (pathPrefix.ints[i] == ContextSuggestField.CONTEXT_SEPARATOR) {
          if (i > pathPrefix.offset) {
            currentContext = Util.toBytesRef(new IntsRef(pathPrefix.ints, pathPrefix.offset, i), scratch).utf8ToString();
          } else {
            currentContext = null;
          }
          ref.offset = ++i;
          assert ref.offset < ref.length : "input should not end with the context separator";
          if (pathPrefix.ints[i] == CompletionAnalyzer.SEP_LABEL) {
            ref.offset++;
            assert ref.offset < ref.length : "input should not end with a context separator followed by SEP_LABEL";
          }
          ref.length -= ref.offset;
          innerWeight.setNextMatch(ref);
        }
      }
    }

    @Override
    protected CharSequence context() {
      return currentContext;
    }

    @Override
    protected float boost() {
      return currentBoost + innerWeight.boost();
    }
  }
}
