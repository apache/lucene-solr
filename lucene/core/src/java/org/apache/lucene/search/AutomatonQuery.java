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
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;

/**
 * A {@link Query} that will match terms against a finite-state machine.
 * <p>
 * This query will match documents that contain terms accepted by a given
 * finite-state machine. The automaton can be constructed with the
 * {@link org.apache.lucene.util.automaton} API. Alternatively, it can be
 * created from a regular expression with {@link RegexpQuery} or from
 * the standard Lucene wildcard syntax with {@link WildcardQuery}.
 * </p>
 * <p>
 * When the query is executed, it will create an equivalent DFA of the
 * finite-state machine, and will enumerate the term dictionary in an
 * intelligent way to reduce the number of comparisons. For example: the regular
 * expression of <code>[dl]og?</code> will make approximately four comparisons:
 * do, dog, lo, and log.
 * </p>
 * @lucene.experimental
 */
public class AutomatonQuery extends MultiTermQuery implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(AutomatonQuery.class);

  /** the automaton to match index terms against */
  protected final Automaton automaton;
  protected final CompiledAutomaton compiled;
  /** term containing the field, and possibly some pattern structure */
  protected final Term term;
  protected final boolean automatonIsBinary;

  private final long ramBytesUsed; // cache

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   * 
   * @param term Term containing field and possibly some pattern structure. The
   *        term text is ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a
   *        match.
   */
  public AutomatonQuery(final Term term, Automaton automaton) {
    this(term, automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
  }

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   * 
   * @param term Term containing field and possibly some pattern structure. The
   *        term text is ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a
   *        match.
   * @param determinizeWorkLimit maximum effort to spend determinizing the automaton. If the
   *        automaton would need more than this much effort, TooComplexToDeterminizeException is
   *        thrown. Higher numbers require more space but can process more complex automata.
   */
  public AutomatonQuery(final Term term, Automaton automaton, int determinizeWorkLimit) {
    this(term, automaton, determinizeWorkLimit, false);
  }

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   * 
   * @param term Term containing field and possibly some pattern structure. The
   *        term text is ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a
   *        match.
   * @param determinizeWorkLimit maximum effort to spend determinizing the automaton. If the
   *        automaton will need more than this much effort, TooComplexToDeterminizeException is thrown.
   *        Higher numbers require more space but can process more complex automata.
   * @param isBinary if true, this automaton is already binary and
   *   will not go through the UTF32ToUTF8 conversion
   */
  public AutomatonQuery(final Term term, Automaton automaton, int determinizeWorkLimit, boolean isBinary) {
    super(term.field());
    this.term = term;
    this.automaton = automaton;
    this.automatonIsBinary = isBinary;
    // TODO: we could take isFinite too, to save a bit of CPU in CompiledAutomaton ctor?:
    this.compiled = new CompiledAutomaton(automaton, null, true, determinizeWorkLimit, isBinary);

    this.ramBytesUsed = BASE_RAM_BYTES + term.ramBytesUsed() + automaton.ramBytesUsed() + compiled.ramBytesUsed();
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    return compiled.getTermsEnum(terms);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + compiled.hashCode();
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    AutomatonQuery other = (AutomatonQuery) obj;
    if (!compiled.equals(other.compiled))
      return false;
    if (term == null) {
      if (other.term != null)
        return false;
    } else if (!term.equals(other.term))
      return false;
    return true;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(getClass().getSimpleName());
    buffer.append(" {");
    buffer.append('\n');
    buffer.append(automaton.toString());
    buffer.append("}");
    return buffer.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      compiled.visit(visitor, this, field);
    }
  }

  /** Returns the automaton used to create this query */
  public Automaton getAutomaton() {
    return automaton;
  }

  /** Is this a binary (byte) oriented automaton. See the constructor.  */
  public boolean isAutomatonBinary() {
    return automatonIsBinary;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }
}
