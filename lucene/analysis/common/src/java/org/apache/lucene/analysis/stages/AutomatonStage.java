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
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/** Pass-through stage that builds an Automaton from the
 *  input tokens it sees. */

public class AutomatonStage extends Stage {

  /** We create transition between two adjacent tokens. */
  public static final int POS_SEP = 256;

  /** We add this arc to represent a hole. */
  public static final int HOLE = 257;

  private Automaton automaton;
  private Map<Integer,State> toStates;
  private Map<Integer,State> fromStates;
  private final ArcAttribute arcAtt;
  private final TermAttribute termAtt;

  public AutomatonStage(Stage prevStage) {
    super(prevStage);
    arcAtt = prevStage.get(ArcAttribute.class);
    termAtt = prevStage.get(TermAttribute.class);
  }

  @Override
  public void reset(Reader reader) {
    super.reset(reader);
    toStates = new HashMap<Integer,State>();
    fromStates = new HashMap<Integer,State>();
    State initial = new State();
    // Node 0 is always the start state:
    fromStates.put(0, initial);
    automaton = new Automaton(initial);
  }

  public Automaton getAutomaton() {
    return automaton;
  }

  private State getToState(int number) {
    State state = toStates.get(number);
    if (state == null) {
      state = new State();
      toStates.put(number, state);
      State fromState = fromStates.get(number);
      if (fromState != null) {
        state.addTransition(new Transition(POS_SEP, fromState));
      }
    }
    return state;
  }

  private State getFromState(int number) {
    State state = fromStates.get(number);
    if (state == null) {
      state = new State();
      fromStates.put(number, state);
      State toState = toStates.get(number);
      if (toState != null) {
        toState.addTransition(new Transition(POS_SEP, state));
      }
    }
    return state;
  }

  @Override
  public boolean next() throws IOException {
    if (prevStage.next()) {
      String term = termAtt.get();
      if (term.length() == 0) {
        throw new IllegalStateException("cannot handle empty-string term");
      }
      State lastState = getFromState(arcAtt.from());
      for(int i=0;i<term.length();i++) {
        State toState;
        if (i == term.length()-1) {
          toState = getToState(arcAtt.to());
        } else {
          toState = new State();
        }
        lastState.addTransition(new Transition(term.charAt(i), toState));
        lastState = toState;
      }    
      return true;
    } else {
      // Assume any to state w/ no transitions:
      for(State state : toStates.values()) {
        if (state.numTransitions() == 0) {
          state.setAccept(true);
        }
      }
      // TODO: we could be smarter...
      automaton.setDeterministic(false);
      return false;
    }
  }
}
