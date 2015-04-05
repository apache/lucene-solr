package org.apache.lucene.search.spans;

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
import java.util.Collection;
import java.util.Objects;

import org.apache.lucene.search.TwoPhaseIterator;

/**
 * A {@link Spans} implementation wrapping another spans instance,
 * allowing to override selected methods in a subclass.
 */
public abstract class FilterSpans extends Spans {
 
  /** The wrapped spans instance. */
  protected final Spans in;
  
  /** Wrap the given {@link Spans}. */
  public FilterSpans(Spans in) {
    this.in = Objects.requireNonNull(in);
  }
  
  @Override
  public int nextDoc() throws IOException {
    return in.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return in.advance(target);
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public int nextStartPosition() throws IOException {
    return in.nextStartPosition();
  }

  @Override
  public int startPosition() {
    return in.startPosition();
  }
  
  @Override
  public int endPosition() {
    return in.endPosition();
  }
  
  @Override
  public Collection<byte[]> getPayload() throws IOException {
    return in.getPayload();
  }

  @Override
  public boolean isPayloadAvailable() throws IOException {
    return in.isPayloadAvailable();
  }
  
  @Override
  public long cost() {
    return in.cost();
  }
  
  @Override
  public String toString() {
    return "Filter(" + in.toString() + ")";
  }
  
  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    final TwoPhaseIterator inner = in.asTwoPhaseIterator();
    if (inner != null) {
      // wrapped instance has an approximation
      return new TwoPhaseIterator(inner.approximation()) {
        @Override
        public boolean matches() throws IOException {
          return inner.matches() && twoPhaseCurrentDocMatches();
        }
      };
    } else {
      // wrapped instance has no approximation, but 
      // we can still defer matching until absolutely needed.
      return new TwoPhaseIterator(in) {
        @Override
        public boolean matches() throws IOException {
          return twoPhaseCurrentDocMatches();
        }
      };
    }
  }
  
  /**
   * Returns true if the current document matches.
   * <p>
   * This is called during two-phase processing.
   */
  public abstract boolean twoPhaseCurrentDocMatches() throws IOException;
}
