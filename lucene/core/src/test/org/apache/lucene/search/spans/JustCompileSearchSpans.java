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
package org.apache.lucene.search.spans;


import java.io.IOException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;

/**
 * Holds all implementations of classes in the o.a.l.s.spans package as a
 * back-compatibility test. It does not run any tests per-se, however if
 * someone adds a method to an interface or abstract method to an abstract
 * class, one of the implementations here will fail to compile and so we know
 * back-compat policy was violated.
 */
final class JustCompileSearchSpans {

  private static final String UNSUPPORTED_MSG = "unsupported: used for back-compat testing only !";

  static final class JustCompileSpans extends Spans {

    @Override
    public int docID() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int startPosition() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int endPosition() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int width() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {

    }

    @Override
    public int nextStartPosition() throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public float positionsCost() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileSpanQuery extends SpanQuery {

    @Override
    public String getField() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public String toString(String field) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }

}
