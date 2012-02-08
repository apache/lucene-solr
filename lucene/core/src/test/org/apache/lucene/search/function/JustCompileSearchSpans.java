package org.apache.lucene.search.function;

/**
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;

import java.io.IOException;

/**
 * Holds all implementations of classes in the o.a.l.s.function package as a
 * back-compatibility test. It does not run any tests per-se, however if
 * someone adds a method to an interface or abstract method to an abstract
 * class, one of the implementations here will fail to compile and so we know
 * back-compat policy was violated.
 */
final class JustCompileSearchFunction {

  private static final String UNSUPPORTED_MSG = "unsupported: used for back-compat testing only !";

  static final class JustCompileDocValues extends DocValues {
    @Override
    public float floatVal(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public String toString(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }

  static final class JustCompileFieldCacheSource extends FieldCacheSource {

    public JustCompileFieldCacheSource(String field) {
      super(field);
    }

    @Override
    public boolean cachedFieldSourceEquals(FieldCacheSource other) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int cachedFieldSourceHashCode() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public DocValues getCachedFieldValues(FieldCache cache, String field,
                                          IndexReader reader) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }

  static final class JustCompileValueSource extends ValueSource {
    @Override
    public String description() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public DocValues getValues(IndexReader reader) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }

}
