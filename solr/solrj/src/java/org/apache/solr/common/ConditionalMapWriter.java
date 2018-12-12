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

package org.apache.solr.common;

import java.io.IOException;
import java.util.Set;
import java.util.function.BiPredicate;

public class ConditionalMapWriter implements MapWriter {
  private final MapWriter delegate;
  private final BiPredicate<CharSequence, Object> predicate;

  public ConditionalMapWriter(MapWriter delegate, BiPredicate<CharSequence, Object> predicate) {
    this.delegate = delegate;
    this.predicate = predicate;
  }

  public static class EntryWriterWrapper implements EntryWriter {
    private final EntryWriter delegate;
    private final BiPredicate<CharSequence, Object> predicate;

    public EntryWriterWrapper(EntryWriter delegate, BiPredicate<CharSequence, Object> predicate) {
      this.delegate = delegate;
      this.predicate = predicate;
    }

    @Override
    public EntryWriter put(CharSequence k, Object v) throws IOException {
      if (predicate.test(k, v)) delegate.put(k, v);
      return this;
    }

    @Override
    public EntryWriter put(CharSequence k, int v) throws IOException {
      return put(k, Integer.valueOf(v));
    }

    @Override
    public EntryWriter put(CharSequence k, long v) throws IOException {
      return put(k, Long.valueOf(v));
    }

    @Override
    public EntryWriter put(CharSequence k, float v) throws IOException {
      return put(k, Float.valueOf(v));
    }

    @Override
    public EntryWriter put(CharSequence k, double v) throws IOException {
      return put(k, Double.valueOf(v));
    }

    @Override
    public EntryWriter put(CharSequence k, boolean v) throws IOException {
      return put(k, Boolean.valueOf(v));
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if (delegate != null) delegate.writeMap(new EntryWriterWrapper(ew, predicate));
  }

  public static BiPredicate<CharSequence, Object> dedupeKeyPredicate(Set<CharSequence> keys) {
    return (k, v) -> keys.add(k);
  }

  public static final BiPredicate<CharSequence, Object> NON_NULL_VAL = (s, o) -> o != null;
}
