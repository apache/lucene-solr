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
  private final BiPredicate<String, Object> predicate;

  public ConditionalMapWriter(MapWriter delegate, BiPredicate<String, Object> predicate) {
    this.delegate = delegate;
    this.predicate = predicate;
  }

  private class EntryWriterWrapper implements EntryWriter {
    private final EntryWriter delegate;

    EntryWriterWrapper(EntryWriter delegate) {
      this.delegate = delegate;
    }

    @Override
    public EntryWriter put(String k, Object v) throws IOException {
      if (predicate.test(k, v)) delegate.put(k, v);
      return this;
    }

    @Override
    public EntryWriter put(String k, int v) throws IOException {
      return put(k, Integer.valueOf(v));
    }

    @Override
    public EntryWriter put(String k, long v) throws IOException {
      return put(k, Long.valueOf(v));
    }

    @Override
    public EntryWriter put(String k, float v) throws IOException {
      return put(k, Float.valueOf(v));
    }

    @Override
    public EntryWriter put(String k, double v) throws IOException {
      return put(k, Double.valueOf(v));
    }

    @Override
    public EntryWriter put(String k, boolean v) throws IOException {
      return put(k, Boolean.valueOf(v));
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if(delegate!=null) delegate.writeMap(new EntryWriterWrapper(ew));
  }

  public static BiPredicate<String, Object> dedupeKeyPredicate(Set<String> keys) {
    return (k, v) -> keys.add(k);
  }

  public static final BiPredicate<String, Object> NON_NULL_VAL = (s, o) -> o != null;
}
