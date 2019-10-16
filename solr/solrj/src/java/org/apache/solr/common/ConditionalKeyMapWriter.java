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
import java.util.function.Predicate;

public class ConditionalKeyMapWriter implements MapWriter {
  private final MapWriter delegate;
  private final Predicate<CharSequence> predicate;

  public ConditionalKeyMapWriter(MapWriter delegate, Predicate<CharSequence> predicate) {
    this.delegate = delegate;
    this.predicate = predicate;
  }

  public static class EntryWriterWrapper implements EntryWriter {
    private final EntryWriter delegate;
    private final Predicate<CharSequence> predicate;

    public EntryWriterWrapper(EntryWriter delegate, Predicate<CharSequence> predicate) {
      this.delegate = delegate;
      this.predicate = predicate;
    }

    @Override
    public EntryWriter put(CharSequence k, Object v) throws IOException {
      if (predicate.test(k)) delegate.put(k, v);
      return this;
    }

    @Override
    public EntryWriter put(CharSequence k, int v) throws IOException {
      if (predicate.test(k)) delegate.put(k, v);
      return this;
    }

    @Override
    public EntryWriter put(CharSequence k, long v) throws IOException {
      if (predicate.test(k)) delegate.put(k, v);
      return this;
    }

    @Override
    public EntryWriter put(CharSequence k, float v) throws IOException {
      if (predicate.test(k)) delegate.put(k, v);
      return this;
    }

    @Override
    public EntryWriter put(CharSequence k, double v) throws IOException {
      if (predicate.test(k)) delegate.put(k, v);
      return this;
    }

    @Override
    public EntryWriter put(CharSequence k, boolean v) throws IOException {
      if (predicate.test(k)) delegate.put(k, v);
      return this;
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if (delegate != null) delegate.writeMap(new EntryWriterWrapper(ew, predicate));
  }

  public static Predicate<CharSequence> dedupeKeyPredicate(Set<CharSequence> keys) {
    return (k) -> keys.add(k);
  }

}
