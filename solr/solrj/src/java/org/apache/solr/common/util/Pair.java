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
package org.apache.solr.common.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.common.MapWriter;

import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.common.util.Utils.toJSONString;

public class Pair<T1, T2> implements Serializable, MapWriter {
  private final T1 first;
  private final T2 second;

  public T1 first() {
    return first;
  }

  public T2 second() {
    return second;
  }

  public Pair(T1 key, T2 value) {
    this.first = key;
    this.second = value;
  }

  @Override
  public boolean equals(Object that) {
    return that instanceof Pair &&
        Objects.equals(this.first, ((Pair) that).first) &&
        Objects.equals(this.second, ((Pair) that).second);
  }

  @Override
  public String toString() {
    return toJSONString(makeMap("first", first, "second", second));
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("first", first);
    ew.put("second", second);
  }

  @SuppressWarnings("unchecked")
  public static Pair parse(Map m) {
    return new Pair(m.get("first"), m.get("second"));
  }

}