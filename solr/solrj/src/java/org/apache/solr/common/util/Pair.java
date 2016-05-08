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

import java.io.Serializable;
import java.util.Objects;

public class Pair<T1, T2> implements Serializable {
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
  public boolean equals(Object obj) {
    if (obj instanceof Pair) {
      Pair that = (Pair) obj;
      return Objects.equals(this.first, that.first) && Objects.equals(this.second, that.second);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (this.first == null ? 0 : this.first.hashCode()) ^ (this.second == null ? 0 : this.second.hashCode());
  }
}