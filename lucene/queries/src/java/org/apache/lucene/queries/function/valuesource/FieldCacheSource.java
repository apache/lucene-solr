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
package org.apache.lucene.queries.function.valuesource;

import org.apache.lucene.queries.function.ValueSource;

/**
 * A base class for ValueSource implementations that retrieve values for a single field from
 * DocValues.
 */
public abstract class FieldCacheSource extends ValueSource {
  protected final String field;

  public FieldCacheSource(String field) {
    this.field = field;
  }

  public String getField() {
    return field;
  }

  @Override
  public String description() {
    return field;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof FieldCacheSource)) return false;
    FieldCacheSource other = (FieldCacheSource) o;
    return this.field.equals(other.field);
  }

  @Override
  public int hashCode() {
    return field.hashCode();
  }
}
