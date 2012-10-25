package org.apache.lucene.queries.function.valuesource;

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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.queries.function.ValueSource;

/**
 * A {@link ValueSource} that is based on a field's {@link DocValues}.
 * @lucene.experimental
 */
public abstract class DocValuesFieldSource extends ValueSource {

  protected final String fieldName;
  protected final boolean direct;

  protected DocValuesFieldSource(String fieldName, boolean direct) {
    this.fieldName = fieldName;
    this.direct = direct;
  }

  protected final DocValues.Source getSource(AtomicReader reader, DocValues.Type defaultType) throws IOException {
    final DocValues vals = reader.docValues(fieldName);
    if (vals == null) {
      switch (defaultType) {
        case BYTES_FIXED_SORTED:
        case BYTES_VAR_SORTED:
          return DocValues.getDefaultSortedSource(defaultType, reader.maxDoc());
        default:
          return DocValues.getDefaultSource(defaultType);
      }
    }
    return direct ? vals.getDirectSource() : vals.getSource();
  }

  /**
   * @return whether or not a direct
   * {@link org.apache.lucene.index.DocValues.Source} is used.
   */
  public boolean isDirect() {
    return direct;
  }

  /**
   * @return the field name
   */
  public String getFieldName() {
    return fieldName;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !getClass().isInstance(o)) {
      return false;
    }
    final DocValuesFieldSource other = (DocValuesFieldSource) o;
    return fieldName.equals(other.fieldName) && direct == other.direct;
  }

  @Override
  public int hashCode() {
    int h = getClass().hashCode();
    h = 31 * h + fieldName.hashCode();
    h = 31 * h + (direct ? 1 : 0);
    return h;
  }

  @Override
  public String description() {
    return fieldName;
  }
}
