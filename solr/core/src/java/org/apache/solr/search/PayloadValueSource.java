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

package org.apache.solr.search;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

public abstract class PayloadValueSource extends ValueSource {
  protected final String field;
  protected final String val;
  protected final String indexedField;
  protected final BytesRef indexedBytes;
  protected final ValueSource defaultValueSource;

  public PayloadValueSource(String field, String val, String indexedField, BytesRef indexedBytes, ValueSource defaultValueSource) {
    this.field = field;
    this.val = val;
    this.indexedField = indexedField;
    this.indexedBytes = indexedBytes;
    this.defaultValueSource = defaultValueSource;
  }

//  public abstract SortField getSortField(boolean reverse);

  // TODO: should this be formalized at the ValueSource level?  Seems to be the convention
  public abstract String name();

  @Override
  public abstract String description();

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PayloadValueSource that = (PayloadValueSource) o;

    if (!indexedField.equals(that.indexedField)) return false;
    if (indexedBytes != null ? !indexedBytes.equals(that.indexedBytes) : that.indexedBytes != null) return false;
    return defaultValueSource.equals(that.defaultValueSource);

  }

  @Override
  public int hashCode() {
    int result = indexedField.hashCode();
    result = 31 * result + (indexedBytes != null ? indexedBytes.hashCode() : 0);
    result = 31 * result + defaultValueSource.hashCode();
    return result;
  }
}
