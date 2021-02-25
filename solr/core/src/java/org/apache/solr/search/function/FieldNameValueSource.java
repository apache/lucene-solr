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

package org.apache.solr.search.function;


import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/** Placeholder value source.
 * @lucene.internal */
public class FieldNameValueSource extends ValueSource {
  private String fieldName;

  public FieldNameValueSource(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    throw new UnsupportedOperationException("FieldNameValueSource should not be directly used: " + this);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FieldNameValueSource && fieldName.equals(((FieldNameValueSource)o).getFieldName());
  }

  @Override
  public int hashCode() {
    return fieldName.hashCode();
  }

  @Override
  public String description() {
    return "FIELDNAME(" + fieldName + ")";
  }
}
