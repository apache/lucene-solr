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
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/**
 * Expert: obtains numeric field values from a {@link FunctionValues} field.
 * This {@link ValueSource} is compatible with all numerical
 * {@link FunctionValues}
 * 
 * @lucene.experimental
 * 
 */
public class NumericIndexDocValueSource extends ValueSource {

  private final String field;

  public NumericIndexDocValueSource(String field) {
    this.field = field;
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final Source source = readerContext.reader().docValues(field)
        .getSource();
    Type type = source.getType();
    switch (type) {
    case FLOAT_32:
    case FLOAT_64:
      // TODO (chrism) Change to use FloatDocValues and IntDocValues
      return new FunctionValues() {

        @Override
        public String toString(int doc) {
          return "float: [" + floatVal(doc) + "]";
        }

        @Override
        public float floatVal(int doc) {
          return (float) source.getFloat(doc);
        }
      };

    case VAR_INTS:
      return new FunctionValues() {
        @Override
        public String toString(int doc) {
          return "float: [" + floatVal(doc) + "]";
        }

        @Override
        public float floatVal(int doc) {
          return (float) source.getInt(doc);
        }
      };
    default:
      throw new IOException("Type: " + type + "is not numeric");
    }

  }

  @Override
  public String description() {
    return toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    NumericIndexDocValueSource other = (NumericIndexDocValueSource) obj;
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (!field.equals(other.field))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "FunctionValues float(" + field + ')';
  }
}
