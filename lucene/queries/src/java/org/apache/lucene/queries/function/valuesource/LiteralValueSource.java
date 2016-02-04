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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.StrDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.util.Map;
import java.io.IOException;


/**
 * Pass a the field value through as a String, no matter the type // Q: doesn't this mean it's a "string"?
 *
 **/
public class LiteralValueSource extends ValueSource {
  protected final String string;
  protected final BytesRef bytesRef;

  public LiteralValueSource(String string) {
    this.string = string;
    this.bytesRef = new BytesRef(string);
  }

  /** returns the literal value */
  public String getValue() {
    return string;
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {

    return new StrDocValues(this) {
      @Override
      public String strVal(int doc) {
        return string;
      }

      @Override
      public boolean bytesVal(int doc, BytesRefBuilder target) {
        target.copyBytes(bytesRef);
        return true;
      }

      @Override
      public String toString(int doc) {
        return string;
      }
    };
  }

  @Override
  public String description() {
    return "literal(" + string + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LiteralValueSource)) return false;

    LiteralValueSource that = (LiteralValueSource) o;

    return string.equals(that.string);

  }

  public static final int hash = LiteralValueSource.class.hashCode();
  @Override
  public int hashCode() {
    return hash + string.hashCode();
  }
}
