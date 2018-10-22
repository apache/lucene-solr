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
package org.apache.solr.legacy;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;

/**
 * Field extension with support for legacy numerics
 * @deprecated Please switch to {@link org.apache.lucene.index.PointValues} instead
 */
@Deprecated
public class LegacyField extends Field {

  /**
   * Expert: creates a field with no initial value.
   * Intended only for custom LegacyField subclasses.
   * @param name field name
   * @param type field type
   * @throws IllegalArgumentException if either the name or type
   *         is null.
   */
  public LegacyField(String name, LegacyFieldType type) {
    super(name, type);
  }
  
  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
    if (fieldType().indexOptions() == IndexOptions.NONE) {
      // Not indexed
      return null;
    }
    final LegacyFieldType fieldType = (LegacyFieldType) fieldType();
    final LegacyNumericType numericType = fieldType.numericType();
    if (numericType != null) {
      if (!(reuse instanceof LegacyNumericTokenStream && ((LegacyNumericTokenStream)reuse).getPrecisionStep() == fieldType.numericPrecisionStep())) {
        // lazy init the TokenStream as it is heavy to instantiate
        // (attributes,...) if not needed (stored field loading)
        reuse = new LegacyNumericTokenStream(fieldType.numericPrecisionStep());
      }
      final LegacyNumericTokenStream nts = (LegacyNumericTokenStream) reuse;
      // initialize value in TokenStream
      final Number val = (Number) fieldsData;
      switch (numericType) {
      case INT:
        nts.setIntValue(val.intValue());
        break;
      case LONG:
        nts.setLongValue(val.longValue());
        break;
      case FLOAT:
        nts.setFloatValue(val.floatValue());
        break;
      case DOUBLE:
        nts.setDoubleValue(val.doubleValue());
        break;
      default:
        throw new AssertionError("Should never get here");
      }
      return reuse;
    }
    return super.tokenStream(analyzer, reuse);
  }
  
  @Override
  public void setTokenStream(TokenStream tokenStream) {
    final LegacyFieldType fieldType = (LegacyFieldType) fieldType();
    if (fieldType.numericType() != null) {
      throw new IllegalArgumentException("cannot set private TokenStream on numeric fields");
    }
    super.setTokenStream(tokenStream);
  }

}
