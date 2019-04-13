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

package org.apache.lucene.luke.models.documents;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;

/**
 * Holder for doc values.
 */
public final class DocValues {

  private final DocValuesType dvType;

  private final List<BytesRef> values;

  private final List<Long> numericValues;

  /**
   * Returns a new doc values entry representing the specified doc values type and values.
   * @param dvType - doc values type
   * @param values - (string) values
   * @param numericValues numeric values
   * @return doc values
   */
  static DocValues of(DocValuesType dvType, List<BytesRef> values, List<Long> numericValues) {
    return new DocValues(dvType, values, numericValues);
  }

  private DocValues(DocValuesType dvType, List<BytesRef> values, List<Long> numericValues) {
    this.dvType = dvType;
    this.values = values;
    this.numericValues = numericValues;
  }

  /**
   * Returns the type of this doc values.
   */
  public DocValuesType getDvType() {
    return dvType;
  }

  /**
   * Returns the list of (string) values.
   */
  public List<BytesRef> getValues() {
    return values;
  }

  /**
   * Returns the list of numeric values.
   */
  public List<Long> getNumericValues() {
    return numericValues;
  }

  @Override
  public String toString() {
    String numValuesStr = numericValues.stream().map(String::valueOf).collect(Collectors.joining(","));
    return "DocValues{" +
        "dvType=" + dvType +
        ", values=" + values +
        ", numericValues=[" + numValuesStr + "]" +
        '}';
  }
}
