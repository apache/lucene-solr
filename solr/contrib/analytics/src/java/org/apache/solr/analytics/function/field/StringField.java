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
package org.apache.solr.analytics.function.field;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.value.StringValue.CastingStringValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.StrField;

/**
 * An analytics wrapper for a single-valued {@link StrField} with DocValues enabled.
 */
public class StringField extends AnalyticsField implements CastingStringValue {
  private BinaryDocValues binaryDocValues;
  private SortedDocValues sortedDocValues;
  String value;
  boolean exists;

  public StringField(String fieldName) {
    super(fieldName);
    exists = false;
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(fieldName);
    if (fieldInfo == null) {
      binaryDocValues = DocValues.emptyBinary();
      sortedDocValues = null;
    } else {
      switch (fieldInfo.getDocValuesType()) {
        case BINARY:
          binaryDocValues = DocValues.getBinary(context.reader(), fieldName);
          sortedDocValues = null;
          break;
        case SORTED:
          binaryDocValues = null;
          sortedDocValues = DocValues.getSorted(context.reader(), fieldName);
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid doc values type " + fieldInfo.getDocValuesType());
      }
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    if (binaryDocValues == null) {
      exists = sortedDocValues.advanceExact(doc);
      if (exists) {
        value = sortedDocValues.lookupOrd(sortedDocValues.ordValue()).utf8ToString();
      }
    } else {
      exists = binaryDocValues.advanceExact(doc);
      if (exists) {
        value = binaryDocValues.binaryValue().utf8ToString();
      }
    }
  }

  @Override
  public String getString() {
    return exists ? value : null;
  }
  @Override
  public Object getObject() {
    return exists ? value : null;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public void streamStrings(Consumer<String> cons) {
    if (exists) {
      cons.accept(value);
    }
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    if (exists) {
      cons.accept(value);
    }
  }

  @Override
  public ExpressionComparator<String> getObjectComparator(String expression) {
    return new ExpressionComparator<>(expression);
  }
}
