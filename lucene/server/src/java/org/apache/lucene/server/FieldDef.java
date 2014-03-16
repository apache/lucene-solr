package org.apache.lucene.server;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.similarities.Similarity;

/** Defines the type of one field. */
public class FieldDef {
  /** Field name. */
  public final String name;

  /** {@link FieldType}, used during indexing. */
  public final FieldType fieldType;

  /** {@link FieldType} minus doc values, used during
   * indexing because we separately add the doc values field
   * and then the "normal" field. */
  public final FieldType fieldTypeNoDV;

  // nocommit use enum:
  /** Value type (atom, text, boolean, etc.). */
  public final String valueType;

  // nocommit use enum:
  /** Facet type (no, flat, hierarchical, numericRange, sortedSetDocValues). */
  public final String faceted;

  /** Postings format (codec). */
  public final String postingsFormat;

  /** Doc values format (codec). */
  public final String docValuesFormat;

  /** True if the field is multi valued. */
  public final boolean multiValued;

  /** {@link Similarity} to use during indexing searching. */
  public final Similarity sim;

  /** Index-time {@link Analyzer}. */
  public final Analyzer indexAnalyzer;

  /** Search-time {@link Analyzer}. */
  public final Analyzer searchAnalyzer;

  /** True if the field will be highlighted. */
  public final boolean highlighted;

  /** Field name to use as the ID field for live-values. */
  public final String liveValuesIDField;

  /** Only set for a virtual field (expression). */
  public final ValueSource valueSource;

  /** Sole constructor. */
  public FieldDef(String name, FieldType fieldType, String valueType, String faceted,
                  String postingsFormat, String docValuesFormat, boolean multiValued,
                  Similarity sim, Analyzer indexAnalyzer, Analyzer searchAnalyzer, boolean highlighted, String liveValuesIDField,
                  ValueSource valueSource) {
    this.name = name;
    this.fieldType = fieldType;
    if (fieldType != null) {
      fieldType.freeze();
    }
    this.valueType = valueType;
    this.faceted = faceted;
    this.postingsFormat = postingsFormat;
    this.docValuesFormat = docValuesFormat;
    this.multiValued = multiValued;
    this.sim = sim;
    this.indexAnalyzer = indexAnalyzer;
    this.searchAnalyzer = searchAnalyzer;
    this.highlighted = highlighted;
    this.liveValuesIDField = liveValuesIDField;
    // nocommit messy:
    if (fieldType != null) {
      fieldTypeNoDV = new FieldType(fieldType);
      fieldTypeNoDV.setDocValueType(null);
      fieldTypeNoDV.freeze();
    } else {
      fieldTypeNoDV = null;
    }
    this.valueSource = valueSource;
  }
}
