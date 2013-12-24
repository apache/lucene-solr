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
import org.apache.lucene.search.similarities.Similarity;

public class FieldDef {
  public final String name;
  public final FieldType fieldType;
  public final FieldType fieldTypeNoDV;
  public final String valueType;
  public final String faceted;
  public final String postingsFormat;
  public final String docValuesFormat;
  public final boolean singleValued;
  public final Similarity sim;
  public final Analyzer indexAnalyzer;
  public final Analyzer searchAnalyzer;
  public final boolean highlighted;
  public final String liveValuesIDField;
  public final String blendFieldName;
  public final float blendMaxBoost;
  public final long blendRange;

  public FieldDef(String name, FieldType fieldType, String valueType, String faceted, String postingsFormat, String docValuesFormat, boolean singleValued,
                  Similarity sim, Analyzer indexAnalyzer, Analyzer searchAnalyzer, boolean highlighted, String liveValuesIDField,
                  String blendFieldName, float blendMaxBoost, long blendRange) {
    this.name = name;
    this.fieldType = fieldType;
    if (fieldType != null) {
      fieldType.freeze();
    }
    this.valueType = valueType;
    this.faceted = faceted;
    this.postingsFormat = postingsFormat;
    this.docValuesFormat = docValuesFormat;
    this.singleValued = singleValued;
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
    // nocommit make this a subclass somehow
    this.blendFieldName = blendFieldName;
    this.blendMaxBoost = blendMaxBoost;
    this.blendRange = blendRange;
  }
}
