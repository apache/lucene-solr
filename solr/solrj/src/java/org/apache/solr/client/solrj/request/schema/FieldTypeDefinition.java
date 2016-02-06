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
package org.apache.solr.client.solrj.request.schema;

import java.util.Map;

public class FieldTypeDefinition {
  private Map<String, Object> attributes;

  private AnalyzerDefinition analyzer;

  private AnalyzerDefinition indexAnalyzer;

  private AnalyzerDefinition queryAnalyzer;

  private AnalyzerDefinition multiTermAnalyzer;

  private Map<String, Object> similarity;

  public FieldTypeDefinition() {
  }

  public Map<String, Object> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, Object> attributes) {
    this.attributes = attributes;
  }

  public Map<String, Object> getSimilarity() {
    return similarity;
  }

  public void setSimilarity(Map<String, Object> similarity) {
    this.similarity = similarity;
  }

  public AnalyzerDefinition getAnalyzer() {
    return analyzer;
  }

  public void setAnalyzer(AnalyzerDefinition analyzer) {
    this.analyzer = analyzer;
  }

  public AnalyzerDefinition getIndexAnalyzer() {
    return indexAnalyzer;
  }

  public void setIndexAnalyzer(AnalyzerDefinition indexAnalyzer) {
    this.indexAnalyzer = indexAnalyzer;
  }

  public AnalyzerDefinition getQueryAnalyzer() {
    return queryAnalyzer;
  }

  public void setQueryAnalyzer(AnalyzerDefinition queryAnalyzer) {
    this.queryAnalyzer = queryAnalyzer;
  }

  public AnalyzerDefinition getMultiTermAnalyzer() {
    return multiTermAnalyzer;
  }

  public void setMultiTermAnalyzer(AnalyzerDefinition multiTermAnalyzer) {
    this.multiTermAnalyzer = multiTermAnalyzer;
  }
}


