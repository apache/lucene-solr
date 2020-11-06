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
package org.apache.solr.handler.clustering;

import org.carrot2.clustering.Document;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Representation of a single logical "document" for clustering.
 */
final class InputDocument implements Document {
  private final Object id;
  private final Map<String, String> clusteredFields = new LinkedHashMap<>();
  private final String language;

  InputDocument(Object docId, String language) {
    this.id = Objects.requireNonNull(docId);
    this.language = language;
  }

  @Override
  public void visitFields(BiConsumer<String, String> fieldConsumer) {
    clusteredFields.forEach(fieldConsumer);
  }

  Object getId() {
    return id;
  }

  String language() {
    return language;
  }

  void addClusteredField(String fieldName, String fieldValue) {
    assert !clusteredFields.containsKey(fieldName);
    clusteredFields.put(fieldName, fieldValue);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT,
        "doc[%s, lang=%s, fields=%s]",
        getId(),
        language,
        clusteredFields.entrySet().stream().map(e -> e.getKey() + ": " + e.getValue()).collect(Collectors.joining(", ")));
  }
}
