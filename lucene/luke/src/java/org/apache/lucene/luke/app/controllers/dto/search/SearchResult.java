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

package org.apache.lucene.luke.app.controllers.dto.search;

import org.apache.lucene.luke.models.search.SearchResults;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SearchResult {
  private int docId;
  private float score;
  private String values;

  public static SearchResult of(SearchResults.Doc doc) {
    SearchResult res = new SearchResult();
    res.docId = doc.getDocId();
    res.score = doc.getScore();
    List<String> concatValues = doc.getFieldValues().entrySet().stream().map(e -> {
      String v = String.join(",", Arrays.asList(e.getValue()));
      return e.getKey() + "=" + v + ";";
    }).collect(Collectors.toList());
    res.values = String.join(" ", concatValues);
    return res;
  }

  private SearchResult() {
  }

  public int getDocId() {
    return docId;
  }

  public float getScore() {
    return score;
  }

  public String getValues() {
    return values;
  }
}
