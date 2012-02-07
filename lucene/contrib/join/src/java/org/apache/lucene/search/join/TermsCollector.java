package org.apache.lucene.search.join;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A collector that collects all terms from a specified field matching the query.
 *
 * @lucene.experimental
 */
class TermsCollector extends Collector {

  final String field;
  final Set<String> collectorTerms = new HashSet<String>();
  
  String[] fromDocTerms;

  TermsCollector(String field) {
    this.field = field;
  }

  public Set<String> getCollectorTerms() {
    return collectorTerms;
  }

  public void setScorer(Scorer scorer) throws IOException {
  }

  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  public void collect(int doc) throws IOException {
    collectorTerms.add(fromDocTerms[doc]);
  }

  public void setNextReader(IndexReader indexReader, int docBase) throws IOException {
    fromDocTerms = FieldCache.DEFAULT.getStrings(indexReader, field);
  }

}
