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

package org.apache.lucene.queries.mlt.terms.scorer;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;

/**
* This class has the responsibility of calculating a score for a term.
 * The score will measure how much interesting the term is in the field given :
 * - term stats local to the field content
 * - field stats global to the index
 */
public interface TermScorer {
  float score(String fieldName, CollectionStatistics fieldStats, TermStatistics termStats, float termFrequency) throws IOException;

  Similarity.SimWeight getSimilarityStats(String fieldName, CollectionStatistics fieldStats, TermStatistics termStats, float termFrequency) throws IOException;

  void setField2normsFromIndex(Map<String, NumericDocValues> field2normsFromIndex);

  void setField2norm(Map<String, Float> field2norm);

  void setDocId(int docId);

  void setTextNorm(float textNorm);

  }
