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
package org.apache.lucene.classification;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Testcase for {@link KNearestNeighborClassifier}
 */
public class KNearestNeighborClassifierTest extends ClassificationTestBase<BytesRef> {

  @Test
  public void testBasicUsage() throws Exception {
    // usage with default MLT min docs / term freq
    checkCorrectClassification(new KNearestNeighborClassifier(3), POLITICS_INPUT, POLITICS_RESULT, new MockAnalyzer(random()), textFieldName, categoryFieldName);
    // usage without custom min docs / term freq for MLT
    checkCorrectClassification(new KNearestNeighborClassifier(3, 2, 1), TECHNOLOGY_INPUT, TECHNOLOGY_RESULT, new MockAnalyzer(random()), textFieldName, categoryFieldName);
  }

  @Test
  public void testBasicUsageWithQuery() throws Exception {
    checkCorrectClassification(new KNearestNeighborClassifier(1), TECHNOLOGY_INPUT, TECHNOLOGY_RESULT, new MockAnalyzer(random()), textFieldName, categoryFieldName, new TermQuery(new Term(textFieldName, "it")));
  }

  @Test
  public void testPerformance() throws Exception {
    checkPerformance(new KNearestNeighborClassifier(100), new MockAnalyzer(random()), categoryFieldName);
  }

}