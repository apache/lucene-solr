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
import org.junit.Test;

/**
 * Testcase for {@link org.apache.lucene.classification.BooleanPerceptronClassifier}
 */
public class BooleanPerceptronClassifierTest extends ClassificationTestBase<Boolean> {

  @Test
  public void testBasicUsage() throws Exception {
    checkCorrectClassification(new BooleanPerceptronClassifier(), TECHNOLOGY_INPUT, false, new MockAnalyzer(random()), textFieldName, booleanFieldName);
  }

  @Test
  public void testExplicitThreshold() throws Exception {
    checkCorrectClassification(new BooleanPerceptronClassifier(100d, 1), TECHNOLOGY_INPUT, false, new MockAnalyzer(random()), textFieldName, booleanFieldName);
  }

  @Test
  public void testBasicUsageWithQuery() throws Exception {
    checkCorrectClassification(new BooleanPerceptronClassifier(), TECHNOLOGY_INPUT, false, new MockAnalyzer(random()), textFieldName, booleanFieldName, new TermQuery(new Term(textFieldName, "it")));
  }

  @Test
  public void testPerformance() throws Exception {
    checkPerformance(new BooleanPerceptronClassifier(), new MockAnalyzer(random()), booleanFieldName);
  }

}
