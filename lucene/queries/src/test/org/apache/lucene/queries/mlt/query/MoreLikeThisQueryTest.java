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

package org.apache.lucene.queries.mlt.query;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.queries.mlt.MoreLikeThisTestBase;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class MoreLikeThisQueryTest extends MoreLikeThisTestBase {
  private MoreLikeThisQuery queryToTest;

  protected Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

  @Override
  public void setUp() throws Exception {
    super.setUp();
    initIndex();
  }

  @Test
  public void seedFieldNameConfigured_shouldRewriteUsingSeedFieldName() throws IOException {
    String seedText = "1a 2a 4a 3b";
    queryToTest = new MoreLikeThisQuery(seedText,new String[]{FIELD1}, analyzer);

    Query actualMltQuery = queryToTest.rewrite(reader);

    assertThat(actualMltQuery.toString(), is("field1:1a field1:2a field1:4a"));
  }

  @Test
  public void seedFieldNamesConfigured_shouldRewriteUsingAllFieldNames() throws IOException {
    String seedText = "1a 2a 4a 3b";
    queryToTest = new MoreLikeThisQuery(seedText,new String[]{FIELD1,FIELD2}, analyzer);

    Query actualMltQuery = queryToTest.rewrite(reader);

    assertThat(actualMltQuery.toString(), is("(field1:1a field1:2a field2:3b field1:4a)~1"));
  }
}
