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

package org.apache.lucene.queries.mlt.terms;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class DocumentTermFrequenciesTest {

  private DocumentTermFrequencies toTest = new DocumentTermFrequencies();

  @Test
  public void getUnknownField_shouldCreateEmptyFieldTermFrequencies() {
    toTest = new DocumentTermFrequencies();
    assertNotNull(toTest.get("First"));
  }

  @Test
  public void incrementFieldFirstTime_shouldUpdateFieldTermFrequencies() {
    toTest = new DocumentTermFrequencies();

    String unknownField = "First";
    String unknownTerm = "term1";
    toTest.increment(unknownField, unknownTerm, 5);

    assertNotNull(toTest.get(unknownField));
    assertThat(toTest.get(unknownField).get(unknownTerm).frequency, is(5));
  }

  @Test
  public void incrementFieldSecondTime_shouldUpdateFieldTermFrequencies() {
    toTest = new DocumentTermFrequencies();
    String unknownField = "First";
    String unknownTerm = "term1";
    toTest.increment(unknownField, unknownTerm, 5);

    assertNotNull(toTest.get(unknownField));
    assertThat(toTest.get(unknownField).get(unknownTerm).frequency, is(5));

    toTest.increment(unknownField, unknownTerm, 3);

    assertThat(toTest.get(unknownField).get(unknownTerm).frequency, is(8));
  }

}
