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

package org.apache.lucene.luke.models.overview;

import java.util.List;

import org.junit.Test;

public class TopTermsTest extends OverviewTestBase {

  @Test
  public void testGetTopTerms() throws Exception {
    TopTerms topTerms = new TopTerms(reader);
    List<TermStats> result = topTerms.getTopTerms("f2", 2);

    assertEquals("a", result.get(0).getDecodedTermText());
    assertEquals(3, result.get(0).getDocFreq());
    assertEquals("f2", result.get(0).getField());

    assertEquals("c", result.get(1).getDecodedTermText());
    assertEquals(2, result.get(1).getDocFreq());
    assertEquals("f2", result.get(1).getField());
  }

}
