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

package org.apache.solr.search;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class SignificantTermsQParserPluginTest extends LuceneTestCase {

  /**
   * Test the backwards compatibility for a typo in the SignificantTermsQParserPlugin. It will fail if the backwards
   * compatibility is broken.
   */
  @Test
  public void testQParserBackwardsCompatibility() {
    // for Solr 7.x backcompat only
    assertEquals("sigificantTerms", SignificantTermsQParserPlugin.OLD_NAME);
    assertEquals(SignificantTermsQParserPlugin.class,
        QParserPlugin.standardPlugins.get(SignificantTermsQParserPlugin.OLD_NAME).getClass());

    assertEquals("significantTerms", SignificantTermsQParserPlugin.NAME);
    assertEquals(SignificantTermsQParserPlugin.class,
        QParserPlugin.standardPlugins.get(SignificantTermsQParserPlugin.NAME).getClass());
  }
}
