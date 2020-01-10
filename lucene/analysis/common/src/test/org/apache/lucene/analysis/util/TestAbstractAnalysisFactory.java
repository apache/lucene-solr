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

package org.apache.lucene.analysis.util;

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.util.LuceneTestCase;

public class TestAbstractAnalysisFactory extends LuceneTestCase {

  public void testLookupTokenizerSPIName() throws NoSuchFieldException, IllegalAccessException {
    assertEquals("whitespace", AbstractAnalysisFactory.lookupSPIName(WhitespaceTokenizerFactory.class));
    assertEquals("whitespace", TokenizerFactory.findSPIName(WhitespaceTokenizerFactory.class));
  }

  public void testLookupCharFilterSPIName() throws NoSuchFieldException, IllegalAccessException {
    assertEquals("htmlStrip", AbstractAnalysisFactory.lookupSPIName(HTMLStripCharFilterFactory.class));
    assertEquals("htmlStrip", CharFilterFactory.findSPIName(HTMLStripCharFilterFactory.class));
  }

  public void testLookupTokenFilterSPIName() throws NoSuchFieldException, IllegalAccessException{
    assertEquals("lowercase", AbstractAnalysisFactory.lookupSPIName(LowerCaseFilterFactory.class));
    assertEquals("lowercase", TokenFilterFactory.findSPIName(LowerCaseFilterFactory.class));
  }
}
