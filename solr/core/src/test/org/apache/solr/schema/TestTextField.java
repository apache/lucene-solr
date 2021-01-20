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

package org.apache.solr.schema;

import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.Test;

/**
 * Tests directly {@link org.apache.solr.schema.TextField} methods.
 */
public class TestTextField extends SolrTestCaseJ4 {

  @Test
  public void testAnalyzeMultiTerm() {
    // No terms provided by the StopFilter (stop word) for the multi-term part.
    // This is supported. Check TextField.analyzeMultiTerm returns null (and does not throw an exception).
    BytesRef termBytes = TextField.analyzeMultiTerm("field", "the", new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET));
    assertNull(termBytes);

    // One term provided by the WhitespaceTokenizer for the multi-term part.
    // This is the regular case. Check TextField.analyzeMultiTerm returns it (and does not throw an exception).
    termBytes = TextField.analyzeMultiTerm("field", "Sol", new WhitespaceAnalyzer());
    assertEquals("Sol", termBytes.utf8ToString());

    // Two terms provided by the WhitespaceTokenizer for the multi-term part.
    // This is not allowed. Expect an exception.
    SolrException exception = expectThrows(SolrException.class, () -> TextField.analyzeMultiTerm("field", "term1 term2", new WhitespaceAnalyzer()));
    assertEquals("Unexpected error code", SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
  }
}