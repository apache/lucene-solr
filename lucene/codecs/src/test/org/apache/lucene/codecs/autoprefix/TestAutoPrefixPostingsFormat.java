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
package org.apache.lucene.codecs.autoprefix;


import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.RandomPostingsTester;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests AutoPrefix's postings
 */

// NOTE: we don't extend BasePostingsFormatTestCase becase we can only handle DOCS_ONLY fields:

public class TestAutoPrefixPostingsFormat extends LuceneTestCase {
  public void test() throws Exception {
    new RandomPostingsTester(random()).testFull(TestUtil.alwaysPostingsFormat(new AutoPrefixPostingsFormat()),
                                                createTempDir("autoprefix"),
                                                IndexOptions.DOCS,
                                                false);
  }
}
