package org.apache.lucene.analysis.ar;

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

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.Version;

/**
 * Testcase for {@link TestArabicLetterTokenizer}
 * @deprecated (3.1) Remove in Lucene 5.0
 */
@Deprecated
public class TestArabicLetterTokenizer extends BaseTokenStreamTestCase {
  
  public void testArabicLetterTokenizer() throws IOException {
    StringReader reader = new StringReader("1234567890 Tokenizer \ud801\udc1c\u0300test");
    ArabicLetterTokenizer tokenizer = new ArabicLetterTokenizer(Version.LUCENE_31,
        reader);
    assertTokenStreamContents(tokenizer, new String[] {"Tokenizer",
        "\ud801\udc1c\u0300test"});
  }
  
  public void testArabicLetterTokenizerBWCompat() throws IOException {
    StringReader reader = new StringReader("1234567890 Tokenizer \ud801\udc1c\u0300test");
    ArabicLetterTokenizer tokenizer = new ArabicLetterTokenizer(Version.LUCENE_30,
        reader);
    assertTokenStreamContents(tokenizer, new String[] {"Tokenizer", "\u0300test"});
  }
}
