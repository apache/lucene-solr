package org.apache.lucene.analysis.ru;

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
 * Testcase for {@link RussianLetterTokenizer}
 * @deprecated (3.1) Remove this test class in Lucene 5.0
 */
@Deprecated
public class TestRussianLetterTokenizer extends BaseTokenStreamTestCase {
  
  public void testRussianLetterTokenizer() throws IOException {
    StringReader reader = new StringReader("1234567890 Вместе \ud801\udc1ctest");
    RussianLetterTokenizer tokenizer = new RussianLetterTokenizer(Version.LUCENE_CURRENT,
        reader);
    assertTokenStreamContents(tokenizer, new String[] {"1234567890", "Вместе",
        "\ud801\udc1ctest"});
  }
  
  public void testRussianLetterTokenizerBWCompat() throws IOException {
    StringReader reader = new StringReader("1234567890 Вместе \ud801\udc1ctest");
    RussianLetterTokenizer tokenizer = new RussianLetterTokenizer(Version.LUCENE_30,
        reader);
    assertTokenStreamContents(tokenizer, new String[] {"1234567890", "Вместе", "test"});
  }
}
