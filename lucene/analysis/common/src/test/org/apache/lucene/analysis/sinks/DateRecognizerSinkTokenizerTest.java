package org.apache.lucene.analysis.sinks;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;

public class DateRecognizerSinkTokenizerTest extends BaseTokenStreamTestCase {

  public void test() throws IOException {
    DateRecognizerSinkFilter sinkFilter = new DateRecognizerSinkFilter(new SimpleDateFormat("MM/dd/yyyy", Locale.ROOT));
    String test = "The quick red fox jumped over the lazy brown dogs on 7/11/2006  The dogs finally reacted on 7/12/2006";
    TeeSinkTokenFilter tee = new TeeSinkTokenFilter(new MockTokenizer(new StringReader(test), MockTokenizer.WHITESPACE, false));
    TeeSinkTokenFilter.SinkTokenStream sink = tee.newSinkTokenStream(sinkFilter);
    int count = 0;
    
    tee.reset();
    while (tee.incrementToken()) {
      count++;
    }
    assertTrue(count + " does not equal: " + 18, count == 18);
    
    int sinkCount = 0;
    sink.reset();
    while (sink.incrementToken()) {
      sinkCount++;
    }
    assertTrue("sink Size: " + sinkCount + " is not: " + 2, sinkCount == 2);

  }
}