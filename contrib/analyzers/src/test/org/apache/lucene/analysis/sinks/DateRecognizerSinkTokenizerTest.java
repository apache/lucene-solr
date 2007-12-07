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

import junit.framework.TestCase;
import org.apache.lucene.analysis.TeeTokenFilter;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.Token;

import java.io.StringReader;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class DateRecognizerSinkTokenizerTest extends TestCase {


  public DateRecognizerSinkTokenizerTest(String s) {
    super(s);
  }

  protected void setUp() {
  }

  protected void tearDown() {

  }

  public void test() throws IOException {
    DateRecognizerSinkTokenizer sink = new DateRecognizerSinkTokenizer(new SimpleDateFormat("MM/dd/yyyy"));
    String test = "The quick red fox jumped over the lazy brown dogs on 7/11/2006  The dogs finally reacted on 7/12/2006";
    TeeTokenFilter tee = new TeeTokenFilter(new WhitespaceTokenizer(new StringReader(test)), sink);
    Token tok = null;
    int count = 0;
    while ((tok = tee.next()) != null){
      assertTrue("tok is null and it shouldn't be", tok != null);
      if (tok.termBuffer()[0] == '7'){
        assertTrue(tok.type() + " is not equal to " + DateRecognizerSinkTokenizer.DATE_TYPE,
                tok.type().equals(DateRecognizerSinkTokenizer.DATE_TYPE) == true);
      }
      count++;
    }
    assertTrue(count + " does not equal: " + 18, count == 18);
    assertTrue("sink Size: " + sink.getTokens().size() + " is not: " + 2, sink.getTokens().size() == 2);

  }
}