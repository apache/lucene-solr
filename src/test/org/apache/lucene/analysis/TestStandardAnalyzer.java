package org.apache.lucene.analysis;

import junit.framework.TestCase;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.io.StringReader;

/**
 * Copyright 2004 The Apache Software Foundation
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class TestStandardAnalyzer extends TestCase {

  public void assertAnalyzesTo(Analyzer a, String input, String[] expected) throws Exception {
    TokenStream ts = a.tokenStream("dummy", new StringReader(input));
    for (int i = 0; i < expected.length; i++) {
      Token t = ts.next();
      assertNotNull(t);
      assertEquals(expected[i], t.termText());
    }
    assertNull(ts.next());
    ts.close();
  }


  public void testStandard() throws Exception {
    Analyzer a = new StandardAnalyzer();

    // alphanumeric tokens
    assertAnalyzesTo(a, "B2B", new String[]{"b2b"});
    assertAnalyzesTo(a, "2B", new String[]{"2b"});

    // underscores are delimiters, but not in email addresses (below)
    assertAnalyzesTo(a, "word_having_underscore", new String[]{"word", "having", "underscore"});
    assertAnalyzesTo(a, "word_with_underscore_and_stopwords", new String[]{"word", "underscore", "stopwords"});

    // other delimiters: "-", "/", ","
    assertAnalyzesTo(a, "some-dashed-phrase",   new String[]{"some", "dashed", "phrase" });
    assertAnalyzesTo(a, "dogs,chase,cats", new String[]{"dogs", "chase", "cats"});
    assertAnalyzesTo(a, "ac/dc", new String[]{"ac", "dc"});

    // internal apostrophes: O'Reilly, you're, O'Reilly's
    // possessives are actually removed by StardardFilter, not the tokenizer
    assertAnalyzesTo(a, "O'Reilly", new String[]{"o'reilly"});
    assertAnalyzesTo(a, "you're", new String[]{"you're"});
    assertAnalyzesTo(a, "O'Reilly's", new String[]{"o'reilly"});

    // company names
    assertAnalyzesTo(a, "AT&T", new String[]{"at&t"});
    assertAnalyzesTo(a, "Excite@Home", new String[]{"excite@home"});

    // domain names
    assertAnalyzesTo(a, "www.nutch.org",   new String[]{"www.nutch.org" });

    // email addresses, possibly with underscores, periods, etc
    assertAnalyzesTo(a, "test@example.com", new String[]{"test@example.com"});
    assertAnalyzesTo(a, "first.lastname@example.com", new String[]{"first.lastname@example.com"});
    assertAnalyzesTo(a, "first_lastname@example.com", new String[]{"first_lastname@example.com"});

    // floating point, serial, model numbers, ip addresses, etc.
    // every other segment must have at least one digit
    assertAnalyzesTo(a, "21.35", new String[]{"21.35"});
    assertAnalyzesTo(a, "R2D2 C3PO", new String[]{"r2d2", "c3po"});
    assertAnalyzesTo(a, "216.239.63.104",   new String[]{"216.239.63.104"});
    assertAnalyzesTo(a, "1-2-3",   new String[]{"1-2-3"});
    assertAnalyzesTo(a, "a1-b2-c3",   new String[]{"a1-b2-c3"});
    assertAnalyzesTo(a, "a1-b-c3",   new String[]{"a1-b-c3"});

    // numbers
    assertAnalyzesTo(a, "David has 5000 bones", new String[]{"david", "has", "5000", "bones"});

    // various
    assertAnalyzesTo(a, "C embedded developers wanted", new String[]{"c", "embedded", "developers", "wanted" });
    assertAnalyzesTo(a, "foo bar FOO BAR", new String[]{"foo", "bar", "foo", "bar"});
    assertAnalyzesTo(a, "foo      bar .  FOO <> BAR", new String[]{"foo", "bar", "foo", "bar"});
    assertAnalyzesTo(a, "\"QUOTED\" word", new String[]{"quoted", "word"});

    // acronyms have their dots stripped
    assertAnalyzesTo(a, "U.S.A.", new String[]{ "usa" });

    // It would be nice to change the grammar in StandardTokenizer.jj to make "C#" and "C++" end up as tokens.
    assertAnalyzesTo(a, "C++", new String[]{"c"});
    assertAnalyzesTo(a, "C#", new String[]{"c"});

  }
}
