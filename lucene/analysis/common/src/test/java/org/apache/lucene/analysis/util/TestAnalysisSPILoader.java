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


import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilterFactory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

public class TestAnalysisSPILoader extends LuceneTestCase {
  
  private Map<String,String> versionArgOnly() {
    return new HashMap<String,String>() {{
      put("luceneMatchVersion", Version.LATEST.toString());
    }};
  }
  
  public void testLookupTokenizer() {
    assertSame(WhitespaceTokenizerFactory.class, TokenizerFactory.forName("Whitespace", versionArgOnly()).getClass());
    assertSame(WhitespaceTokenizerFactory.class, TokenizerFactory.forName("WHITESPACE", versionArgOnly()).getClass());
    assertSame(WhitespaceTokenizerFactory.class, TokenizerFactory.forName("whitespace", versionArgOnly()).getClass());
  }
  
  public void testBogusLookupTokenizer() {
    expectThrows(IllegalArgumentException.class, () -> {
      TokenizerFactory.forName("sdfsdfsdfdsfsdfsdf", new HashMap<String,String>());
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      TokenizerFactory.forName("!(**#$U*#$*", new HashMap<String,String>());
    });
  }

  public void testLookupTokenizerClass() {
    assertSame(WhitespaceTokenizerFactory.class, TokenizerFactory.lookupClass("Whitespace"));
    assertSame(WhitespaceTokenizerFactory.class, TokenizerFactory.lookupClass("WHITESPACE"));
    assertSame(WhitespaceTokenizerFactory.class, TokenizerFactory.lookupClass("whitespace"));
  }
  
  public void testBogusLookupTokenizerClass() {
    expectThrows(IllegalArgumentException.class, () -> {
      TokenizerFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      TokenizerFactory.lookupClass("!(**#$U*#$*");
    });
  }
  
  public void testAvailableTokenizers() {
    assertTrue(TokenizerFactory.availableTokenizers().contains("whitespace"));
  }
  
  public void testLookupTokenFilter() {
    assertSame(LowerCaseFilterFactory.class, TokenFilterFactory.forName("Lowercase", versionArgOnly()).getClass());
    assertSame(LowerCaseFilterFactory.class, TokenFilterFactory.forName("LOWERCASE", versionArgOnly()).getClass());
    assertSame(LowerCaseFilterFactory.class, TokenFilterFactory.forName("lowercase", versionArgOnly()).getClass());
    
    assertSame(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.forName("RemoveDuplicates", versionArgOnly()).getClass());
    assertSame(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.forName("REMOVEDUPLICATES", versionArgOnly()).getClass());
    assertSame(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.forName("removeduplicates", versionArgOnly()).getClass());
  }
  
  public void testBogusLookupTokenFilter() {
    expectThrows(IllegalArgumentException.class, () -> {
      TokenFilterFactory.forName("sdfsdfsdfdsfsdfsdf", new HashMap<String,String>());
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      TokenFilterFactory.forName("!(**#$U*#$*", new HashMap<String,String>());
    });
  }

  public void testLookupTokenFilterClass() {
    assertSame(LowerCaseFilterFactory.class, TokenFilterFactory.lookupClass("Lowercase"));
    assertSame(LowerCaseFilterFactory.class, TokenFilterFactory.lookupClass("LOWERCASE"));
    assertSame(LowerCaseFilterFactory.class, TokenFilterFactory.lookupClass("lowercase"));
    
    assertSame(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.lookupClass("RemoveDuplicates"));
    assertSame(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.lookupClass("REMOVEDUPLICATES"));
    assertSame(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.lookupClass("removeduplicates"));
  }
  
  public void testBogusLookupTokenFilterClass() {
    expectThrows(IllegalArgumentException.class, () -> {
      TokenFilterFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      TokenFilterFactory.lookupClass("!(**#$U*#$*");
    });
  }
  
  public void testAvailableTokenFilters() {
    assertTrue(TokenFilterFactory.availableTokenFilters().contains("lowercase"));
    assertTrue(TokenFilterFactory.availableTokenFilters().contains("removeduplicates"));
  }
  
  public void testLookupCharFilter() {
    assertSame(HTMLStripCharFilterFactory.class, CharFilterFactory.forName("HTMLStrip", versionArgOnly()).getClass());
    assertSame(HTMLStripCharFilterFactory.class, CharFilterFactory.forName("HTMLSTRIP", versionArgOnly()).getClass());
    assertSame(HTMLStripCharFilterFactory.class, CharFilterFactory.forName("htmlstrip", versionArgOnly()).getClass());
  }
  
  public void testBogusLookupCharFilter() {
    expectThrows(IllegalArgumentException.class, () -> {
      CharFilterFactory.forName("sdfsdfsdfdsfsdfsdf", new HashMap<String,String>());
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      CharFilterFactory.forName("!(**#$U*#$*", new HashMap<String,String>());
    });
  }

  public void testLookupCharFilterClass() {
    assertSame(HTMLStripCharFilterFactory.class, CharFilterFactory.lookupClass("HTMLStrip"));
    assertSame(HTMLStripCharFilterFactory.class, CharFilterFactory.lookupClass("HTMLSTRIP"));
    assertSame(HTMLStripCharFilterFactory.class, CharFilterFactory.lookupClass("htmlstrip"));
  }
  
  public void testBogusLookupCharFilterClass() {
    expectThrows(IllegalArgumentException.class, () -> {
      CharFilterFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      CharFilterFactory.lookupClass("!(**#$U*#$*");
    });
  }
  
  public void testAvailableCharFilters() {
    assertTrue(CharFilterFactory.availableCharFilters().contains("htmlstrip"));
  }
}
