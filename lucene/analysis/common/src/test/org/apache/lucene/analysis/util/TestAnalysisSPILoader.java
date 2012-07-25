package org.apache.lucene.analysis.util;

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

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilterFactory;
import org.apache.lucene.util.LuceneTestCase;

public class TestAnalysisSPILoader extends LuceneTestCase {
  
  public void testLookupTokenizer() {
    assertEquals(WhitespaceTokenizerFactory.class, TokenizerFactory.forName("Whitespace").getClass());
    assertEquals(WhitespaceTokenizerFactory.class, TokenizerFactory.forName("WHITESPACE").getClass());
    assertEquals(WhitespaceTokenizerFactory.class, TokenizerFactory.forName("whitespace").getClass());
  }
  
  public void testBogusLookupTokenizer() {
    try {
      TokenizerFactory.forName("sdfsdfsdfdsfsdfsdf");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
    
    try {
      TokenizerFactory.forName("!(**#$U*#$*");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
  }

  public void testLookupTokenizerClass() {
    assertEquals(WhitespaceTokenizerFactory.class, TokenizerFactory.lookupClass("Whitespace"));
    assertEquals(WhitespaceTokenizerFactory.class, TokenizerFactory.lookupClass("WHITESPACE"));
    assertEquals(WhitespaceTokenizerFactory.class, TokenizerFactory.lookupClass("whitespace"));
  }
  
  public void testBogusLookupTokenizerClass() {
    try {
      TokenizerFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
    
    try {
      TokenizerFactory.lookupClass("!(**#$U*#$*");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
  }
  
  public void testAvailableTokenizers() {
    assertTrue(TokenizerFactory.availableTokenizers().contains("whitespace"));
  }
  
  public void testLookupTokenFilter() {
    assertEquals(LowerCaseFilterFactory.class, TokenFilterFactory.forName("Lowercase").getClass());
    assertEquals(LowerCaseFilterFactory.class, TokenFilterFactory.forName("LOWERCASE").getClass());
    assertEquals(LowerCaseFilterFactory.class, TokenFilterFactory.forName("lowercase").getClass());
    
    assertEquals(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.forName("RemoveDuplicates").getClass());
    assertEquals(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.forName("REMOVEDUPLICATES").getClass());
    assertEquals(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.forName("removeduplicates").getClass());
  }
  
  public void testBogusLookupTokenFilter() {
    try {
      TokenFilterFactory.forName("sdfsdfsdfdsfsdfsdf");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
    
    try {
      TokenFilterFactory.forName("!(**#$U*#$*");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
  }

  public void testLookupTokenFilterClass() {
    assertEquals(LowerCaseFilterFactory.class, TokenFilterFactory.lookupClass("Lowercase"));
    assertEquals(LowerCaseFilterFactory.class, TokenFilterFactory.lookupClass("LOWERCASE"));
    assertEquals(LowerCaseFilterFactory.class, TokenFilterFactory.lookupClass("lowercase"));
    
    assertEquals(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.lookupClass("RemoveDuplicates"));
    assertEquals(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.lookupClass("REMOVEDUPLICATES"));
    assertEquals(RemoveDuplicatesTokenFilterFactory.class, TokenFilterFactory.lookupClass("removeduplicates"));
  }
  
  public void testBogusLookupTokenFilterClass() {
    try {
      TokenFilterFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
    
    try {
      TokenFilterFactory.lookupClass("!(**#$U*#$*");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
  }
  
  public void testAvailableTokenFilters() {
    assertTrue(TokenFilterFactory.availableTokenFilters().contains("lowercase"));
    assertTrue(TokenFilterFactory.availableTokenFilters().contains("removeduplicates"));
  }
  
  public void testLookupCharFilter() {
    assertEquals(HTMLStripCharFilterFactory.class, CharFilterFactory.forName("HTMLStrip").getClass());
    assertEquals(HTMLStripCharFilterFactory.class, CharFilterFactory.forName("HTMLSTRIP").getClass());
    assertEquals(HTMLStripCharFilterFactory.class, CharFilterFactory.forName("htmlstrip").getClass());
  }
  
  public void testBogusLookupCharFilter() {
    try {
      CharFilterFactory.forName("sdfsdfsdfdsfsdfsdf");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
    
    try {
      CharFilterFactory.forName("!(**#$U*#$*");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
  }

  public void testLookupCharFilterClass() {
    assertEquals(HTMLStripCharFilterFactory.class, CharFilterFactory.lookupClass("HTMLStrip"));
    assertEquals(HTMLStripCharFilterFactory.class, CharFilterFactory.lookupClass("HTMLSTRIP"));
    assertEquals(HTMLStripCharFilterFactory.class, CharFilterFactory.lookupClass("htmlstrip"));
  }
  
  public void testBogusLookupCharFilterClass() {
    try {
      CharFilterFactory.lookupClass("sdfsdfsdfdsfsdfsdf");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
    
    try {
      CharFilterFactory.lookupClass("!(**#$U*#$*");
      fail();
    } catch (IllegalArgumentException expected) {
      //
    }
  }
  
  public void testAvailableCharFilters() {
    assertTrue(CharFilterFactory.availableCharFilters().contains("htmlstrip"));
  }
}
