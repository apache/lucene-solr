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
package org.apache.lucene.analysis.miscellaneous;


import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure the simple truncation filter factory is working.
 */
public class TestProtectedTermFilterFactory extends BaseTokenStreamFactoryTestCase {
  public void testInform() throws Exception {
    ProtectedTermFilterFactory factory = (ProtectedTermFilterFactory)tokenFilterFactory("ProtectedTerm",
        "protected", "protected-1.txt", "ignoreCase", "true", "wrappedFilters", "lowercase");
    CharArraySet protectedTerms = factory.getProtectedTerms();
    assertTrue("protectedTerms is null and it shouldn't be", protectedTerms != null);
    assertTrue("protectedTerms Size: " + protectedTerms.size() + " is not: " + 2, protectedTerms.size() == 2);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    factory = (ProtectedTermFilterFactory)tokenFilterFactory("ProtectedTerm",
        "protected", "protected-1.txt, protected-2.txt", "ignoreCase", "true", "wrappedFilters", "lowercase");
    protectedTerms = factory.getProtectedTerms();
    assertTrue("protectedTerms is null and it shouldn't be", protectedTerms != null);
    assertTrue("protectedTerms Size: " + protectedTerms.size() + " is not: " + 4, protectedTerms.size() == 4);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    // defaults
    factory = (ProtectedTermFilterFactory)tokenFilterFactory("ProtectedTerm",
        "protected", "protected-1.txt");
    assertEquals(false, factory.isIgnoreCase());
  }

  public void testBasic() throws Exception {
    String str = "Foo Clara Bar David";
    TokenStream stream = whitespaceMockTokenizer(str);
    stream = tokenFilterFactory("ProtectedTerm", "ignoreCase", "true",
        "protected", "protected-1.txt", "wrappedFilters", "lowercase").create(stream);
    assertTokenStreamContents(stream, new String[]{"Foo", "clara", "Bar", "david"});
  }

  public void testMultipleWrappedFiltersWithParams() throws Exception {
    String str = "Foo Clara Bar David";
    TokenStream stream = whitespaceMockTokenizer(str);
    stream = tokenFilterFactory("ProtectedTerm", "ignoreCase", "true",
        "protected", "protected-1.txt", "wrappedFilters", "lowercase, truncate",
        "truncate.prefixLength", "2").create(stream);
    assertTokenStreamContents(stream, new String[]{"Foo", "cl", "Bar", "da"});
  }

  public void testMultipleSameNamedFiltersWithParams() throws Exception {
    String str = "Foo Clara Bar David";
    TokenStream stream = whitespaceMockTokenizer(str);
    stream = tokenFilterFactory("ProtectedTerm", "ignoreCase", "true",
        "protected", "protected-1.txt", "wrappedFilters", "truncate-A, reversestring, truncate-B",
        "truncate-A.prefixLength", "3", "truncate-B.prefixLength", "2").create(stream);
    assertTokenStreamContents(stream, new String[]{"Foo", "al", "Bar", "va"});

    // same-named wrapped filters, one with an ID and another without
    stream = whitespaceMockTokenizer(str);
    stream = tokenFilterFactory("ProtectedTerm", "ignoreCase", "true",
        "protected", "protected-1.txt", "wrappedFilters", "truncate, reversestring, truncate-A",
        "truncate.prefixLength", "3", "truncate-A.prefixLength", "2").create(stream);
    assertTokenStreamContents(stream, new String[]{"Foo", "al", "Bar", "va"});

    // Case-insensitive wrapped "filter-id"
    stream = whitespaceMockTokenizer(str);
    stream = tokenFilterFactory("ProtectedTerm", "ignoreCase", "true",
        "protected", "protected-1.txt", "wrappedFilters", "TRUNCATE-a, reversestring, truncate-b",
        "truncate-A.prefixLength", "3", "TRUNCATE-B.prefixLength", "2").create(stream);
    assertTokenStreamContents(stream, new String[]{"Foo", "al", "Bar", "va"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
      tokenFilterFactory("ProtectedTerm", "protected", "protected-1.txt", "bogusArg", "bogusValue"));
    assertTrue(exception.getMessage().contains("Unknown parameters"));

    // same-named wrapped filters
    exception = expectThrows(IllegalArgumentException.class, () ->
        tokenFilterFactory("ProtectedTerm",
            "protected", "protected-1.txt", "wrappedFilters", "truncate, truncate"));
    assertTrue(exception.getMessage().contains("wrappedFilters contains duplicate"));

    // case-insensitive same-named wrapped filters
    exception = expectThrows(IllegalArgumentException.class, () ->
        tokenFilterFactory("ProtectedTerm",
            "protected", "protected-1.txt", "wrappedFilters", "TRUNCATE, truncate"));
    assertTrue(exception.getMessage().contains("wrappedFilters contains duplicate"));

    // case-insensitive same-named wrapped filter IDs
    exception = expectThrows(IllegalArgumentException.class, () ->
        tokenFilterFactory("ProtectedTerm",
            "protected", "protected-1.txt", "wrappedFilters", "truncate-ABC, truncate-abc"));
    assertTrue(exception.getMessage().contains("wrappedFilters contains duplicate"));

    // mismatched wrapped filter and associated args
    exception = expectThrows(IllegalArgumentException.class, () ->
        tokenFilterFactory("ProtectedTerm",
            "protected", "protected-1.txt", "wrappedFilters", "truncate-A, reversestring, truncate-B",
            "truncate.prefixLength", "3", "truncate-A.prefixLength", "2"));
    assertTrue(exception.getMessage().contains("Unknown parameters: {truncate.prefixLength=3}"));

    // missing required arg(s) for wrapped filter
    String str = "Foo Clara Bar David";
    TokenStream stream = whitespaceMockTokenizer(str);
    exception = expectThrows(IllegalArgumentException.class, () ->
      tokenFilterFactory("ProtectedTerm",
            "protected", "protected-1.txt", "wrappedFilters", "length").create(stream));
    assertTrue(exception.getMessage().contains("Configuration Error: missing parameter"));
  }
}
