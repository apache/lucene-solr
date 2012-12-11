package org.apache.lucene.sandbox.queries.regex;

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;

/**
 * An implementation tying Java's built-in java.util.regex to RegexQuery.
 *
 * Note that because this implementation currently only returns null from
 * {@link RegexCapabilities.RegexMatcher#prefix()} that queries using this implementation 
 * will enumerate and attempt to {@link RegexCapabilities.RegexMatcher#match(BytesRef)} each 
 * term for the specified field in the index.
 */
public class JavaUtilRegexCapabilities implements RegexCapabilities {

  private int flags = 0;

  // Define the optional flags from Pattern that can be used.
  // Do this here to keep Pattern contained within this class.
  
  public static final int FLAG_CANON_EQ = Pattern.CANON_EQ;
  public static final int FLAG_CASE_INSENSITIVE = Pattern.CASE_INSENSITIVE;
  public static final int FLAG_COMMENTS = Pattern.COMMENTS;
  public static final int FLAG_DOTALL = Pattern.DOTALL;
  public static final int FLAG_LITERAL = Pattern.LITERAL;
  public static final int FLAG_MULTILINE = Pattern.MULTILINE;
  public static final int FLAG_UNICODE_CASE = Pattern.UNICODE_CASE;
  public static final int FLAG_UNIX_LINES = Pattern.UNIX_LINES;
  
  /**
   * Default constructor that uses java.util.regex.Pattern 
   * with its default flags.
   */
  public JavaUtilRegexCapabilities()  {
    this.flags = 0;
  }
  
  /**
   * Constructor that allows for the modification of the flags that
   * the java.util.regex.Pattern will use to compile the regular expression.
   * This gives the user the ability to fine-tune how the regular expression 
   * to match the functionality that they need. 
   * The {@link java.util.regex.Pattern Pattern} class supports specifying 
   * these fields via the regular expression text itself, but this gives the caller
   * another option to modify the behavior. Useful in cases where the regular expression text
   * cannot be modified, or if doing so is undesired.
   * 
   * @param flags The flags that are ORed together.
   */
  public JavaUtilRegexCapabilities(int flags) {
    this.flags = flags;
  }
  
  @Override
  public RegexCapabilities.RegexMatcher compile(String regex) {
    return new JavaUtilRegexMatcher(regex, flags);
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + flags;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    JavaUtilRegexCapabilities other = (JavaUtilRegexCapabilities) obj;
    return flags == other.flags;
  }

  class JavaUtilRegexMatcher implements RegexCapabilities.RegexMatcher {
    private final Pattern pattern;
    private final Matcher matcher;
    private final CharsRef utf16 = new CharsRef(10);
    
    public JavaUtilRegexMatcher(String regex, int flags) {
      this.pattern = Pattern.compile(regex, flags);
      this.matcher = this.pattern.matcher(utf16);
    }
    
    @Override
    public boolean match(BytesRef term) {
      UnicodeUtil.UTF8toUTF16(term.bytes, term.offset, term.length, utf16);
      return matcher.reset().matches();
    }

    @Override
    public String prefix() {
      return null;
    }
  }
}
