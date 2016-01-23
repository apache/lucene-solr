package org.apache.lucene.search.regex;

/**
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

import java.util.regex.Pattern;

/**
 * An implementation tying Java's built-in java.util.regex to RegexQuery.
 *
 * Note that because this implementation currently only returns null from
 * {@link #prefix} that queries using this implementation will enumerate and
 * attempt to {@link #match} each term for the specified field in the index.
 */
public class JavaUtilRegexCapabilities implements RegexCapabilities {
  private Pattern pattern;
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
  
  public void compile(String pattern) {
    this.pattern = Pattern.compile(pattern, this.flags);
  }

  public boolean match(String string) {
    return pattern.matcher(string).matches();
  }

  public String prefix() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final JavaUtilRegexCapabilities that = (JavaUtilRegexCapabilities) o;

    if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (pattern != null ? pattern.hashCode() : 0);
  }
}
