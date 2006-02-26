package org.apache.lucene.search.regex;

/**
 * Copyright 2006 The Apache Software Foundation
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

  public void compile(String pattern) {
    this.pattern = Pattern.compile(pattern);
  }

  public boolean match(String string) {
    return pattern.matcher(string).lookingAt();
  }

  public String prefix() {
    return null;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final JavaUtilRegexCapabilities that = (JavaUtilRegexCapabilities) o;

    if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) return false;

    return true;
  }

  public int hashCode() {
    return (pattern != null ? pattern.hashCode() : 0);
  }
}
