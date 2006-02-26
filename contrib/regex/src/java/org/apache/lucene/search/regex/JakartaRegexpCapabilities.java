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

import org.apache.regexp.RE;
import org.apache.regexp.RegexpTunnel;

/**
 * Implementation tying <a href="http://jakarta.apache.org/regexp">Jakarta Regexp</a>
 * to RegexQuery.  Thanks to some internals of Jakarta Regexp, this
 * has a solid {@link #prefix} implementation.
 */
public class JakartaRegexpCapabilities implements RegexCapabilities {
  private RE regexp;

  public void compile(String pattern) {
    regexp = new RE(pattern);
  }

  public boolean match(String string) {
    return regexp.match(string);
  }

  public String prefix() {
    char[] prefix = RegexpTunnel.getPrefix(regexp);
    return prefix == null ? null : new String(prefix);
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final JakartaRegexpCapabilities that = (JakartaRegexpCapabilities) o;

    if (regexp != null ? !regexp.equals(that.regexp) : that.regexp != null) return false;

    return true;
  }

  public int hashCode() {
    return (regexp != null ? regexp.hashCode() : 0);
  }
}
