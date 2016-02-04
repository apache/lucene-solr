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
package org.apache.lucene.sandbox.queries.regex;

import org.apache.lucene.util.BytesRef;

/**
 * Defines basic operations needed by {@link RegexQuery} for a regular
 * expression implementation.
 * @deprecated Use core's regex query.
 */
@Deprecated
public interface RegexCapabilities {
  /**
   * Called by the constructor of {@link RegexTermsEnum} allowing
   * implementations to cache a compiled version of the regular
   * expression pattern.
   *
   * @param pattern regular expression pattern
   */
  public RegexMatcher compile(String pattern);

  /**
   * Interface for basic regex matching.
   * <p>
   * Implementations return true for {@link #match} if the term 
   * matches the regex.
   * <p>
   * Implementing {@link #prefix()} can restrict the TermsEnum to only
   * a subset of terms when the regular expression matches a constant
   * prefix.
   * <p>
   * NOTE: implementations cannot seek.
   * @deprecated Use core's regex query.
   */
  @Deprecated
  public interface RegexMatcher {
    /**
     *
     * @param term The term in bytes.
     * @return true if string matches the pattern last passed to {@link #compile}.
     */
    public boolean match(BytesRef term);

    /**
     * A wise prefix implementation can reduce the term enumeration (and thus increase performance)
     * of RegexQuery dramatically!
     *
     * @return static non-regex prefix of the pattern last passed to {@link #compile}.  May return null.
     */
    public String prefix();
  }
}
