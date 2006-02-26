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

/**
 * Defines basic operations needed by {@link RegexQuery} for a regular
 * expression implementation.
 */
interface RegexCapabilities {
  /**
   * Called by the constructor of {@link RegexTermEnum} allowing
   * implementations to cache a compiled version of the regular
   * expression pattern.
   *
   * @param pattern regular expression pattern
   */
  void compile(String pattern);

  /**
   *
   * @param string
   * @return true if string matches the pattern last passed to {@link #compile}.
   */
  boolean match(String string);

  /**
   * A wise prefix implementation can reduce the term enumeration (and thus performance)
   * of RegexQuery dramatically!
   *
   * @return static non-regex prefix of the pattern last passed to {@link #compile}.  May return null.
   */
  String prefix();
}
