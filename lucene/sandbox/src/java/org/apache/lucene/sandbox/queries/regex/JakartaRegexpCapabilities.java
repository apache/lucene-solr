package org.apache.lucene.sandbox.queries.regex;

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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.regexp.CharacterIterator;
import org.apache.regexp.RE;
import org.apache.regexp.REProgram;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Implementation tying <a href="http://jakarta.apache.org/regexp">Jakarta
 * Regexp</a> to RegexQuery. Jakarta Regexp internally supports a
 * {@link RegexCapabilities.RegexMatcher#prefix()} implementation which can offer 
 * performance gains under certain circumstances. Yet, the implementation appears 
 * to be rather shaky as it doesn't always provide a prefix even if one would exist.
 */
public class JakartaRegexpCapabilities implements RegexCapabilities {
  private static Field prefixField;
  private static Method getPrefixMethod;

  static {
    try {
      getPrefixMethod = REProgram.class.getMethod("getPrefix");
    } catch (Exception e) {
      getPrefixMethod = null;
    }
    try {
      prefixField = REProgram.class.getDeclaredField("prefix");
      prefixField.setAccessible(true);
    } catch (Exception e) {
      prefixField = null;
    }
  }
  
  // Define the flags that are possible. Redefine them here
  // to avoid exposing the RE class to the caller.
  
  private int flags = RE.MATCH_NORMAL;

  /**
   * Flag to specify normal, case-sensitive matching behaviour. This is the default.
   */
  public static final int FLAG_MATCH_NORMAL = RE.MATCH_NORMAL;
  
  /**
   * Flag to specify that matching should be case-independent (folded)
   */
  public static final int FLAG_MATCH_CASEINDEPENDENT = RE.MATCH_CASEINDEPENDENT;
 
  /**
   * Constructs a RegexCapabilities with the default MATCH_NORMAL match style.
   */
  public JakartaRegexpCapabilities() {}
  
  /**
   * Constructs a RegexCapabilities with the provided match flags.
   * Multiple flags should be ORed together.
   * 
   * @param flags The matching style
   */
  public JakartaRegexpCapabilities(int flags) {
    this.flags = flags;
  }
  
  public RegexCapabilities.RegexMatcher compile(String regex) {
    return new JakartaRegexMatcher(regex, flags);
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
    
    JakartaRegexpCapabilities other = (JakartaRegexpCapabilities) obj;
    return flags == other.flags;
  }

  class JakartaRegexMatcher implements RegexCapabilities.RegexMatcher {
    
    private RE regexp;
    private final CharsRef utf16 = new CharsRef(10);
    private final CharacterIterator utf16wrapper = new CharacterIterator() {

      public char charAt(int pos) {
        return utf16.chars[pos];
      }

      public boolean isEnd(int pos) {
        return pos >= utf16.length;
      }

      public String substring(int beginIndex) {
        return substring(beginIndex, utf16.length);
      }

      public String substring(int beginIndex, int endIndex) {
        return new String(utf16.chars, beginIndex, endIndex - beginIndex);
      }
      
    };
    
    public JakartaRegexMatcher(String regex, int flags) {
      regexp = new RE(regex, flags);
    }
    
    public boolean match(BytesRef term) {
      UnicodeUtil.UTF8toUTF16(term.bytes, term.offset, term.length, utf16);
      return regexp.match(utf16wrapper, 0);
    }

    public String prefix() {
      try {
        final char[] prefix;
        if (getPrefixMethod != null) {
          prefix = (char[]) getPrefixMethod.invoke(regexp.getProgram());
        } else if (prefixField != null) {
          prefix = (char[]) prefixField.get(regexp.getProgram());
        } else {
          return null;
        }
        return prefix == null ? null : new String(prefix);
      } catch (Exception e) {
        // if we cannot get the prefix, return none
        return null;
      }
    }
  }
}
