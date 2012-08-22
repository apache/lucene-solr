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

package org.apache.lucene.analysis.pattern;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.charfilter.BaseCharFilter;

/**
 * CharFilter that uses a regular expression for the target of replace string.
 * The pattern match will be done in each "block" in char stream.
 * 
 * <p>
 * ex1) source="aa&nbsp;&nbsp;bb&nbsp;aa&nbsp;bb", pattern="(aa)\\s+(bb)" replacement="$1#$2"<br/>
 * output="aa#bb&nbsp;aa#bb"
 * </p>
 * 
 * NOTE: If you produce a phrase that has different length to source string
 * and the field is used for highlighting for a term of the phrase, you will
 * face a trouble.
 * 
 * <p>
 * ex2) source="aa123bb", pattern="(aa)\\d+(bb)" replacement="$1&nbsp;$2"<br/>
 * output="aa&nbsp;bb"<br/>
 * and you want to search bb and highlight it, you will get<br/>
 * highlight snippet="aa1&lt;em&gt;23bb&lt;/em&gt;"
 * </p>
 * 
 * @since Solr 1.5
 */
public class PatternReplaceCharFilter extends BaseCharFilter {
  @Deprecated
  public static final int DEFAULT_MAX_BLOCK_CHARS = 10000;

  private final Pattern pattern;
  private final String replacement;
  private Reader transformedInput;

  public PatternReplaceCharFilter(Pattern pattern, String replacement, Reader in) {
    super(in);
    this.pattern = pattern;
    this.replacement = replacement;
  }

  @Deprecated
  public PatternReplaceCharFilter(Pattern pattern, String replacement, 
      int maxBlockChars, String blockDelimiter, Reader in) {
    this(pattern, replacement, in);
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    // Buffer all input on the first call.
    if (transformedInput == null) {
      fill();
    }

    return transformedInput.read(cbuf, off, len);
  }
  
  private void fill() throws IOException {
    StringBuilder buffered = new StringBuilder();
    char [] temp = new char [1024];
    for (int cnt = input.read(temp); cnt > 0; cnt = input.read(temp)) {
      buffered.append(temp, 0, cnt);
    }
    transformedInput = new StringReader(processPattern(buffered).toString());
  }

  @Override
  public int read() throws IOException {
    if (transformedInput == null) {
      fill();
    }
    
    return transformedInput.read();
  }

  @Override
  protected int correct(int currentOff) {
    return Math.max(0,  super.correct(currentOff));
  }

  /**
   * Replace pattern in input and mark correction offsets. 
   */
  CharSequence processPattern(CharSequence input) {
    final Matcher m = pattern.matcher(input);

    final StringBuffer cumulativeOutput = new StringBuffer();
    int cumulative = 0;
    int lastMatchEnd = 0;
    while (m.find()) {
      final int groupSize = m.end() - m.start();
      final int skippedSize = m.start() - lastMatchEnd;
      lastMatchEnd = m.end();

      final int lengthBeforeReplacement = cumulativeOutput.length() + skippedSize;
      m.appendReplacement(cumulativeOutput, replacement);
      // Matcher doesn't tell us how many characters have been appended before the replacement.
      // So we need to calculate it. Skipped characters have been added as part of appendReplacement.
      final int replacementSize = cumulativeOutput.length() - lengthBeforeReplacement;

      if (groupSize != replacementSize) {
        if (replacementSize < groupSize) {
          // The replacement is smaller. 
          // Add the 'backskip' to the next index after the replacement (this is possibly 
          // after the end of string, but it's fine -- it just means the last character 
          // of the replaced block doesn't reach the end of the original string.
          cumulative += groupSize - replacementSize;
          int atIndex = lengthBeforeReplacement + replacementSize;
          // System.err.println(atIndex + "!" + cumulative);
          addOffCorrectMap(atIndex, cumulative);
        } else {
          // The replacement is larger. Every new index needs to point to the last
          // element of the original group (if any).
          for (int i = groupSize; i < replacementSize; i++) {
            addOffCorrectMap(lengthBeforeReplacement + i, --cumulative);
            // System.err.println((lengthBeforeReplacement + i) + " " + cumulative);
          }
        }
      }
    }

    // Append the remaining output, no further changes to indices.
    m.appendTail(cumulativeOutput);
    return cumulativeOutput;    
  }
}
