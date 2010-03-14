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

package org.apache.solr.analysis;

import java.io.IOException;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.commons.io.IOUtils;

/**
 * This tokenizer uses regex pattern matching to construct distinct tokens
 * for the input stream.  It takes two arguments:  "pattern" and "group".
 * <p/>
 * <ul>
 * <li>"pattern" is the regular expression.</li>
 * <li>"group" says which group to extract into tokens.</li>
 *  </ul>
 * <p>
 * group=-1 (the default) is equivalent to "split".  In this case, the tokens will
 * be equivalent to the output from (without empty tokens):
 * {@link String#split(java.lang.String)}
 * </p>
 * <p>
 * Using group >= 0 selects the matching group as the token.  For example, if you have:<br/>
 * <pre>
 *  pattern = \'([^\']+)\'
 *  group = 0
 *  input = aaa 'bbb' 'ccc'
 *</pre>
 * the output will be two tokens: 'bbb' and 'ccc' (including the ' marks).  With the same input
 * but using group=1, the output would be: bbb and ccc (no ' marks)
 * </p>
 * <p>NOTE: This Tokenizer does not output tokens that are of zero length.</p>
 *
 * @version $Id$
 * @see Pattern
 */
public final class PatternTokenizer extends Tokenizer {

  private final TermAttribute termAtt = (TermAttribute) addAttribute(TermAttribute.class);
  private final OffsetAttribute offsetAtt = (OffsetAttribute) addAttribute(OffsetAttribute.class);

  private String str;
  private int index;
  
  private final Pattern pattern;
  private final int group;
  private final Matcher matcher;

  /** creates a new PatternTokenizer returning tokens from group (-1 for split functionality) */
  public PatternTokenizer(Reader input, Pattern pattern, int group) throws IOException {
    super(input);
    this.pattern = pattern;
    this.group = group;
    str = IOUtils.toString(input);
    matcher = pattern.matcher(str);
    index = 0;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (index >= str.length()) return false;
    clearAttributes();
    if (group >= 0) {
    
      // match a specific group
      while (matcher.find()) {
        final String match = matcher.group(group);
        if (match.length() == 0) continue;
        termAtt.setTermBuffer(match);
        index = matcher.start(group);
        offsetAtt.setOffset(correctOffset(index), correctOffset(matcher.end(group)));
        return true;
      }
      
      index = Integer.MAX_VALUE; // mark exhausted
      return false;
      
    } else {
    
      // String.split() functionality
      while (matcher.find()) {
        if (matcher.start() - index > 0) {
          // found a non-zero-length token
          termAtt.setTermBuffer(str, index, matcher.start() - index);
          offsetAtt.setOffset(correctOffset(index), correctOffset(matcher.start()));
          index = matcher.end();
          return true;
        }
        
        index = matcher.end();
      }
      
      if (str.length() - index == 0) {
        index = Integer.MAX_VALUE; // mark exhausted
        return false;
      }
      
      termAtt.setTermBuffer(str, index, str.length() - index);
      offsetAtt.setOffset(correctOffset(index), correctOffset(str.length()));
      index = Integer.MAX_VALUE; // mark exhausted
      return true;
    }
  }

  @Override
  public void end() throws IOException {
    final int ofs = correctOffset(str.length());
    offsetAtt.setOffset(ofs, ofs);
  }

  @Override
  public void reset(Reader input) throws IOException {
    super.reset(input);
    str = IOUtils.toString(input);
    matcher.reset(str);
    index = 0;
  }

}
