package org.apache.lucene.analysis.standard;

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

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;

/**
 * Normalizes tokens extracted with {@link StandardTokenizer}.
 */
public class StandardFilter extends TokenFilter {
  private final Version matchVersion;
  
  public StandardFilter(Version matchVersion, TokenStream in) {
    super(in);
    this.matchVersion = matchVersion;
  }
  
  private static final String APOSTROPHE_TYPE = ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.APOSTROPHE];
  private static final String ACRONYM_TYPE = ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.ACRONYM];

  // this filters uses attribute type
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  
  @Override
  public final boolean incrementToken() throws IOException {
    if (matchVersion.onOrAfter(Version.LUCENE_31))
      return input.incrementToken(); // TODO: add some niceties for the new grammar
    else
      return incrementTokenClassic();
  }
  
  public final boolean incrementTokenClassic() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }

    final char[] buffer = termAtt.buffer();
    final int bufferLength = termAtt.length();
    final String type = typeAtt.type();

    if (type == APOSTROPHE_TYPE &&      // remove 's
        bufferLength >= 2 &&
        buffer[bufferLength-2] == '\'' &&
        (buffer[bufferLength-1] == 's' || buffer[bufferLength-1] == 'S')) {
      // Strip last 2 characters off
      termAtt.setLength(bufferLength - 2);
    } else if (type == ACRONYM_TYPE) {      // remove dots
      int upto = 0;
      for(int i=0;i<bufferLength;i++) {
        char c = buffer[i];
        if (c != '.')
          buffer[upto++] = c;
      }
      termAtt.setLength(upto);
    }

    return true;
  }
}
