package org.apache.lucene.analysis.standard;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.analysis.*;

/** Normalizes tokens extracted with {@link StandardTokenizer}. */

public final class StandardFilter extends TokenFilter
  implements StandardTokenizerConstants  {


  /** Construct filtering <i>in</i>. */
  public StandardFilter(TokenStream in) {
    super(in);
  }

  private static final String APOSTROPHE_TYPE = tokenImage[APOSTROPHE];
  private static final String ACRONYM_TYPE = tokenImage[ACRONYM];
  
  /** Returns the next token in the stream, or null at EOS.
   * <p>Removes <tt>'s</tt> from the end of words.
   * <p>Removes dots from acronyms.
   */
  public final org.apache.lucene.analysis.Token next() throws java.io.IOException {
    org.apache.lucene.analysis.Token t = input.next();

    if (t == null)
      return null;

    String text = t.termText();
    String type = t.type();

    if (type == APOSTROPHE_TYPE &&		  // remove 's
	(text.endsWith("'s") || text.endsWith("'S"))) {
      return new org.apache.lucene.analysis.Token
	(text.substring(0,text.length()-2),
	 t.startOffset(), t.endOffset(), type);

    } else if (type == ACRONYM_TYPE) {		  // remove dots
      StringBuffer trimmed = new StringBuffer();
      for (int i = 0; i < text.length(); i++) {
	char c = text.charAt(i);
	if (c != '.')
	  trimmed.append(c);
      }
      return new org.apache.lucene.analysis.Token
	(trimmed.toString(), t.startOffset(), t.endOffset(), type);

    } else {
      return t;
    }
  }
}
