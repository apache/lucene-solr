package org.apache.lucene.search.highlight;

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

/**
 * Simple {@link Formatter} implementation to highlight terms with a pre and
 * post tag.
 */
public class SimpleHTMLFormatter implements Formatter {
  
  private static final String DEFAULT_PRE_TAG = "<B>";
  private static final String DEFAULT_POST_TAG = "</B>";
  
	private String preTag;
	private String postTag;
	
	public SimpleHTMLFormatter(String preTag, String postTag) {
		this.preTag = preTag;
		this.postTag = postTag;
	}

	/** Default constructor uses HTML: &lt;B&gt; tags to markup terms. */
	public SimpleHTMLFormatter() {
	  this(DEFAULT_PRE_TAG, DEFAULT_POST_TAG);
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.search.highlight.Formatter#highlightTerm(java.lang.String, org.apache.lucene.search.highlight.TokenGroup)
	 */
	public String highlightTerm(String originalText, TokenGroup tokenGroup) {
	  if (tokenGroup.getTotalScore() <= 0) {
	    return originalText;
	  }
	  
	  // Allocate StringBuilder with the right number of characters from the
    // beginning, to avoid char[] allocations in the middle of appends.
	  StringBuilder returnBuffer = new StringBuilder(preTag.length() + originalText.length() + postTag.length());
	  returnBuffer.append(preTag);
	  returnBuffer.append(originalText);
	  returnBuffer.append(postTag);
	  return returnBuffer.toString();
	}
	
}
