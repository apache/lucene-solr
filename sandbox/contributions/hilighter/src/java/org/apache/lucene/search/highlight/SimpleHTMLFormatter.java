package org.apache.lucene.search.highlight;
/**
 * Copyright 2002-2004 The Apache Software Foundation
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
 * Simple {@link Formatter} implementation to highlight terms with a pre and post tag
 * @author MAHarwood
 *
 */
public class SimpleHTMLFormatter implements Formatter
{
	String preTag;
	String postTag;

	public SimpleHTMLFormatter(String preTag, String postTag)
	{
		this.preTag = preTag;
		this.postTag = postTag;
	}

	/**
	 * Default constructor uses HTML: &lt;B&gt; tags to markup terms
	 * 
	 **/
	public SimpleHTMLFormatter()
	{
		this.preTag = "<B>";
		this.postTag = "</B>";
	}

	public String highlightTerm(String originalText, String term, float score, int startOffset)
	{
		if(score<=0)
		{
			return originalText;
		}
		StringBuffer sb = new StringBuffer();
		sb.append(preTag);
		sb.append(originalText);
		sb.append(postTag);
		return sb.toString();
	}

}
