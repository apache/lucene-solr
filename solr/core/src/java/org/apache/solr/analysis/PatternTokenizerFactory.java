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
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.solr.common.SolrException;


/**
 * Factory for {@link PatternTokenizer}.
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
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_ptn" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.PatternTokenizerFactory" pattern="\'([^\']+)\'" group="1"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre> 
 * 
 * @see PatternTokenizer
 * @since solr1.2
 *
 */
public class PatternTokenizerFactory extends BaseTokenizerFactory 
{
  public static final String PATTERN = "pattern";
  public static final String GROUP = "group";
 
  protected Pattern pattern;
  protected int group;
  
  /**
   * Require a configured pattern
   */
  @Override
  public void init(Map<String,String> args) 
  {
    this.args = args;
    String regex = args.get( PATTERN );
    if( regex == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "missing required argument: "+PATTERN );
    }
    int flags = 0; // TODO? -- read flags from config CASE_INSENSITIVE, etc
    pattern = Pattern.compile( regex, flags );
    
    group = -1;  // use 'split'
    String g = args.get( GROUP );
    if( g != null ) {
      try {
        group = Integer.parseInt( g );
      }
      catch( Exception ex ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "invalid group argument: "+g );
      }
    }
  }
  
  /**
   * Split the input using configured pattern
   */
  public Tokenizer create(final Reader in) {
    try {
      return new PatternTokenizer(in, pattern, group);
    } catch( IOException ex ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, ex );
    }
  }
}
