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

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrConfig;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
 * be equivalent to the output from:
 *
 * http://java.sun.com/j2se/1.4.2/docs/api/java/lang/String.html#split(java.lang.String)
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
 *
 * @since solr1.2
 * @version $Id:$
 */
public class PatternTokenizerFactory extends BaseTokenizerFactory 
{
  public static final String PATTERN = "pattern";
  public static final String GROUP = "group";
 
  protected Map<String,String> args;
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
  public TokenStream create(Reader input) {
    try {
      // Read the input into a single string
      String str = IOUtils.toString( input );
      
      Matcher matcher = pattern.matcher( str );
      List<Token> tokens = (group < 0 ) 
        ? split( matcher, str )
        : group( matcher, str, group );
        
      final Iterator<Token> iter = tokens.iterator();
      return new TokenStream() {
        @Override
        public Token next() throws IOException {
          if( iter.hasNext() ) {
            return iter.next();
          }
          return null;
        }
      };
    }
    catch( IOException ex ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, ex );
    }
  }
  
  /**
   * This behaves just like String.split( ), but returns a list of Tokens
   * rather then an array of strings
   */
  public static List<Token> split( Matcher matcher, String input )
  {
    int index = 0;
    int lastNonEmptySize = Integer.MAX_VALUE;
    ArrayList<Token> matchList = new ArrayList<Token>();

    // Add segments before each match found
    while(matcher.find()) {
      String match = input.subSequence(index, matcher.start()).toString();
      matchList.add( new Token( match, index, matcher.start()) );
      index = matcher.end();
      if( match.length() > 0 ) {
        lastNonEmptySize = matchList.size();
      }
    }

    // If no match is found, return the full string
    if (index == 0) {
      matchList.add( new Token( input, 0, input.length()) );
    }
    else { 
      String match = input.subSequence(index, input.length()).toString();
      matchList.add( new Token( match, index, input.length()) );
      if( match.length() > 0 ) {
        lastNonEmptySize = matchList.size();
      }
    }
    
    // Don't use trailing empty strings.  This behavior matches String.split();
    if( lastNonEmptySize < matchList.size() ) {
      return matchList.subList( 0, lastNonEmptySize );
    }
    return matchList;
  }
  

  /**
   * Create tokens from the matches in a matcher 
   */
  public static List<Token> group( Matcher matcher, String input, int group )
  {
    ArrayList<Token> matchList = new ArrayList<Token>();
    while(matcher.find()) {
      Token t = new Token( 
        matcher.group(group), 
        matcher.start(group), 
        matcher.end(group) );
      matchList.add( t );
    }
    return matchList;
  }
}
