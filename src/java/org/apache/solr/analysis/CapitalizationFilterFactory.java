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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.analysis.BaseTokenFilterFactory;

/**
 * A filter to apply normal capitalization rules to Tokens.  It will make the first letter
 * capital and the rest lower case.  
 * 
 * This filter is particularly useful to build nice looking facet parameters.  This filter
 * is not appropriate if you intend to use a prefix query.
 * 
 * The factory takes parameters:
 * "onlyFirstWord" - should each word be capitalized or all of the words?
 * "keep" - a keep word list.  Each word that should be kept separated by whitespace.
 * "okPrefix" - do not change word capitalization if a word begins with something in this list.
 *   for example if "McK" is on the okPrefix list, the word "McKinley" should not be changed to
 *   "Mckinley"
 * "minWordLength" - how long the word needs to be to get capitalization applied.  If the 
 *   minWordLength is 3, "and" > "And" but "or" stays "or"
 * "maxWordCount" - if the token contains more then maxWordCount words, the capitalization is
 *   assumed to be correct.
 * 
 * @since solr 1.3
 * @version $Id$
 */
public class CapitalizationFilterFactory extends BaseTokenFilterFactory 
{
  public static final String KEEP = "keep";
  public static final String OK_PREFIX = "okPrefix";
  public static final String MIN_WORD_LENGTH = "minWordLength";
  public static final String MAX_WORD_COUNT = "maxWordCount";
  public static final String MAX_TOKEN_LENGTH = "maxTokenLength";
  public static final String ONLY_FIRST_WORD = "onlyFirstWord";
  public static final String FORCE_FIRST_LETTER = "forceFirstLetter";
  
  Map<String,String> keep = new HashMap<String, String>(); // not synchronized because it is only initialized once
  
  Collection<String> okPrefix = new ArrayList<String>(); // for Example: McK
  
  int minWordLength = 0;  // don't modify capitalization for words shorter then this
  int maxWordCount  = Integer.MAX_VALUE;
  int maxTokenLength = Integer.MAX_VALUE;
  boolean onlyFirstWord = true;
  boolean forceFirstLetter = true; // make sure the first letter is capitol even if it is in the keep list
  
  @Override
  public void init(Map<String,String> args) {
    super.init( args );
    
    String k = args.get( KEEP );
    if( k != null ) {
      StringTokenizer st = new StringTokenizer( k );
      while( st.hasMoreTokens() ) {
        k = st.nextToken().trim();
        keep.put( k.toUpperCase(), k );
      }
    }
    
    k = args.get( OK_PREFIX );
    if( k != null ) {
      StringTokenizer st = new StringTokenizer( k );
      while( st.hasMoreTokens() ) {
        okPrefix.add( st.nextToken().trim() );
      }
    }
    
    k = args.get( MIN_WORD_LENGTH );
    if( k != null ) {
      minWordLength = Integer.valueOf( k );
    }

    k = args.get( MAX_WORD_COUNT );
    if( k != null ) {
      maxWordCount = Integer.valueOf( k );
    }

    k = args.get( MAX_TOKEN_LENGTH );
    if( k != null ) {
      maxTokenLength = Integer.valueOf( k );
    }

    k = args.get( ONLY_FIRST_WORD );
    if( k != null ) {
      onlyFirstWord = Boolean.valueOf( k );
    }

    k = args.get( FORCE_FIRST_LETTER );
    if( k != null ) {
      forceFirstLetter = Boolean.valueOf( k );
    }
  }
  
  public String processWord( String w, int wordCount )
  {
    if( w.length() < 1 ) {
      return w;
    }
    if( onlyFirstWord && wordCount > 0 ) {
      return w.toLowerCase();
    }
    
    String k = keep.get( w.toUpperCase() );
    if( k != null ) {
      if( wordCount == 0 && forceFirstLetter && Character.isLowerCase( k.charAt(0) ) ) {
        return Character.toUpperCase( k.charAt(0) ) + k.substring( 1 );
      }
      return k;
    }
    if( w.length() < minWordLength ) {
      return w;
    }
    for( String prefix : okPrefix ) {
      if( w.startsWith( prefix ) ) {
        return w;
      }
    }
    
    // We know it has at least one character
    char[] chars = w.toCharArray();
    StringBuilder word = new StringBuilder( w.length() );
    word.append( Character.toUpperCase( chars[0] ) );
    for( int i=1; i<chars.length; i++ ) {
      word.append( Character.toLowerCase( chars[i] ) );
    }
    return word.toString();
  }
  
  public CapitalizationFilter create(TokenStream input) {
    return new CapitalizationFilter(input,this);
  }
}



/**
 * This relies on the Factory so that the difficult stuff does not need to be
 * re-initialized each time the filter runs.
 * 
 * This is package protected since it is not useful without the Factory
 */
class CapitalizationFilter extends TokenFilter 
{
  protected final CapitalizationFilterFactory factory;
  
  public CapitalizationFilter(TokenStream in, final CapitalizationFilterFactory factory ) {
    super(in);
    this.factory = factory;
  }
  
  @Override
  public final Token next() throws IOException {
    
    Token t = input.next();
    if( t != null ) {
      String s = t.termText();
      if( s.length() < factory.maxTokenLength ) {
        int wordCount = 0;

        StringBuilder word = new StringBuilder( s.length() );
        StringBuilder text = new StringBuilder( s.length() );
        for( char c : s.toCharArray() ) {
          if( c <= ' ' || c == '.' ) { 
            if( word.length() > 0 ) {
              text.append( factory.processWord( word.toString(), wordCount++ ) );
              word.setLength( 0 );
            }
            text.append( c );
          }
          else { 
            word.append( c );
          }
        }
        
        // Add the last word
        if( word.length() > 0 ) {
          text.append( factory.processWord( word.toString(), wordCount++ ) );
        }
        
        if( wordCount <= factory.maxWordCount ) {
          t.setTermText( text.toString() );
        }
      }
    }
    return t;
  }
}

