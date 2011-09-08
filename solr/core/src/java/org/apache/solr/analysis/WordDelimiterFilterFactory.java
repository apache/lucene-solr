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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterIterator;
import org.apache.lucene.analysis.util.CharArraySet;

import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.util.StrUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;

import static org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter.*;


/**
 * Factory for {@link WordDelimiterFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_wd" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.WordDelimiterFilterFactory" protected="protectedword.txt"
 *             preserveOriginal="0" splitOnNumerics="1" splitOnCaseChange="1"
 *             catenateWords="0" catenateNumbers="0" catenateAll="0"
 *             generateWordParts="1" generateNumberParts="1" stemEnglishPossessive="1"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre> 
 *
 */
public class WordDelimiterFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  public static final String PROTECTED_TOKENS = "protected";
  public static final String TYPES = "types";
  
  public void inform(ResourceLoader loader) {
    String wordFiles = args.get(PROTECTED_TOKENS);
    if (wordFiles != null) {  
      try {
        protectedWords = getWordSet(loader, wordFiles, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    String types = args.get(TYPES);
    if (types != null) {
      try {
        List<String> files = StrUtils.splitFileNames( types );
        List<String> wlist = new ArrayList<String>();
        for( String file : files ){
          List<String> lines = loader.getLines( file.trim() );
          wlist.addAll( lines );
        }
      typeTable = parseTypes(wlist);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private CharArraySet protectedWords = null;
  private int flags;
  byte[] typeTable = null;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    if (getInt("generateWordParts", 1) != 0) {
      flags |= GENERATE_WORD_PARTS;
    }
    if (getInt("generateNumberParts", 1) != 0) {
      flags |= GENERATE_NUMBER_PARTS;
    }
    if (getInt("catenateWords", 0) != 0) {
      flags |= CATENATE_WORDS;
    }
    if (getInt("catenateNumbers", 0) != 0) {
      flags |= CATENATE_NUMBERS;
    }
    if (getInt("catenateAll", 0) != 0) {
      flags |= CATENATE_ALL;
    }
    if (getInt("splitOnCaseChange", 1) != 0) {
      flags |= SPLIT_ON_CASE_CHANGE;
    }
    if (getInt("splitOnNumerics", 1) != 0) {
      flags |= SPLIT_ON_NUMERICS;
    }
    if (getInt("preserveOriginal", 0) != 0) {
      flags |= PRESERVE_ORIGINAL;
    }
    if (getInt("stemEnglishPossessive", 1) != 0) {
      flags |= STEM_ENGLISH_POSSESSIVE;
    }
  }

  public WordDelimiterFilter create(TokenStream input) {
    return new WordDelimiterFilter(input, typeTable == null ? WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE : typeTable,
                                   flags, protectedWords);
  }
  
  // source => type
  private static Pattern typePattern = Pattern.compile( "(.*)\\s*=>\\s*(.*)\\s*$" );
  
  /** parses a list of MappingCharFilter style rules into a custom byte[] type table */
  private byte[] parseTypes(List<String> rules) {
    SortedMap<Character,Byte> typeMap = new TreeMap<Character,Byte>();
    for( String rule : rules ){
      Matcher m = typePattern.matcher(rule);
      if( !m.find() )
        throw new RuntimeException("Invalid Mapping Rule : [" + rule + "]");
      String lhs = parseString(m.group(1).trim());
      Byte rhs = parseType(m.group(2).trim());
      if (lhs.length() != 1)
        throw new RuntimeException("Invalid Mapping Rule : [" + rule + "]. Only a single character is allowed.");
      if (rhs == null)
        throw new RuntimeException("Invalid Mapping Rule : [" + rule + "]. Illegal type.");
      typeMap.put(lhs.charAt(0), rhs);
    }
    
    // ensure the table is always at least as big as DEFAULT_WORD_DELIM_TABLE for performance
    byte types[] = new byte[Math.max(typeMap.lastKey()+1, WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE.length)];
    for (int i = 0; i < types.length; i++)
      types[i] = WordDelimiterIterator.getType(i);
    for (Map.Entry<Character,Byte> mapping : typeMap.entrySet())
      types[mapping.getKey()] = mapping.getValue();
    return types;
  }
  
  private Byte parseType(String s) {
    if (s.equals("LOWER"))
      return LOWER;
    else if (s.equals("UPPER"))
      return UPPER;
    else if (s.equals("ALPHA"))
      return ALPHA;
    else if (s.equals("DIGIT"))
      return DIGIT;
    else if (s.equals("ALPHANUM"))
      return ALPHANUM;
    else if (s.equals("SUBWORD_DELIM"))
      return SUBWORD_DELIM;
    else
      return null;
  }
  
  char[] out = new char[256];
  
  private String parseString(String s){
    int readPos = 0;
    int len = s.length();
    int writePos = 0;
    while( readPos < len ){
      char c = s.charAt( readPos++ );
      if( c == '\\' ){
        if( readPos >= len )
          throw new RuntimeException( "Invalid escaped char in [" + s + "]" );
        c = s.charAt( readPos++ );
        switch( c ) {
          case '\\' : c = '\\'; break;
          case 'n' : c = '\n'; break;
          case 't' : c = '\t'; break;
          case 'r' : c = '\r'; break;
          case 'b' : c = '\b'; break;
          case 'f' : c = '\f'; break;
          case 'u' :
            if( readPos + 3 >= len )
              throw new RuntimeException( "Invalid escaped char in [" + s + "]" );
            c = (char)Integer.parseInt( s.substring( readPos, readPos + 4 ), 16 );
            readPos += 4;
            break;
        }
      }
      out[writePos++] = c;
    }
    return new String( out, 0, writePos );
  }
}
