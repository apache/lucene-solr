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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.MappingCharFilter;
import org.apache.lucene.analysis.NormalizeCharMap;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class TestPatternTokenizerFactory extends BaseTokenTestCase 
{
	public void testSplitting() throws Exception 
  {
    String qpattern = "\\'([^\\']+)\\'"; // get stuff between "'"
    String[][] tests = {
      // group  pattern        input                    output
      { "-1",   "--",          "aaa--bbb--ccc",         "aaa bbb ccc" },
      { "-1",   ":",           "aaa:bbb:ccc",           "aaa bbb ccc" },
      { "-1",   "\\p{Space}",  "aaa   bbb \t\tccc  ",   "aaa bbb ccc" },
      { "-1",   ":",           "boo:and:foo",           "boo and foo" },
      { "-1",   "o",           "boo:and:foo",           "b :and:f" },
      { "0",    ":",           "boo:and:foo",           ": :" },
      { "0",    qpattern,      "aaa 'bbb' 'ccc'",       "'bbb' 'ccc'" },
      { "1",    qpattern,      "aaa 'bbb' 'ccc'",       "bbb ccc" }
    };
    
    
    Map<String,String> args = new HashMap<String, String>();
    for( String[] test : tests ) {
      args.put( PatternTokenizerFactory.GROUP, test[0] );
      args.put( PatternTokenizerFactory.PATTERN, test[1] );

      PatternTokenizerFactory tokenizer = new PatternTokenizerFactory();
      tokenizer.init( args );
      
      TokenStream stream = tokenizer.create( new StringReader( test[2] ) );
      String out = tsToString( stream );
      System.out.println( test[2] + " ==> " + out );
      
      assertEquals("pattern: "+test[1]+" with input: "+test[2], test[3], out );
      
      // Make sure it is the same as if we called 'split'
      // test disabled, as we remove empty tokens
      /*if( "-1".equals( test[0] ) ) {
        String[] split = test[2].split( test[1] );
        stream = tokenizer.create( new StringReader( test[2] ) );
        int i=0;
        for( Token t = stream.next(); null != t; t = stream.next() ) 
        {
          assertEquals( "split: "+test[1] + " "+i, split[i++], new String(t.termBuffer(), 0, t.termLength()) );
        }
      }*/
    } 
	}
	
  public void testOffsetCorrection() throws Exception {
    final String INPUT = "G&uuml;nther G&uuml;nther is here";

    // create MappingCharFilter
    MappingCharFilterFactory cfFactory = new MappingCharFilterFactory();
    List<String> mappingRules = new ArrayList<String>();
    mappingRules.add( "\"&uuml;\" => \"ü\"" );
    NormalizeCharMap normMap = new NormalizeCharMap();
    cfFactory.parseRules( mappingRules, normMap );
    CharStream charStream = new MappingCharFilter( normMap, CharReader.get( new StringReader( INPUT ) ) );

    // create PatternTokenizer
    Map<String,String> args = new HashMap<String, String>();
    args.put( PatternTokenizerFactory.PATTERN, "[,;/\\s]+" );
    PatternTokenizerFactory tokFactory = new PatternTokenizerFactory();
    tokFactory.init( args );
    TokenStream stream = tokFactory.create( charStream );
    assertTokenStreamContents(stream,
        new String[] { "Günther", "Günther", "is", "here" },
        new int[] { 0, 13, 26, 29 },
        new int[] { 12, 25, 28, 33 },
        new int[] { 1, 1, 1, 1 });
    
    charStream = new MappingCharFilter( normMap, CharReader.get( new StringReader( INPUT ) ) );
    args.put( PatternTokenizerFactory.PATTERN, "Günther" );
    args.put( PatternTokenizerFactory.GROUP, "0" );
    tokFactory = new PatternTokenizerFactory();
    tokFactory.init( args );
    stream = tokFactory.create( charStream );
    assertTokenStreamContents(stream,
        new String[] { "Günther", "Günther" },
        new int[] { 0, 13 },
        new int[] { 12, 25 },
        new int[] { 1, 1 });
  }
  
  /** 
   * TODO: rewrite tests not to use string comparison.
   * @deprecated only tests TermAttribute!
   */
  private static String tsToString(TokenStream in) throws IOException {
    StringBuilder out = new StringBuilder();
    TermAttribute termAtt = (TermAttribute) in.addAttribute(TermAttribute.class);
    // extra safety to enforce, that the state is not preserved and also
    // assign bogus values
    in.clearAttributes();
    termAtt.setTermBuffer("bogusTerm");
    while (in.incrementToken()) {
      if (out.length() > 0)
        out.append(' ');
      out.append(termAtt.term());
      in.clearAttributes();
      termAtt.setTermBuffer("bogusTerm");
    }

    in.close();
    return out.toString();
  }
}
