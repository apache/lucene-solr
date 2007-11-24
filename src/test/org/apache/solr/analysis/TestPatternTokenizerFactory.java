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

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

public class TestPatternTokenizerFactory extends AnalysisTestCase 
{
	public void testSplitting() throws Exception 
  {
    String qpattern = "\\'([^\\']+)\\'"; // get stuff between "'"
    String[][] tests = {
      // group  pattern        input                    output
      { "-1",   "--",          "aaa--bbb--ccc",         "aaa bbb ccc" },
      { "-1",   ":",           "aaa:bbb:ccc",           "aaa bbb ccc" },
      { "-1",   "\\p{Space}",  "aaa   bbb \t\tccc  ",   "aaa   bbb   ccc" },
      { "-1",   ":",           "boo:and:foo",           "boo and foo" },
      { "-1",   "o",           "boo:and:foo",           "b  :and:f" },
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
      String out = TestHyphenatedWordsFilter.tsToString( stream );
      System.out.println( test[2] + " ==> " + out );
      
      assertEquals("pattern: "+test[2], test[3], out );
      
      // Make sure it is the same as if we called 'split'
      if( "-1".equals( test[0] ) ) {
        String[] split = test[2].split( test[1] );
        stream = tokenizer.create( new StringReader( test[2] ) );
        int i=0;
        for( Token t = stream.next(); null != t; t = stream.next() ) 
        {
          assertEquals( "split: "+test[1] + " "+i, split[i++], t.termText() );
        }
      }
    } 
	}
}
