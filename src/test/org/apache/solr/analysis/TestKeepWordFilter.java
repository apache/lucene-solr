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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;


/**
 * @version $Id$
 */
public class TestKeepWordFilter extends BaseTokenTestCase {
  
  public void testStopAndGo() throws Exception 
  {  
    Set<String> words = new HashSet<String>();
    words.add( "aaa" );
    words.add( "bbb" );
    
    String input = "aaa BBB ccc ddd EEE";
    Map<String,String> args = new HashMap<String, String>();

    
    // Test Stopwords
    KeepWordFilterFactory factory = new KeepWordFilterFactory();
    args.put( "ignoreCase", "true" );
    factory.init( args );
    factory.inform( solrConfig.getResourceLoader() );
    factory.setWords( words );
    assertTrue(factory.isIgnoreCase());
    TokenStream stream = factory.create(new WhitespaceTokenizer(new StringReader(input)));
    assertTokenStreamContents(stream, new String[] { "aaa", "BBB" });
    
    // Test Stopwords (ignoreCase via the setter instead)
    factory = new KeepWordFilterFactory();
    args = new HashMap<String, String>();
    factory.init( args );
    factory.inform( solrConfig.getResourceLoader() );
    factory.setIgnoreCase(true);
    factory.setWords( words );
    assertTrue(factory.isIgnoreCase());
    stream = factory.create(new WhitespaceTokenizer(new StringReader(input)));
    assertTokenStreamContents(stream, new String[] { "aaa", "BBB" });
    
    // Now force case
    args = new HashMap<String, String>();
    args.put( "ignoreCase", "false" );
    factory.init( args );
    factory.inform( solrConfig.getResourceLoader() );
    assertFalse(factory.isIgnoreCase());
    stream = factory.create(new WhitespaceTokenizer(new StringReader(input)));
    assertTokenStreamContents(stream, new String[] { "aaa" });
  }
}
