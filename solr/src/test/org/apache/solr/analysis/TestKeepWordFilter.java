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
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.core.SolrResourceLoader;


/**
 * @version $Id$
 */
public class TestKeepWordFilter extends BaseTokenTestCase {
  
  public void testStopAndGo() throws Exception 
  {  
    Set<String> words = new HashSet<String>();
    words.add( "aaa" );
    words.add( "bbb" );
    
    String input = "xxx yyy aaa zzz BBB ccc ddd EEE";
    Map<String,String> args = new HashMap<String, String>(DEFAULT_VERSION_PARAM);
    ResourceLoader loader = new SolrResourceLoader(null, null);
    
    // Test Stopwords
    KeepWordFilterFactory factory = new KeepWordFilterFactory();
    args.put( "ignoreCase", "true" );
    args.put( "enablePositionIncrements", "true" );
    factory.init( args );
    factory.inform( loader );
    factory.setWords( words );
    assertTrue(factory.isIgnoreCase());
    assertTrue(factory.isEnablePositionIncrements());
    TokenStream stream = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, new StringReader(input)));
    assertTokenStreamContents(stream, new String[] { "aaa", "BBB" }, new int[] { 3, 2 });
    
    // Test Stopwords (ignoreCase via the setter instead)
    factory = new KeepWordFilterFactory();
    args = new HashMap<String, String>(DEFAULT_VERSION_PARAM);
    factory.init( args );
    factory.inform( loader );
    factory.setIgnoreCase(true);
    factory.setWords( words );
    assertTrue(factory.isIgnoreCase());
    assertFalse(factory.isEnablePositionIncrements());
    stream = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, new StringReader(input)));
    assertTokenStreamContents(stream, new String[] { "aaa", "BBB" }, new int[] { 1, 1 });
    
    // Now force case and posIncr
    factory = new KeepWordFilterFactory();
    args = new HashMap<String, String>(DEFAULT_VERSION_PARAM);
    args.put( "ignoreCase", "false" );
    args.put( "enablePositionIncrements", "true" );
    factory.init( args );
    factory.inform( loader );
    factory.setWords( words );    
    assertFalse(factory.isIgnoreCase());
    assertTrue(factory.isEnablePositionIncrements());
    stream = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, new StringReader(input)));
    assertTokenStreamContents(stream, new String[] { "aaa" }, new int[] { 3 });
  }
}
