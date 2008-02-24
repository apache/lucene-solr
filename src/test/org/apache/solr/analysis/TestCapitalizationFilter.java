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

import java.util.HashMap;
import java.util.Map;


/**
 * @version $Id$
 */
public class TestCapitalizationFilter extends BaseTokenTestCase {
  
  public void testCapitalization() throws Exception 
  {
    Map<String,String> args = new HashMap<String, String>();
    args.put( CapitalizationFilterFactory.KEEP, "and the it BIG" );
    args.put( CapitalizationFilterFactory.ONLY_FIRST_WORD, "true" );  
    
    CapitalizationFilterFactory factory = new CapitalizationFilterFactory();
    factory.init( args );
    char[] termBuffer;
    termBuffer = "kiTTEN".toCharArray();
    factory.processWord(termBuffer, 0, termBuffer.length, 0 );
    assertEquals( "Kitten",  new String(termBuffer, 0, termBuffer.length));

    factory.forceFirstLetter = true;

    termBuffer = "and".toCharArray();
    factory.processWord(termBuffer, 0, termBuffer.length, 0 );
    assertEquals( "And",  new String(termBuffer, 0, termBuffer.length));//first is forced

    termBuffer = "AnD".toCharArray();
    factory.processWord(termBuffer, 0, termBuffer.length, 0 );
    assertEquals( "And",  new String(termBuffer, 0, termBuffer.length));//first is forced, but it's not a keep word, either

    factory.forceFirstLetter = false;
    termBuffer = "AnD".toCharArray();
    factory.processWord(termBuffer, 0, termBuffer.length, 0 );
    assertEquals( "And",  new String(termBuffer, 0, termBuffer.length)); //first is not forced, but it's not a keep word, either

    factory.forceFirstLetter = true;
    termBuffer = "big".toCharArray();
    factory.processWord(termBuffer, 0, termBuffer.length, 0 );
    assertEquals( "Big",  new String(termBuffer, 0, termBuffer.length));
    termBuffer = "BIG".toCharArray();
    factory.processWord(termBuffer, 0, termBuffer.length, 0 );
    assertEquals( "BIG",  new String(termBuffer, 0, termBuffer.length));
    
    String out = tsToString( factory.create( new IterTokenStream( "Hello thEre my Name is Ryan" ) ) );
    assertEquals( "Hello there my name is ryan", out );
    
    // now each token
    factory.onlyFirstWord = false;
    out = tsToString( factory.create( new IterTokenStream( "Hello thEre my Name is Ryan" ) ) );
    assertEquals( "Hello There My Name Is Ryan", out );
    
    // now only the long words
    factory.minWordLength = 3;
    out = tsToString( factory.create( new IterTokenStream( "Hello thEre my Name is Ryan" ) ) );
    assertEquals( "Hello There my Name is Ryan", out );
    
    // without prefix
    out = tsToString( factory.create( new IterTokenStream( "McKinley" ) ) );
    assertEquals( "Mckinley", out );
    
    // Now try some prefixes
    factory = new CapitalizationFilterFactory();
    args.put( "okPrefix", "McK" );  // all words
    factory.init( args );
    out = tsToString( factory.create( new IterTokenStream( "McKinley" ) ) );
    assertEquals( "McKinley", out );
    
    // now try some stuff with numbers
    factory.forceFirstLetter = false;
    factory.onlyFirstWord = false;
    out = tsToString( factory.create( new IterTokenStream( "1st 2nd third" ) ) );
    assertEquals( "1st 2nd Third", out );
    
    factory.forceFirstLetter = true;
    out = tsToString( factory.create( new IterTokenStream( "the The the" ) ) );
    assertEquals( "The The the", out );
  }

  public void testKeepIgnoreCase() throws Exception {
    Map<String,String> args = new HashMap<String, String>();
    args.put( CapitalizationFilterFactory.KEEP, "kitten" );
    args.put( CapitalizationFilterFactory.KEEP_IGNORE_CASE, "true" );
    args.put( CapitalizationFilterFactory.ONLY_FIRST_WORD, "true" );

    CapitalizationFilterFactory factory = new CapitalizationFilterFactory();
    factory.init( args );
    char[] termBuffer;
    termBuffer = "kiTTEN".toCharArray();
    factory.forceFirstLetter = true;
    factory.processWord(termBuffer, 0, termBuffer.length, 0 );
    assertEquals( "KiTTEN",  new String(termBuffer, 0, termBuffer.length));

    factory.forceFirstLetter = false;
    termBuffer = "kiTTEN".toCharArray();
    factory.processWord(termBuffer, 0, termBuffer.length, 0 );
    assertEquals( "kiTTEN",  new String(termBuffer, 0, termBuffer.length));
  }
}
