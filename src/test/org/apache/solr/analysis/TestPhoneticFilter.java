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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.Encoder;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.lucene.analysis.Token;


/**
 * @version $Id$
 */
public class TestPhoneticFilter extends BaseTokenTestCase {
  
  public void testFactory()
  {
    Map<String,String> args = new HashMap<String, String>();
    
    PhoneticFilterFactory ff = new PhoneticFilterFactory();
    try {
      ff.init( args );
      fail( "missing encoder parameter" );
    }
    catch( Exception ex ) {}
    args.put( PhoneticFilterFactory.ENCODER, "XXX" );
    try {
      ff.init( args );
      fail( "unknown encoder parameter" );
    }
    catch( Exception ex ) {}
    
    args.put( PhoneticFilterFactory.ENCODER, "Metaphone" );
    ff.init( args );
    assertTrue( ff.encoder instanceof Metaphone );
    assertTrue( ff.inject ); // default

    args.put( PhoneticFilterFactory.INJECT, "false" );
    ff.init( args );
    assertFalse( ff.inject );
  }
  
  public void runner( Encoder enc, boolean inject ) throws Exception
  {
    String[] input = new String[] {
       "aaa", "bbb", "ccc", "easgasg"
    };

    ArrayList<Token> stream = new ArrayList<Token>();
    ArrayList<Token> output = new ArrayList<Token>();
    for( String s : input ) {
      stream.add( new Token( s, 0, s.length() ) );

      // phonetic token is added first in the current impl
      output.add( new Token( enc.encode(s).toString(), 0, s.length() ) );

      // add the original if applicable
      if( inject ) {
        output.add( new Token( s, 0, s.length() ) );
      }
    }

    // System.out.println("###stream="+stream);
    // System.out.println("###output="+output);

    PhoneticFilter filter = new PhoneticFilter( 
        new IterTokenStream(stream.iterator()), enc, "text", inject );
    
    for( Token t : output ) {
      Token got = filter.next(t);
      // System.out.println("##### got="+got);

      assertEquals( new String(t.termBuffer(), 0, t.termLength()), new String(got.termBuffer(), 0, got.termLength()));
    }
    assertNull( filter.next() );  // no more tokens
  }
  
  public void testEncodes() throws Exception {
    runner( new DoubleMetaphone(), true );
    runner( new Metaphone(), true );
    runner( new Soundex(), true );
    runner( new RefinedSoundex(), true );

    runner( new DoubleMetaphone(), false );
    runner( new Metaphone(), false );
    runner( new Soundex(), false );
    runner( new RefinedSoundex(), false );
  }
}
