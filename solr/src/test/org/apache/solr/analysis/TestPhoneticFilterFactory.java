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

import org.apache.commons.codec.language.Metaphone;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;


/**
 *
 */
public class TestPhoneticFilterFactory extends BaseTokenTestCase {
  
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
  
  public void testAlgorithms() throws Exception {
    assertAlgorithm("Metaphone", "true", "aaa bbb ccc easgasg",
        new String[] { "A", "aaa", "B", "bbb", "KKK", "ccc", "ESKS", "easgasg" });
    assertAlgorithm("Metaphone", "false", "aaa bbb ccc easgasg",
        new String[] { "A", "B", "KKK", "ESKS" });
    
    assertAlgorithm("DoubleMetaphone", "true", "aaa bbb ccc easgasg",
        new String[] { "A", "aaa", "PP", "bbb", "KK", "ccc", "ASKS", "easgasg" });
    assertAlgorithm("DoubleMetaphone", "false", "aaa bbb ccc easgasg",
        new String[] { "A", "PP", "KK", "ASKS" });
    
    assertAlgorithm("Soundex", "true", "aaa bbb ccc easgasg",
        new String[] { "A000", "aaa", "B000", "bbb", "C000", "ccc", "E220", "easgasg" });
    assertAlgorithm("Soundex", "false", "aaa bbb ccc easgasg",
        new String[] { "A000", "B000", "C000", "E220" });
    
    assertAlgorithm("RefinedSoundex", "true", "aaa bbb ccc easgasg",
        new String[] { "A0", "aaa", "B1", "bbb", "C3", "ccc", "E034034", "easgasg" });
    assertAlgorithm("RefinedSoundex", "false", "aaa bbb ccc easgasg",
        new String[] { "A0", "B1", "C3", "E034034" });
    
    assertAlgorithm("Caverphone", "true", "Darda Karleen Datha Carlene",
        new String[] { "TTA1111111", "Darda", "KLN1111111", "Karleen", 
          "TTA1111111", "Datha", "KLN1111111", "Carlene" });
    assertAlgorithm("Caverphone", "false", "Darda Karleen Datha Carlene",
        new String[] { "TTA1111111", "KLN1111111", "TTA1111111", "KLN1111111" });
  }
  
  static void assertAlgorithm(String algName, String inject, String input,
      String[] expected) throws Exception {
    Tokenizer tokenizer = new WhitespaceTokenizer(DEFAULT_VERSION,
        new StringReader(input));
    Map<String,String> args = new HashMap<String,String>();
    args.put("encoder", algName);
    args.put("inject", inject);
    PhoneticFilterFactory factory = new PhoneticFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, expected);
  }
}
