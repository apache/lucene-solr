package org.apache.lucene.analysis.phonetic;

/*
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

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.Caverphone2;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;

public class TestPhoneticFilterFactory extends BaseTokenStreamTestCase {
  
  /**
   * Case: default
   */
  public void testFactoryDefaults() throws IOException {
    Map<String,String> args = new HashMap<String,String>();
    args.put(PhoneticFilterFactory.ENCODER, "Metaphone");
    PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
    factory.inform(new ClasspathResourceLoader(factory.getClass()));
    assertTrue(factory.getEncoder() instanceof Metaphone);
    assertTrue(factory.inject); // default
  }
  
  public void testInjectFalse() throws IOException {
    Map<String,String> args = new HashMap<String,String>();
    args.put(PhoneticFilterFactory.ENCODER, "Metaphone");
    args.put(PhoneticFilterFactory.INJECT, "false");
    PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
    factory.inform(new ClasspathResourceLoader(factory.getClass()));
    assertFalse(factory.inject);
  }
  
  public void testMaxCodeLength() throws IOException {
    Map<String,String> args = new HashMap<String,String>();
    args.put(PhoneticFilterFactory.ENCODER, "Metaphone");
    args.put(PhoneticFilterFactory.MAX_CODE_LENGTH, "2");
    PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
    factory.inform(new ClasspathResourceLoader(factory.getClass()));
    assertEquals(2, ((Metaphone) factory.getEncoder()).getMaxCodeLen());
  }
  
  /**
   * Case: Failures and Exceptions
   */
  public void testMissingEncoder() throws IOException {
    try {
      new PhoneticFilterFactory(new HashMap<String,String>());
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Configuration Error: missing parameter 'encoder'"));
    }
  }
  
  public void testUnknownEncoder() throws IOException {
    try {
      Map<String,String> args = new HashMap<String,String>();
      args.put("encoder", "XXX");
      PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
      factory.inform(new ClasspathResourceLoader(factory.getClass()));
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Error loading encoder"));
    }
  }
  
  public void testUnknownEncoderReflection() throws IOException {
    try {
      Map<String,String> args = new HashMap<String,String>();
      args.put("encoder", "org.apache.commons.codec.language.NonExistence");
      PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
      factory.inform(new ClasspathResourceLoader(factory.getClass()));
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Error loading encoder"));
    }
  }
  
  /**
   * Case: Reflection
   */
  public void testFactoryReflection() throws IOException {
    Map<String,String> args = new HashMap<String, String>();
    args.put(PhoneticFilterFactory.ENCODER, "org.apache.commons.codec.language.Metaphone");
    PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
    factory.inform(new ClasspathResourceLoader(factory.getClass()));
    assertTrue(factory.getEncoder() instanceof Metaphone);
    assertTrue(factory.inject); // default
  }

  /** 
   * we use "Caverphone2" as it is registered in the REGISTRY as Caverphone,
   * so this effectively tests reflection without package name
   */
  public void testFactoryReflectionCaverphone2() throws IOException {
    Map<String,String> args = new HashMap<String, String>();
    args.put(PhoneticFilterFactory.ENCODER, "Caverphone2");
    PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
    factory.inform(new ClasspathResourceLoader(factory.getClass()));
    assertTrue(factory.getEncoder() instanceof Caverphone2);
    assertTrue(factory.inject); // default
  }
  
  public void testFactoryReflectionCaverphone() throws IOException {
    Map<String,String> args = new HashMap<String, String>();
    args.put(PhoneticFilterFactory.ENCODER, "Caverphone");
    PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
    factory.inform(new ClasspathResourceLoader(factory.getClass()));
    assertTrue(factory.getEncoder() instanceof Caverphone2);
    assertTrue(factory.inject); // default
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
    
    assertAlgorithm("ColognePhonetic", "true", "Meier Schmitt Meir Schmidt",
        new String[] { "67", "Meier", "862", "Schmitt", 
          "67", "Meir", "862", "Schmidt" });
    assertAlgorithm("ColognePhonetic", "false", "Meier Schmitt Meir Schmidt",
        new String[] { "67", "862", "67", "862" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new PhoneticFilterFactory(new HashMap<String,String>() {{
        put("encoder", "Metaphone");
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
  
  static void assertAlgorithm(String algName, String inject, String input,
      String[] expected) throws Exception {
    Tokenizer tokenizer = new MockTokenizer(new StringReader(input), MockTokenizer.WHITESPACE, false);
    Map<String,String> args = new HashMap<String,String>();
    args.put("encoder", algName);
    args.put("inject", inject);
    PhoneticFilterFactory factory = new PhoneticFilterFactory(args);
    factory.inform(new ClasspathResourceLoader(factory.getClass()));
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, expected);
  }
}
