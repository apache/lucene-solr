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
package org.apache.lucene.analysis.miscellaneous;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;

public class DateRecognizerFilterFactoryTest extends BaseTokenStreamTestCase {

  public void testBadLanguageTagThrowsException() {
    try { 
      final Map<String,String> args = new HashMap<>();
      args.put(DateRecognizerFilterFactory.LOCALE, "en_US");
      new DateRecognizerFilterFactory(args);
      fail("Bad language tag should have thrown an exception");
    } catch (Exception e) {
      // expected;
    }
  }
  
  public void testGoodLocaleParsesWell() {
    final Map<String,String> args = new HashMap<>();
    args.put(DateRecognizerFilterFactory.LOCALE, "en-US");
    new DateRecognizerFilterFactory(args);
  }
  
}