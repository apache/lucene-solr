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
package org.apache.lucene.queryparser.surround.query;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.util.LuceneTestCase;

public class Test02Boolean extends LuceneTestCase {
  public static void main(String args[]) {
    TestRunner.run(new TestSuite(Test02Boolean.class));
  }

  final String fieldName = "bi";
  boolean verbose = false;
  int maxBasicQueries = 16;

  String[] docs1 = {
    "word1 word2 word3",
    "word4 word5",
    "ord1 ord2 ord3",
    "orda1 orda2 orda3 word2 worda3",
    "a c e a b c"
  };

  @Override
  public void setUp() throws Exception {
    super.setUp();
    db1 = new SingleFieldTestDb(random(), docs1, fieldName);
  }
  
  SingleFieldTestDb db1;

  public void normalTest1(String query, int[] expdnrs) throws Exception {
    BooleanQueryTst bqt = new BooleanQueryTst( query, expdnrs, db1, fieldName, this,
                                                new BasicQueryFactory(maxBasicQueries));
    bqt.setVerbose(verbose);
    bqt.doTest();
  }

  public void test02Terms01() throws Exception {
    int[] expdnrs = {0}; normalTest1( "word1", expdnrs);
  }
  public void test02Terms02() throws Exception {
    int[] expdnrs = {0, 1, 3}; normalTest1( "word*", expdnrs);
  }
  public void test02Terms03() throws Exception {
    int[] expdnrs = {2}; normalTest1( "ord2", expdnrs);
  }
  public void test02Terms04() throws Exception {
    int[] expdnrs = {}; normalTest1( "kxork*", expdnrs);
  }
  public void test02Terms05() throws Exception {
    int[] expdnrs = {0, 1, 3}; normalTest1( "wor*", expdnrs);
  }
  public void test02Terms06() throws Exception {
    int[] expdnrs = {}; normalTest1( "ab", expdnrs);
  }
  
  public void test02Terms10() throws Exception {
    int[] expdnrs = {}; normalTest1( "abc?", expdnrs);
  }
  public void test02Terms13() throws Exception {
    int[] expdnrs = {0,1,3}; normalTest1( "word?", expdnrs);
  }
  public void test02Terms14() throws Exception {
    int[] expdnrs = {0,1,3}; normalTest1( "w?rd?", expdnrs);
  }
  public void test02Terms20() throws Exception {
    int[] expdnrs = {0,1,3}; normalTest1( "w*rd?", expdnrs);
  }
  public void test02Terms21() throws Exception {
    int[] expdnrs = {3}; normalTest1( "w*rd??", expdnrs);
  }
  public void test02Terms22() throws Exception {
    int[] expdnrs = {3}; normalTest1( "w*?da?", expdnrs);
  }
  public void test02Terms23() throws Exception {
    int[] expdnrs = {}; normalTest1( "w?da?", expdnrs);
  }
  
  public void test03And01() throws Exception {
    int[] expdnrs = {0}; normalTest1( "word1 AND word2", expdnrs);
  }
  public void test03And02() throws Exception {
    int[] expdnrs = {3}; normalTest1( "word* and ord*", expdnrs);
  }
  public void test03And03() throws Exception {
    int[] expdnrs = {0}; normalTest1( "and(word1,word2)", expdnrs);
  }
  public void test04Or01() throws Exception {
    int[] expdnrs = {0, 3}; normalTest1( "word1 or word2", expdnrs);
  }
  public void test04Or02() throws Exception {
    int[] expdnrs = {0, 1, 2, 3}; normalTest1( "word* OR ord*", expdnrs);
  }
  public void test04Or03() throws Exception {
    int[] expdnrs = {0, 3}; normalTest1( "OR (word1, word2)", expdnrs);
  }
  public void test05Not01() throws Exception {
    int[] expdnrs = {3}; normalTest1( "word2 NOT word1", expdnrs);
  }
  public void test05Not02() throws Exception {
    int[] expdnrs = {0}; normalTest1( "word2* not ord*", expdnrs);
  }
  public void test06AndOr01() throws Exception {
    int[] expdnrs = {0}; normalTest1( "(word1 or ab)and or(word2,xyz, defg)", expdnrs);
  }
  public void test07AndOrNot02() throws Exception {
    int[] expdnrs = {0}; normalTest1( "or( word2* not ord*, and(xyz,def))", expdnrs);
  }
}
