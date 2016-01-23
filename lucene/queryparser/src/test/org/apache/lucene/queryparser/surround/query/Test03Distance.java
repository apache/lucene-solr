package org.apache.lucene.queryparser.surround.query;

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

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.util.LuceneTestCase;

public class Test03Distance extends LuceneTestCase {
  public static void main(String args[]) {
    TestRunner.run(new TestSuite(Test03Distance.class));
  }

  boolean verbose = false;
  int maxBasicQueries = 16;
  
  String [] exceptionQueries = {
    "(aa and bb) w cc",
    "(aa or bb) w (cc and dd)",
    "(aa opt bb) w cc",
    "(aa not bb) w cc",
    "(aa or bb) w (bi:cc)",
    "(aa or bb) w bi:cc",
    "(aa or bi:bb) w cc",
    "(aa or (bi:bb)) w cc",
    "(aa or (bb and dd)) w cc"
  };
  
  public void test00Exceptions() throws Exception {
    String m = ExceptionQueryTst.getFailQueries(exceptionQueries, verbose);
    if (m.length() > 0) {
      fail("No ParseException for:\n" + m);
    }
  }

  final String fieldName = "bi";

  String[] docs1 = {
    "word1 word2 word3",
    "word4 word5",
    "ord1 ord2 ord3",
    "orda1 orda2 orda3 word2 worda3",
    "a c e a b c"
  };

  SingleFieldTestDb db1;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    db1 = new SingleFieldTestDb(random(), docs1, fieldName);
    db2 = new SingleFieldTestDb(random(), docs2, fieldName);
    db3 = new SingleFieldTestDb(random(), docs3, fieldName);
  }
  
  private void distanceTst(String query, int[] expdnrs, SingleFieldTestDb db) throws Exception {
    BooleanQueryTst bqt = new BooleanQueryTst( query, expdnrs, db, fieldName, this,
                                                new BasicQueryFactory(maxBasicQueries));
    bqt.setVerbose(verbose);
    bqt.doTest();
  }

  public void distanceTest1(String query, int[] expdnrs) throws Exception {
    distanceTst(query, expdnrs, db1);
  }
  
  public void test0W01() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word1 w word2", expdnrs);
  }
  public void test0N01() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word1 n word2", expdnrs);
  }
  public void test0N01r() throws Exception { /* r reverse */
    int[] expdnrs = {0}; distanceTest1( "word2 n word1", expdnrs);
  }
  
  public void test0W02() throws Exception {
    int[] expdnrs = {}; distanceTest1( "word2 w word1", expdnrs);
  }
  
  public void test0W03() throws Exception {
    int[] expdnrs = {}; distanceTest1( "word2 2W word1", expdnrs);
  }
  public void test0N03() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word2 2N word1", expdnrs);
  }
  public void test0N03r() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word1 2N word2", expdnrs);
  }
  
  public void test0W04() throws Exception {
    int[] expdnrs = {}; distanceTest1( "word2 3w word1", expdnrs);
  }

  public void test0N04() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word2 3n word1", expdnrs);
  }
  public void test0N04r() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word1 3n word2", expdnrs);
  }

  public void test0W05() throws Exception {
    int[] expdnrs = {}; distanceTest1( "orda1 w orda3", expdnrs);
  }
  public void test0W06() throws Exception {
    int[] expdnrs = {3}; distanceTest1( "orda1 2w orda3", expdnrs);
  }
  
  public void test1Wtrunc01() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word1* w word2", expdnrs);
  }
  public void test1Wtrunc02() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word* w word2", expdnrs);
  }
  public void test1Wtrunc02r() throws Exception {
    int[] expdnrs = {0,3}; distanceTest1( "word2 w word*", expdnrs);
  }
  public void test1Ntrunc02() throws Exception {
    int[] expdnrs = {0,3}; distanceTest1( "word* n word2", expdnrs);
  }
  public void test1Ntrunc02r() throws Exception {
    int[] expdnrs = {0,3}; distanceTest1( "word2 n word*", expdnrs);
  }

  public void test1Wtrunc03() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word1* w word2*", expdnrs);
  }
  public void test1Ntrunc03() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word1* N word2*", expdnrs);
  }
  
  public void test1Wtrunc04() throws Exception {
    int[] expdnrs = {}; distanceTest1( "kxork* w kxor*", expdnrs);
  }
  public void test1Ntrunc04() throws Exception {
    int[] expdnrs = {}; distanceTest1( "kxork* 99n kxor*", expdnrs);
  }

  public void test1Wtrunc05() throws Exception {
    int[] expdnrs = {}; distanceTest1( "word2* 2W word1*", expdnrs);
  }
  public void test1Ntrunc05() throws Exception {
    int[] expdnrs = {0}; distanceTest1( "word2* 2N word1*", expdnrs);
  }

  public void test1Wtrunc06() throws Exception {
    int[] expdnrs = {3}; distanceTest1( "ord* W word*", expdnrs);
  }
  public void test1Ntrunc06() throws Exception {
    int[] expdnrs = {3}; distanceTest1( "ord* N word*", expdnrs);
  }
  public void test1Ntrunc06r() throws Exception {
    int[] expdnrs = {3}; distanceTest1( "word* N ord*", expdnrs);
  }
  
  public void test1Wtrunc07() throws Exception {
    int[] expdnrs = {3}; distanceTest1( "(orda2 OR orda3) W word*", expdnrs);
  }
  public void test1Wtrunc08() throws Exception {
    int[] expdnrs = {3}; distanceTest1( "(orda2 OR orda3) W (word2 OR worda3)", expdnrs);
  }
  public void test1Wtrunc09() throws Exception {
    int[] expdnrs = {3}; distanceTest1( "(orda2 OR orda3) 2W (word2 OR worda3)", expdnrs);
  }
  public void test1Ntrunc09() throws Exception {
    int[] expdnrs = {3}; distanceTest1( "(orda2 OR orda3) 2N (word2 OR worda3)", expdnrs);
  }

  String[] docs2 = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3",
    ""
  };

  SingleFieldTestDb db2;
  
  public void distanceTest2(String query, int[] expdnrs) throws Exception {
    distanceTst(query, expdnrs, db2);
  }
  
  public void test2Wprefix01() throws Exception {
    int[] expdnrs = {0}; distanceTest2( "W (w1, w2, w3)", expdnrs);
  }
  public void test2Nprefix01a() throws Exception {
    int[] expdnrs = {0,1}; distanceTest2( "N(w1, w2, w3)", expdnrs);
  }
  public void test2Nprefix01b() throws Exception {
    int[] expdnrs = {0,1}; distanceTest2( "N(w3, w1, w2)", expdnrs);
  }
  
  public void test2Wprefix02() throws Exception {
    int[] expdnrs = {0,1}; distanceTest2( "2W(w1,w2,w3)", expdnrs);
  }

  public void test2Nprefix02a() throws Exception {
    int[] expdnrs = {0,1}; distanceTest2( "2N(w1,w2,w3)", expdnrs);
  }
  public void test2Nprefix02b() throws Exception {
    int[] expdnrs = {0,1}; distanceTest2( "2N(w2,w3,w1)", expdnrs);
  }

  public void test2Wnested01() throws Exception {
    int[] expdnrs = {0}; distanceTest2( "w1 W w2 W w3", expdnrs);
  }
  public void test2Nnested01() throws Exception {
    int[] expdnrs = {0}; distanceTest2( "w1 N w2 N w3", expdnrs);
  }
  
  public void test2Wnested02() throws Exception {
    int[] expdnrs = {0,1}; distanceTest2( "w1 2W w2 2W w3", expdnrs);
  }
  public void test2Nnested02() throws Exception {
    int[] expdnrs = {0,1}; distanceTest2( "w1 2N w2 2N w3", expdnrs);
  }
  
  String[] docs3 = {
    "low pressure temperature inversion and rain",
    "when the temperature has a negative height above a depression no precipitation gradient is expected",
    "when the temperature has a negative height gradient above a depression no precipitation is expected",
    ""
  };

  SingleFieldTestDb db3;

  public void distanceTest3(String query, int[] expdnrs) throws Exception {
    distanceTst(query, expdnrs, db3);
  }

  public void test3Example01() throws Exception {
    int[] expdnrs = {0,2}; // query does not match doc 1 because "gradient" is in wrong place there.
    distanceTest3("50n((low w pressure*) or depression*,"
                   + "5n(temperat*, (invers* or (negativ* 3n gradient*))),"
                   + "rain* or precipitat*)",
                   expdnrs);
  }
}
