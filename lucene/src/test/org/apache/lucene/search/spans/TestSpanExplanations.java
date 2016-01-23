package org.apache.lucene.search.spans;

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

import org.apache.lucene.search.*;


/**
 * TestExplanations subclass focusing on span queries
 */
public class TestSpanExplanations extends TestExplanations {

  /* simple SpanTermQueries */
  
  public void testST1() throws Exception {
    SpanQuery q = st("w1");
    qtest(q, new int[] {0,1,2,3});
  }
  public void testST2() throws Exception {
    SpanQuery q = st("w1");
    q.setBoost(1000);
    qtest(q, new int[] {0,1,2,3});
  }
  public void testST4() throws Exception {
    SpanQuery q = st("xx");
    qtest(q, new int[] {2,3});
  }
  public void testST5() throws Exception {
    SpanQuery q = st("xx");
    q.setBoost(1000);
    qtest(q, new int[] {2,3});
  }

  /* some SpanFirstQueries */
  
  public void testSF1() throws Exception {
    SpanQuery q = sf(("w1"),1);
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSF2() throws Exception {
    SpanQuery q = sf(("w1"),1);
    q.setBoost(1000);
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSF4() throws Exception {
    SpanQuery q = sf(("xx"),2);
    qtest(q, new int[] {2});
  }
  public void testSF5() throws Exception {
    SpanQuery q = sf(("yy"),2);
    qtest(q, new int[] { });
  }
  public void testSF6() throws Exception {
    SpanQuery q = sf(("yy"),4);
    q.setBoost(1000);
    qtest(q, new int[] {2});
  }
  
  /* some SpanOrQueries */

  public void testSO1() throws Exception {
    SpanQuery q = sor("w1","QQ");
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSO2() throws Exception {
    SpanQuery q = sor("w1","w3","zz");
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSO3() throws Exception {
    SpanQuery q = sor("w5","QQ","yy");
    qtest(q, new int[] {0,2,3});
  }
  public void testSO4() throws Exception {
    SpanQuery q = sor("w5","QQ","yy");
    qtest(q, new int[] {0,2,3});
  }

  
  
  /* some SpanNearQueries */
  
  public void testSNear1() throws Exception {
    SpanQuery q = snear("w1","QQ",100,true);
    qtest(q, new int[] {});
  }
  public void testSNear2() throws Exception {
    SpanQuery q = snear("w1","xx",100,true);
    qtest(q, new int[] {2,3});
  }
  public void testSNear3() throws Exception {
    SpanQuery q = snear("w1","xx",0,true);
    qtest(q, new int[] {2});
  }
  public void testSNear4() throws Exception {
    SpanQuery q = snear("w1","xx",1,true);
    qtest(q, new int[] {2,3});
  }
  public void testSNear5() throws Exception {
    SpanQuery q = snear("xx","w1",0,false);
    qtest(q, new int[] {2});
  }

  public void testSNear6() throws Exception {
    SpanQuery q = snear("w1","w2","QQ",100,true);
    qtest(q, new int[] {});
  }
  public void testSNear7() throws Exception {
    SpanQuery q = snear("w1","xx","w2",100,true);
    qtest(q, new int[] {2,3});
  }
  public void testSNear8() throws Exception {
    SpanQuery q = snear("w1","xx","w2",0,true);
    qtest(q, new int[] {2});
  }
  public void testSNear9() throws Exception {
    SpanQuery q = snear("w1","xx","w2",1,true);
    qtest(q, new int[] {2,3});
  }
  public void testSNear10() throws Exception {
    SpanQuery q = snear("xx","w1","w2",0,false);
    qtest(q, new int[] {2});
  }
  public void testSNear11() throws Exception {
    SpanQuery q = snear("w1","w2","w3",1,true);
    qtest(q, new int[] {0,1});
  }

  
  /* some SpanNotQueries */

  public void testSNot1() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("QQ"));
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSNot2() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("QQ"));
    q.setBoost(1000);
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSNot4() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("xx"));
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSNot5() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("xx"));
    q.setBoost(1000);
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSNot7() throws Exception {
    SpanQuery f = snear("w1","w3",10,true);
    f.setBoost(1000);
    SpanQuery q = snot(f, st("xx"));
    qtest(q, new int[] {0,1,3});
  }
  public void testSNot10() throws Exception {
    SpanQuery t = st("xx");
    t.setBoost(10000);
    SpanQuery q = snot(snear("w1","w3",10,true), t);
    qtest(q, new int[] {0,1,3});
  }

}
