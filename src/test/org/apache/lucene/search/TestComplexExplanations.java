package org.apache.lucene.search;

/**
 * Copyright 2006 Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import org.apache.lucene.analysis.WhitespaceAnalyzer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;

import junit.framework.TestCase;

import java.util.Random;
import java.util.BitSet;

/**
 * TestExplanations subclass that builds up super crazy complex queries
 * on the assumption that if the explanations work out right for them,
 * they should work for anything.
 */
public class TestComplexExplanations extends TestExplanations {

  public void test1() throws Exception {
    
    BooleanQuery q = new BooleanQuery();
    
    q.add(qp.parse("\"w1 w2\"~1"), Occur.MUST);
    q.add(snear(st("w2"),
                sor("w5","zz"),
                4, true),
          Occur.SHOULD);
    q.add(snear(sf("w3",2), st("w2"), st("w3"), 5, true),
          Occur.SHOULD);
    
    Query t = new FilteredQuery(qp.parse("xx"),
                                new ItemizedFilter(new int[] {1,3}));
    t.setBoost(1000);
    q.add(t, Occur.SHOULD);
    
    t = new ConstantScoreQuery(new ItemizedFilter(new int[] {0,2}));
    t.setBoost(30);
    q.add(t, Occur.SHOULD);
    
    DisjunctionMaxQuery dm = new DisjunctionMaxQuery(0.2f);
    dm.add(snear(st("w2"),
                 sor("w5","zz"),
                 4, true));
    dm.add(qp.parse("QQ"));
    dm.add(qp.parse("xx yy -zz"));
    dm.add(qp.parse("-xx -w1"));

    DisjunctionMaxQuery dm2 = new DisjunctionMaxQuery(0.5f);
    dm2.add(qp.parse("w1"));
    dm2.add(qp.parse("w2"));
    dm2.add(qp.parse("w3"));
    dm.add(dm2);

    q.add(dm, Occur.SHOULD);

    BooleanQuery b = new BooleanQuery();
    b.setMinimumNumberShouldMatch(2);
    b.add(snear("w1","w2",1,true), Occur.SHOULD);
    b.add(snear("w2","w3",1,true), Occur.SHOULD);
    b.add(snear("w1","w3",3,true), Occur.SHOULD);

    q.add(b, Occur.SHOULD);
    
    qtest(q, new int[] { 0,1,2 });
    
  }

  // :TODO: we really need more crazy complex cases.

}
