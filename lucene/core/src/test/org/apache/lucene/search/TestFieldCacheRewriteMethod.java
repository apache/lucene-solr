package org.apache.lucene.search;

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

import org.apache.lucene.index.Term;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Tests the FieldcacheRewriteMethod with random regular expressions
 */
public class TestFieldCacheRewriteMethod extends TestRegexpRandom2 {
  
  /** Test fieldcache rewrite against filter rewrite */
  @Override
  protected void assertSame(String regexp) throws IOException {   
    RegexpQuery fieldCache = new RegexpQuery(new Term(fieldName, regexp), RegExp.NONE);
    fieldCache.setRewriteMethod(new FieldCacheRewriteMethod());
    
    RegexpQuery filter = new RegexpQuery(new Term(fieldName, regexp), RegExp.NONE);
    filter.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
    
    TopDocs fieldCacheDocs = searcher1.search(fieldCache, 25);
    TopDocs filterDocs = searcher2.search(filter, 25);

    CheckHits.checkEqual(fieldCache, fieldCacheDocs.scoreDocs, filterDocs.scoreDocs);
  }
  
  public void testEquals() throws Exception {
    RegexpQuery a1 = new RegexpQuery(new Term(fieldName, "[aA]"), RegExp.NONE);
    RegexpQuery a2 = new RegexpQuery(new Term(fieldName, "[aA]"), RegExp.NONE);
    RegexpQuery b = new RegexpQuery(new Term(fieldName, "[bB]"), RegExp.NONE);
    assertEquals(a1, a2);
    assertFalse(a1.equals(b));
    
    a1.setRewriteMethod(new FieldCacheRewriteMethod());
    a2.setRewriteMethod(new FieldCacheRewriteMethod());
    b.setRewriteMethod(new FieldCacheRewriteMethod());
    assertEquals(a1, a2);
    assertFalse(a1.equals(b));
    QueryUtils.check(a1);
  }
}
