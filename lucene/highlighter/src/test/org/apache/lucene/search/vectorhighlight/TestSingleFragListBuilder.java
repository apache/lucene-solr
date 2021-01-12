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
package org.apache.lucene.search.vectorhighlight;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class TestSingleFragListBuilder extends AbstractTestCase {

  public void testNullFieldFragList() throws Exception {
    SingleFragListBuilder sflb = new SingleFragListBuilder();
    FieldFragList ffl =
        sflb.createFieldFragList(fpl(new TermQuery(new Term(F, "a")), "b c d"), 100);
    assertEquals(0, ffl.getFragInfos().size());
  }

  public void testShortFieldFragList() throws Exception {
    SingleFragListBuilder sflb = new SingleFragListBuilder();
    FieldFragList ffl =
        sflb.createFieldFragList(fpl(new TermQuery(new Term(F, "a")), "a b c d"), 100);
    assertEquals(1, ffl.getFragInfos().size());
    assertEquals("subInfos=(a((0,1)))/1.0(0,2147483647)", ffl.getFragInfos().get(0).toString());
  }

  public void testLongFieldFragList() throws Exception {
    SingleFragListBuilder sflb = new SingleFragListBuilder();
    FieldFragList ffl =
        sflb.createFieldFragList(
            fpl(
                new TermQuery(new Term(F, "a")),
                "a b c d",
                "a b c d e f g h i",
                "j k l m n o p q r s t u v w x y z a b c",
                "d e f g"),
            100);
    assertEquals(1, ffl.getFragInfos().size());
    assertEquals(
        "subInfos=(a((0,1))a((8,9))a((60,61)))/3.0(0,2147483647)",
        ffl.getFragInfos().get(0).toString());
  }

  private FieldPhraseList fpl(Query query, String... indexValues) throws Exception {
    make1dmfIndex(indexValues);
    FieldQuery fq = new FieldQuery(query, true, true);
    FieldTermStack stack = new FieldTermStack(reader, 0, F, fq);
    return new FieldPhraseList(stack, fq);
  }
}
