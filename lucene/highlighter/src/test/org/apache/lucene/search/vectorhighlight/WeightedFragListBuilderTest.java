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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;

public class WeightedFragListBuilderTest extends AbstractTestCase {
  public void test2WeightedFragList() throws Exception {
    testCase( pqF( "the", "both" ), 100,
        "subInfos=(theboth((195,203)))/0.8679108(149,249)",
        0.8679108 );
  }

  public void test2SubInfos() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add( pqF( "the", "both" ), Occur.MUST );
    query.add( tq( "examples" ), Occur.MUST );

    testCase( query.build(), 1000,
        "subInfos=(examples((19,27))examples((66,74))theboth((195,203)))/1.8411169(0,1000)",
        1.8411169 );
  }

  private void testCase( Query query, int fragCharSize, String expectedFragInfo,
      double expectedTotalSubInfoBoost ) throws Exception {
    makeIndexLongMV();

    FieldQuery fq = new FieldQuery( query, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    WeightedFragListBuilder wflb = new WeightedFragListBuilder();
    FieldFragList ffl = wflb.createFieldFragList( fpl, fragCharSize );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( expectedFragInfo, ffl.getFragInfos().get( 0 ).toString() );

    float totalSubInfoBoost = 0;
    for ( WeightedFragInfo info : ffl.getFragInfos() ) {
      for ( SubInfo subInfo : info.getSubInfos() ) {
        totalSubInfoBoost += subInfo.getBoost();
      }
    }
    assertEquals( expectedTotalSubInfoBoost, totalSubInfoBoost, .0000001 );
  }

}
