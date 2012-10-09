package org.apache.lucene.search.vectorhighlight;

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

public class WeightedFragListBuilderTest extends AbstractTestCase {
  
  public void test2WeightedFragList() throws Exception {
    
    makeIndexLongMV();

    FieldQuery fq = new FieldQuery( pqF( "the", "both" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    WeightedFragListBuilder wflb = new WeightedFragListBuilder();
    FieldFragList ffl = wflb.createFieldFragList( fpl, 100 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(theboth((195,203)))/0.86791086(149,249)", ffl.getFragInfos().get( 0 ).toString() );
  }

}
