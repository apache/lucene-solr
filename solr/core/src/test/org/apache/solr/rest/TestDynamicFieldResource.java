package org.apache.solr.rest;
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

import org.junit.Test;

public class TestDynamicFieldResource extends SchemaRestletTestBase {
  @Test
  public void testGetDynamicField() throws Exception {
    assertQ("/schema/dynamicfields/*_i?indent=on&wt=xml&showDefaults=on",
            "count(/response/lst[@name='dynamicfield']) = 1",
            "/response/lst[@name='dynamicfield']/str[@name='name'] = '*_i'",
            "/response/lst[@name='dynamicfield']/str[@name='type'] = 'int'",
            "/response/lst[@name='dynamicfield']/bool[@name='indexed'] = 'true'",
            "/response/lst[@name='dynamicfield']/bool[@name='stored'] = 'true'",
            "/response/lst[@name='dynamicfield']/bool[@name='docValues'] = 'false'",
            "/response/lst[@name='dynamicfield']/bool[@name='termVectors'] = 'false'",
            "/response/lst[@name='dynamicfield']/bool[@name='termPositions'] = 'false'",
            "/response/lst[@name='dynamicfield']/bool[@name='termOffsets'] = 'false'",
            "/response/lst[@name='dynamicfield']/bool[@name='omitNorms'] = 'true'",
            "/response/lst[@name='dynamicfield']/bool[@name='omitTermFreqAndPositions'] = 'true'",
            "/response/lst[@name='dynamicfield']/bool[@name='omitPositions'] = 'false'",
            "/response/lst[@name='dynamicfield']/bool[@name='storeOffsetsWithPositions'] = 'false'",
            "/response/lst[@name='dynamicfield']/bool[@name='multiValued'] = 'false'",
            "/response/lst[@name='dynamicfield']/bool[@name='required'] = 'false'",
            "/response/lst[@name='dynamicfield']/bool[@name='tokenized'] = 'false'");
  }

  @Test
  public void testGetNotFoundDynamicField() throws Exception {
    assertQ("/schema/dynamicfields/*not_in_there?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicfield']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");
  } 
  
  @Test
  public void testJsonGetDynamicField() throws Exception {
    assertJQ("/schema/dynamicfields/*_i?indent=on&showDefaults=on",
             "/dynamicfield/name=='*_i'",
             "/dynamicfield/type=='int'",
             "/dynamicfield/indexed==true",
             "/dynamicfield/stored==true",
             "/dynamicfield/docValues==false",
             "/dynamicfield/termVectors==false",
             "/dynamicfield/termPositions==false",
             "/dynamicfield/termOffsets==false",
             "/dynamicfield/omitNorms==true",
             "/dynamicfield/omitTermFreqAndPositions==true",
             "/dynamicfield/omitPositions==false",
             "/dynamicfield/storeOffsetsWithPositions==false",
             "/dynamicfield/multiValued==false",
             "/dynamicfield/required==false",
             "/dynamicfield/tokenized==false");
  }
}
