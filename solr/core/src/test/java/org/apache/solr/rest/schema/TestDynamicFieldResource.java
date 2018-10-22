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
package org.apache.solr.rest.schema;
import org.apache.solr.rest.SolrRestletTestBase;
import org.junit.Test;

public class TestDynamicFieldResource extends SolrRestletTestBase {
  @Test
  public void testGetDynamicField() throws Exception {
    final boolean expectedDocValues = Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP);
    assertQ("/schema/dynamicfields/*_i?indent=on&wt=xml&showDefaults=on",
            "count(/response/lst[@name='dynamicField']) = 1",
            "/response/lst[@name='dynamicField']/str[@name='name'] = '*_i'",
            "/response/lst[@name='dynamicField']/str[@name='type'] = 'int'",
            "/response/lst[@name='dynamicField']/bool[@name='indexed'] = 'true'",
            "/response/lst[@name='dynamicField']/bool[@name='stored'] = 'true'",
            "/response/lst[@name='dynamicField']/bool[@name='docValues'] = '"+expectedDocValues+"'",
            "/response/lst[@name='dynamicField']/bool[@name='termVectors'] = 'false'",
            "/response/lst[@name='dynamicField']/bool[@name='termPositions'] = 'false'",
            "/response/lst[@name='dynamicField']/bool[@name='termOffsets'] = 'false'",
            "/response/lst[@name='dynamicField']/bool[@name='omitNorms'] = 'true'",
            "/response/lst[@name='dynamicField']/bool[@name='omitTermFreqAndPositions'] = 'true'",
            "/response/lst[@name='dynamicField']/bool[@name='omitPositions'] = 'false'",
            "/response/lst[@name='dynamicField']/bool[@name='storeOffsetsWithPositions'] = 'false'",
            "/response/lst[@name='dynamicField']/bool[@name='multiValued'] = 'false'",
            "/response/lst[@name='dynamicField']/bool[@name='required'] = 'false'",
            "/response/lst[@name='dynamicField']/bool[@name='tokenized'] = 'false'");
  }

  @Test
  public void testGetNotFoundDynamicField() throws Exception {
    assertQ("/schema/dynamicfields/*not_in_there?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");
  } 
  
  @Test
  public void testJsonGetDynamicField() throws Exception {
    final boolean expectedDocValues = Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP);
    assertJQ("/schema/dynamicfields/*_i?indent=on&showDefaults=on",
             "/dynamicField/name=='*_i'",
             "/dynamicField/type=='int'",
             "/dynamicField/indexed==true",
             "/dynamicField/stored==true",
             "/dynamicField/docValues=="+expectedDocValues,
             "/dynamicField/termVectors==false",
             "/dynamicField/termPositions==false",
             "/dynamicField/termOffsets==false",
             "/dynamicField/omitNorms==true",
             "/dynamicField/omitTermFreqAndPositions==true",
             "/dynamicField/omitPositions==false",
             "/dynamicField/storeOffsetsWithPositions==false",
             "/dynamicField/multiValued==false",
             "/dynamicField/required==false",
             "/dynamicField/tokenized==false");
  }
}
