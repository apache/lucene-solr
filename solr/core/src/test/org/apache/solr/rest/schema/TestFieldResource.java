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

public class TestFieldResource extends SolrRestletTestBase {
  @Test
  public void testGetField() throws Exception {
    assertQ("/schema/fields/test_postv?indent=on&wt=xml&showDefaults=true",
            "count(/response/lst[@name='field']) = 1",
            "count(/response/lst[@name='field']/*) = 19",
            "/response/lst[@name='field']/str[@name='name'] = 'test_postv'",
            "/response/lst[@name='field']/str[@name='type'] = 'text'",
            "/response/lst[@name='field']/bool[@name='indexed'] = 'true'",
            "/response/lst[@name='field']/bool[@name='stored'] = 'true'",
            "/response/lst[@name='field']/bool[@name='uninvertible'] = 'true'",
            "/response/lst[@name='field']/bool[@name='docValues'] = 'false'",
            "/response/lst[@name='field']/bool[@name='termVectors'] = 'true'",
            "/response/lst[@name='field']/bool[@name='termPositions'] = 'true'",
            "/response/lst[@name='field']/bool[@name='termPayloads'] = 'false'",
            "/response/lst[@name='field']/bool[@name='termOffsets'] = 'false'",
            "/response/lst[@name='field']/bool[@name='omitNorms'] = 'false'",
            "/response/lst[@name='field']/bool[@name='omitTermFreqAndPositions'] = 'false'",
            "/response/lst[@name='field']/bool[@name='omitPositions'] = 'false'",
            "/response/lst[@name='field']/bool[@name='storeOffsetsWithPositions'] = 'false'",
            "/response/lst[@name='field']/bool[@name='multiValued'] = 'false'",
            "/response/lst[@name='field']/bool[@name='large'] = 'false'",
            "/response/lst[@name='field']/bool[@name='required'] = 'false'",
            "/response/lst[@name='field']/bool[@name='tokenized'] = 'true'",
            "/response/lst[@name='field']/bool[@name='useDocValuesAsStored'] = 'true'");
  }

  @Test
  public void testGetNotFoundField() throws Exception {
    assertQ("/schema/fields/not_in_there?indent=on&wt=xml",
        "count(/response/lst[@name='field']) = 0",
        "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
        "/response/lst[@name='error']/int[@name='code'] = '404'");
  }

  @Test
  public void testJsonGetField() throws Exception {
    assertJQ("/schema/fields/test_postv?indent=on&showDefaults=true",
             "/field/name=='test_postv'",
             "/field/type=='text'",
             "/field/indexed==true",
             "/field/stored==true",
             "/field/uninvertible==true",
             "/field/docValues==false",
             "/field/termVectors==true",
             "/field/termPositions==true",
             "/field/termOffsets==false",
             "/field/termPayloads==false",
             "/field/omitNorms==false",
             "/field/omitTermFreqAndPositions==false",
             "/field/omitPositions==false",
             "/field/storeOffsetsWithPositions==false",
             "/field/multiValued==false",
             "/field/required==false",
             "/field/tokenized==true");
  }
  @Test
  public void testGetFieldIncludeDynamic() throws Exception {
    assertQ("/schema/fields/some_crazy_name_i?indent=on&wt=xml&includeDynamic=true",
        "/response/lst[@name='field']/str[@name='name'] = 'some_crazy_name_i'",
        "/response/lst[@name='field']/str[@name='dynamicBase'] = '*_i'");
  }
  

  @Test
  public void testGetFieldDontShowDefaults() throws Exception {
    String[] tests = { 
        "count(/response/lst[@name='field']) = 1",
        "count(/response/lst[@name='field']/*) = 6",
        "/response/lst[@name='field']/str[@name='name'] = 'id'",
        "/response/lst[@name='field']/str[@name='type'] = 'string'",
        "/response/lst[@name='field']/bool[@name='indexed'] = 'true'",
        "/response/lst[@name='field']/bool[@name='stored'] = 'true'",
        "/response/lst[@name='field']/bool[@name='multiValued'] = 'false'",
        "/response/lst[@name='field']/bool[@name='required'] = 'true'"
    };
    assertQ("/schema/fields/id?indent=on&wt=xml", tests);
    assertQ("/schema/fields/id?indent=on&wt=xml&showDefaults=false", tests);
  }
  
}
