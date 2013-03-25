package org.apache.solr.rest.schema;
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
 * limit     ations under the License.
 */

import org.apache.solr.rest.SolrRestletTestBase;
import org.junit.Test;

public class TestFieldTypeResource extends SolrRestletTestBase {
  @Test
  public void testGetFieldType() throws Exception {
    assertQ("/schema/fieldtypes/float?indent=on&wt=xml&showDefaults=true",
            "count(/response/lst[@name='fieldType']) = 1",
            "count(/response/lst[@name='fieldType']/*) = 18",
            "/response/lst[@name='fieldType']/str[@name='name'] = 'float'",
            "/response/lst[@name='fieldType']/str[@name='class'] = 'solr.TrieFloatField'",
            "/response/lst[@name='fieldType']/str[@name='precisionStep'] ='0'",
            "/response/lst[@name='fieldType']/bool[@name='indexed'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='stored'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='docValues'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='termVectors'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='termPositions'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='termOffsets'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='omitNorms'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='omitTermFreqAndPositions'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='omitPositions'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='storeOffsetsWithPositions'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='multiValued'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='tokenized'] = 'true'",
            "/response/lst[@name='fieldType']/arr[@name='fields']/str = 'weight'",
            "/response/lst[@name='fieldType']/arr[@name='dynamicFields']/str = '*_f'");
  }

  @Test
  public void testGetNotFoundFieldType() throws Exception {
    assertQ("/schema/fieldtypes/not_in_there?indent=on&wt=xml",
            "count(/response/lst[@name='fieldtypes']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");
  }

  @Test
  public void testJsonGetFieldType() throws Exception {
    assertJQ("/schema/fieldtypes/float?indent=on&showDefaults=on",  // assertJQ will add "&wt=json"
             "/fieldType/name=='float'",
             "/fieldType/class=='solr.TrieFloatField'",
             "/fieldType/precisionStep=='0'",
             "/fieldType/indexed==true",
             "/fieldType/stored==true",
             "/fieldType/docValues==false",
             "/fieldType/termVectors==false",
             "/fieldType/termPositions==false",
             "/fieldType/termOffsets==false",
             "/fieldType/omitNorms==true",
             "/fieldType/omitTermFreqAndPositions==true",
             "/fieldType/omitPositions==false",
             "/fieldType/storeOffsetsWithPositions==false",
             "/fieldType/multiValued==false",
             "/fieldType/tokenized==true",
             "/fieldType/fields==['weight']",
             "/fieldType/dynamicFields==['*_f']");
  }
  
  @Test
  public void testGetFieldTypeDontShowDefaults() throws Exception {
    assertQ("/schema/fieldtypes/teststop?wt=xml&indent=on",
            "count(/response/lst[@name='fieldType']/*) = 5",
            "/response/lst[@name='fieldType']/str[@name='name'] = 'teststop'",
            "/response/lst[@name='fieldType']/str[@name='class'] = 'solr.TextField'",
            "/response/lst[@name='fieldType']/lst[@name='analyzer']/lst[@name='tokenizer']/str[@name='class'] = 'solr.LowerCaseTokenizerFactory'",
            "/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst/str[@name='class'][.='solr.StandardFilterFactory']",
            "/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst/str[@name='class'][.='solr.StopFilterFactory']",
            "/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst/str[@name='words'][.='stopwords.txt']",
            "/response/lst[@name='fieldType']/arr[@name='fields']/str[.='teststop']",
            "/response/lst[@name='fieldType']/arr[@name='dynamicFields']");
  }
}
