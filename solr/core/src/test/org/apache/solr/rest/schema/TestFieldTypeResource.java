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

public class TestFieldTypeResource extends SolrRestletTestBase {
  @Test
  public void testXMLGetFieldType() throws Exception {
    final String expectedFloatClass = RANDOMIZED_NUMERIC_FIELDTYPES.get(Float.class);
    final boolean expectedDocValues = Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP);
    assertQ("/schema/fieldtypes/float?wt=xml&showDefaults=true",
            "count(/response/lst[@name='fieldType']) = 1",
            "count(/response/lst[@name='fieldType']/*) = 18",
            "/response/lst[@name='fieldType']/str[@name='name'] = 'float'",
            "/response/lst[@name='fieldType']/str[@name='class'] = '"+expectedFloatClass+"'",
            "/response/lst[@name='fieldType']/str[@name='precisionStep'] ='0'",
            "/response/lst[@name='fieldType']/bool[@name='indexed'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='stored'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='uninvertible'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='docValues'] = '"+expectedDocValues+"'",
            "/response/lst[@name='fieldType']/bool[@name='termVectors'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='termPositions'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='termOffsets'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='omitNorms'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='omitTermFreqAndPositions'] = 'true'",
            "/response/lst[@name='fieldType']/bool[@name='omitPositions'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='storeOffsetsWithPositions'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='multiValued'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='large'] = 'false'",
            "/response/lst[@name='fieldType']/bool[@name='tokenized'] = 'false'");
  }

  @Test
  public void testXMLGetNotFoundFieldType() throws Exception {
    assertQ("/schema/fieldtypes/not_in_there?wt=xml",
            "count(/response/lst[@name='fieldtypes']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");
  }

  @Test
  public void testJsonGetFieldType() throws Exception {
    final String expectedFloatClass = RANDOMIZED_NUMERIC_FIELDTYPES.get(Float.class);
    final boolean expectedDocValues = Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP);
    assertJQ("/schema/fieldtypes/float?showDefaults=on",
             "/fieldType/name=='float'",
             "/fieldType/class=='"+expectedFloatClass+"'",
             "/fieldType/precisionStep=='0'",
             "/fieldType/indexed==true",
             "/fieldType/stored==true",
             "/fieldType/uninvertible==true",
             "/fieldType/docValues=="+expectedDocValues,
             "/fieldType/termVectors==false",
             "/fieldType/termPositions==false",
             "/fieldType/termOffsets==false",
             "/fieldType/omitNorms==true",
             "/fieldType/omitTermFreqAndPositions==true",
             "/fieldType/omitPositions==false",
             "/fieldType/storeOffsetsWithPositions==false",
             "/fieldType/multiValued==false",
             "/fieldType/tokenized==false");
  }
  
  @Test
  public void testXMLGetFieldTypeDontShowDefaults() throws Exception {
    assertQ("/schema/fieldtypes/teststop?wt=xml",
            "count(/response/lst[@name='fieldType']/*) = 3",
            "/response/lst[@name='fieldType']/str[@name='name'] = 'teststop'",
            "/response/lst[@name='fieldType']/str[@name='class'] = 'solr.TextField'",
            "/response/lst[@name='fieldType']/lst[@name='analyzer']/lst[@name='tokenizer']/str[@name='class'] = 'solr.LetterTokenizerFactory'",
            "/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst/str[@name='class'][.='solr.LowerCaseFilterFactory']",
            "/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst/str[@name='class'][.='solr.StopFilterFactory']",
            "/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst/str[@name='words'][.='stopwords.txt']"
            );
  }
}
