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

public class TestFieldTypeCollectionResource extends SolrRestletTestBase {

  @Test
  public void testGetAllFieldTypes() throws Exception {
    assertQ("/schema/fieldtypes?indent=on&wt=xml",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[1] = 'HTMLstandardtok'",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[2] = 'HTMLwhitetok'",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[3] = 'boolean'");
  }

  @Test
  public void testJsonGetAllFieldTypes() throws Exception {
    assertJQ("/schema/fieldtypes?indent=on",
             "/fieldTypes/[0]/name=='HTMLstandardtok'",
             "/fieldTypes/[1]/name=='HTMLwhitetok'",
             "/fieldTypes/[2]/name=='boolean'");
  }
}
