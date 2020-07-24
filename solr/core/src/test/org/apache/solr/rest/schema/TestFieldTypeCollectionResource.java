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
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[2] = 'HTMLstandardtok_dvSort'",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[3] = 'HTMLstandardtok_dvStored'",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[4] = 'HTMLwhitetok'",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[5] = 'HTMLwhitetok_dvSort'",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[6] = 'HTMLwhitetok_dvStored'",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[7] = 'boolean'",
            "(/response/arr[@name='fieldTypes']/lst/str[@name='name'])[8] = 'booleans'");
  }

  @Test
  public void testJsonGetAllFieldTypes() throws Exception {
    assertJQ("/schema/fieldtypes?indent=on",
             "/fieldTypes/[0]/name=='HTMLstandardtok'",
             "/fieldTypes/[1]/name=='HTMLstandardtok_dvSort'",
             "/fieldTypes/[2]/name=='HTMLstandardtok_dvStored'",
             "/fieldTypes/[3]/name=='HTMLwhitetok'",
             "/fieldTypes/[4]/name=='HTMLwhitetok_dvSort'",
             "/fieldTypes/[5]/name=='HTMLwhitetok_dvStored'",
             "/fieldTypes/[6]/name=='boolean'",
             "/fieldTypes/[7]/name=='booleans'");
  }
}
