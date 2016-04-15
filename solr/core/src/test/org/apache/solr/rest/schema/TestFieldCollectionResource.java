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


public class TestFieldCollectionResource extends SolrRestletTestBase {
  @Test
  public void testGetAllFields() throws Exception {
    assertQ("/schema/fields?indent=on&wt=xml",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[1] = 'HTMLstandardtok'",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[2] = 'HTMLwhitetok'",
            "(/response/arr[@name='fields']/lst/str[@name='name'])[3] = '_version_'");
  }


  @Test
  public void testJsonGetAllFields() throws Exception {
    assertJQ("/schema/fields?indent=on",
             "/fields/[0]/name=='HTMLstandardtok'",
             "/fields/[1]/name=='HTMLwhitetok'",
             "/fields/[2]/name=='_version_'");
  }


  @Test
  public void testJsonGetAllFieldsIncludeDynamic() throws Exception {
    assertJQ("/schema/fields?indent=on&includeDynamic=true",
             "/fields/[0]/name=='HTMLstandardtok'",
             "/fields/[1]/name=='HTMLwhitetok'",
             "/fields/[2]/name=='_version_'",
             "/fields/[98]/name=='*_d'",
             "/fields/[97]/name=='*_f'",
             "/fields/[96]/name=='*_b'",
             "/fields/[95]/name=='*_t'",
             "/fields/[94]/name=='*_l'"

    );
  }

}
