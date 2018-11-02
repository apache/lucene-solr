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
package org.apache.solr.handler.dataimport;

import java.lang.invoke.MethodHandles;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNestedChildren extends AbstractDIHJdbcTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void test() throws Exception {
    h.query("/dataimport", generateRequest());
    assertQ(req("*:*"), "//*[@numFound='1']");
    assertQ(req("third_s:CHICKEN"), "//*[@numFound='1']");
  } 
  
  @Override
  protected String generateConfig() {
    StringBuilder sb = new StringBuilder();
    sb.append("<dataConfig> \n");
    sb.append("<dataSource name=\"derby\" driver=\"org.apache.derby.jdbc.EmbeddedDriver\" url=\"jdbc:derby:memory:derbyDB;territory=en_US\" /> \n");
    sb.append("<document name=\"TestSimplePropertiesWriter\"> \n");
    sb.append("<entity name=\"FIRST\" processor=\"SqlEntityProcessor\" dataSource=\"derby\" ");
    sb.append(" query=\"select 1 as id, 'PORK' as FIRST_S from sysibm.sysdummy1 \" >\n");
    sb.append("  <field column=\"FIRST_S\" name=\"first_s\" /> \n");
    sb.append("  <entity name=\"SECOND\" processor=\"SqlEntityProcessor\" dataSource=\"derby\" ");
    sb.append("   query=\"select 1 as id, 2 as SECOND_ID, 'BEEF' as SECOND_S from sysibm.sysdummy1 WHERE 1=${FIRST.ID}\" >\n");
    sb.append("   <field column=\"SECOND_S\" name=\"second_s\" /> \n");
    sb.append("   <entity name=\"THIRD\" processor=\"SqlEntityProcessor\" dataSource=\"derby\" ");
    sb.append("    query=\"select 1 as id, 'CHICKEN' as THIRD_S from sysibm.sysdummy1 WHERE 2=${SECOND.SECOND_ID}\" >\n");
    sb.append("    <field column=\"THIRD_S\" name=\"third_s\" /> \n");
    sb.append("   </entity>\n");
    sb.append("  </entity>\n");
    sb.append("</entity>\n");
    sb.append("</document> \n");
    sb.append("</dataConfig> \n");
    String config = sb.toString();
    log.debug(config); 
    return config;
  }
  
  @Override
  protected Database setAllowedDatabases() {
    return Database.DERBY;
  }   
}
