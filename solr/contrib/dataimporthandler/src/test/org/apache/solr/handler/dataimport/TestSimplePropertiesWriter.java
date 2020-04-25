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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.util.SuppressForbidden;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestSimplePropertiesWriter extends AbstractDIHJdbcTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private boolean useJdbcEscapeSyntax;
  private String dateFormat;
  private String fileLocation;
  private String fileName;
  
  @Before
  public void spwBefore() throws Exception {
    fileLocation = createTempDir().toFile().getAbsolutePath();
    fileName = "the.properties";
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to construct date stamps")
  @Test
  public void testSimplePropertiesWriter() throws Exception { 
    
    SimpleDateFormat errMsgFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT);
    
    String[] d = { 
        "{'ts' ''yyyy-MM-dd HH:mm:ss.SSSSSS''}",
        "{'ts' ''yyyy-MM-dd HH:mm:ss''}",
        "yyyy-MM-dd HH:mm:ss", 
        "yyyy-MM-dd HH:mm:ss.SSSSSS"
    };
    for(int i=0 ; i<d.length ; i++) {
      delQ("*:*");
      commit();
      if(i<2) {
        useJdbcEscapeSyntax = true;
      } else {
        useJdbcEscapeSyntax = false;
      }
      dateFormat = d[i];
      SimpleDateFormat df = new SimpleDateFormat(dateFormat, Locale.ROOT);
      Date oneSecondAgo = new Date(System.currentTimeMillis() - 1000);
      
      Map<String,String> init = new HashMap<>();
      init.put("dateFormat", dateFormat);
      init.put("filename", fileName);
      init.put("directory", fileLocation);
      SimplePropertiesWriter spw = new SimplePropertiesWriter();
      spw.init(new DataImporter(), init);
      Map<String, Object> props = new HashMap<>();
      props.put("SomeDates.last_index_time", oneSecondAgo);
      props.put("last_index_time", oneSecondAgo);
      spw.persist(props);
      
      h.query("/dataimport", generateRequest());  
      props = spw.readIndexerProperties();
      Date entityDate = df.parse((String) props.get("SomeDates.last_index_time"));
      Date docDate= df.parse((String) props.get("last_index_time"));
      int year = currentYearFromDatabase();
      
      assertTrue("This date: " + errMsgFormat.format(oneSecondAgo) + " should be prior to the document date: " + errMsgFormat.format(docDate), docDate.getTime() - oneSecondAgo.getTime() > 0);
      assertTrue("This date: " + errMsgFormat.format(oneSecondAgo) + " should be prior to the entity date: " + errMsgFormat.format(entityDate), entityDate.getTime() - oneSecondAgo.getTime() > 0);
      assertQ(req("*:*"), "//*[@numFound='1']", "//doc/str[@name=\"ayear_s\"]=\"" + year + "\"");    
    }
  }
  
  private int currentYearFromDatabase() throws Exception {
    try (
        Connection conn = newConnection();
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select year(current_timestamp) from sysibm.sysdummy1");
    ){
      if (rs.next()) {
        return rs.getInt(1);
      }
      fail("We should have gotten a row from the db.");
    }
    return 0;
  }
  
  @Override
  protected Database setAllowedDatabases() {
    return Database.DERBY;
  }  
  @Override
  protected String generateConfig() {
    StringBuilder sb = new StringBuilder();
    String q = useJdbcEscapeSyntax ? "" : "'";
    sb.append("<dataConfig> \n");
    sb.append("<propertyWriter dateFormat=\"" + dateFormat + "\" type=\"SimplePropertiesWriter\" directory=\"" + fileLocation + "\" filename=\"" + fileName + "\" />\n");
    sb.append("<dataSource name=\"derby\" driver=\"org.apache.derby.jdbc.EmbeddedDriver\" url=\"jdbc:derby:memory:derbyDB;territory=en_US\" /> \n");
    sb.append("<document name=\"TestSimplePropertiesWriter\"> \n");
    sb.append("<entity name=\"SomeDates\" processor=\"SqlEntityProcessor\" dataSource=\"derby\" ");
    sb.append("query=\"select 1 as id, YEAR(" + q + "${dih.last_index_time}" + q + ") as AYEAR_S from sysibm.sysdummy1 \" >\n");
    sb.append("<field column=\"AYEAR_S\" name=\"ayear_s\" /> \n");
    sb.append("</entity>\n");
    sb.append("</document> \n");
    sb.append("</dataConfig> \n");
    String config = sb.toString();
    log.debug(config); 
    return config;
  }
    
}
