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
import java.sql.Statement;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;

import org.apache.solr.request.SolrQueryRequest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVariableResolverEndToEnd  extends AbstractDIHJdbcTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void test() throws Exception {
    h.query("/dataimport", generateRequest());
    SolrQueryRequest req = null;
    try {
      req = req("q", "*:*", "wt", "json", "indent", "true");
      String response = h.query(req);
      log.debug(response);
      response = response.replaceAll("\\s","");
      Assert.assertTrue(response.contains("\"numFound\":1"));
      Pattern p = Pattern.compile("[\"]second1_s[\"][:][\"](.*?)[\"]");
      Matcher m = p.matcher(response);
      Assert.assertTrue(m.find());
      String yearStr = m.group(1);
      Assert.assertTrue(response.contains("\"second1_s\":\"" + yearStr + "\""));
      Assert.assertTrue(response.contains("\"second2_s\":\"" + yearStr + "\""));
      Assert.assertTrue(response.contains("\"second3_s\":\"" + yearStr + "\""));
      Assert.assertTrue(response.contains("\"PORK_s\":\"GRILL\""));
      Assert.assertTrue(response.contains("\"FISH_s\":\"FRY\""));
      Assert.assertTrue(response.contains("\"BEEF_CUTS_mult_s\":[\"ROUND\",\"SIRLOIN\"]"));
    } catch(Exception e) {
      throw e;
    } finally {
      req.close();
    }
  } 
  
  @Override
  protected String generateConfig() {
    String thirdLocaleParam = random().nextBoolean() ? "" : (", '" + Locale.getDefault().toLanguageTag() + "'");
    StringBuilder sb = new StringBuilder();
    sb.append("<dataConfig> \n");
    sb.append("<dataSource name=\"hsqldb\" driver=\"${dataimporter.request.dots.in.hsqldb.driver}\" url=\"jdbc:hsqldb:mem:.\" /> \n");
    sb.append("<document name=\"TestEvaluators\"> \n");
    sb.append("<entity name=\"FIRST\" processor=\"SqlEntityProcessor\" dataSource=\"hsqldb\" ");
    sb.append(" query=\"" +
        "select " +
        " 1 as id, " +
        " 'SELECT' as SELECT_KEYWORD, " +
        " {ts '2017-02-18 12:34:56'} as FIRST_TS " +
        "from DUAL \" >\n");
    sb.append("  <field column=\"SELECT_KEYWORD\" name=\"select_keyword_s\" /> \n");
    sb.append("  <entity name=\"SECOND\" processor=\"SqlEntityProcessor\" dataSource=\"hsqldb\" transformer=\"TemplateTransformer\" ");
    sb.append("   query=\"" +
        "${dataimporter.functions.encodeUrl(FIRST.SELECT_KEYWORD)} " +
        " 1 as SORT, " +
        " {ts '2017-02-18 12:34:56'} as SECOND_TS, " +
        " '${dataimporter.functions.formatDate(FIRST.FIRST_TS, 'yyyy'" + thirdLocaleParam + ")}' as SECOND1_S,  " +
        " 'PORK' AS MEAT, " +
        " 'GRILL' AS METHOD, " +
        " 'ROUND' AS CUTS, " +
        " 'BEEF_CUTS' AS WHATKIND " +
        "from DUAL " +
        "WHERE 1=${FIRST.ID} " +
        "UNION " +        
        "${dataimporter.functions.encodeUrl(FIRST.SELECT_KEYWORD)} " +
        " 2 as SORT, " +
        " {ts '2017-02-18 12:34:56'} as SECOND_TS, " +
        " '${dataimporter.functions.formatDate(FIRST.FIRST_TS, 'yyyy'" + thirdLocaleParam + ")}' as SECOND1_S,  " +
        " 'FISH' AS MEAT, " +
        " 'FRY' AS METHOD, " +
        " 'SIRLOIN' AS CUTS, " +
        " 'BEEF_CUTS' AS WHATKIND " +
        "from DUAL " +
        "WHERE 1=${FIRST.ID} " +
        "ORDER BY SORT \"" +
        ">\n");
    sb.append("   <field column=\"SECOND_S\" name=\"second_s\" /> \n");
    sb.append("   <field column=\"SECOND1_S\" name=\"second1_s\" /> \n");
    sb.append("   <field column=\"second2_s\" template=\"${dataimporter.functions.formatDate(SECOND.SECOND_TS, 'yyyy'" + thirdLocaleParam + ")}\" /> \n");
    sb.append("   <field column=\"second3_s\" template=\"${dih.functions.formatDate(SECOND.SECOND_TS, 'yyyy'" + thirdLocaleParam + ")}\" /> \n");
    sb.append("   <field column=\"METHOD\" name=\"${SECOND.MEAT}_s\"/>\n");
    sb.append("   <field column=\"CUTS\" name=\"${SECOND.WHATKIND}_mult_s\"/>\n");
    sb.append("  </entity>\n");
    sb.append("</entity>\n");
    sb.append("</document> \n");
    sb.append("</dataConfig> \n");
    String config = sb.toString();
    log.info(config); 
    return config;
  }
  @Override
  protected void populateData(Connection conn) throws Exception {
    Statement s = null;
    try {
      s = conn.createStatement();
      s.executeUpdate("create table dual(dual char(1) not null)");
      s.executeUpdate("insert into dual values('Y')");
      conn.commit();
    } catch (Exception e) {
      throw e;
    } finally {
      try {
        s.close();
      } catch (Exception ex) {}
      try {
        conn.close();
      } catch (Exception ex) {}
    }
  }
  @Override
  protected Database setAllowedDatabases() {
    return Database.HSQLDB;
  }  
}
