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

import org.junit.Assert;
import org.apache.solr.common.util.SuppressForbidden;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public abstract class AbstractSqlEntityProcessorTestCase extends
    AbstractDIHJdbcTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected boolean underlyingDataModified;  
  protected boolean useSimpleCaches;
  protected boolean countryEntity;
  protected boolean countryCached;
  protected boolean countryZipper;
  protected boolean sportsEntity;
  protected boolean sportsCached;
  protected boolean sportsZipper;
  
  protected boolean wrongPeopleOrder ;
  protected boolean wrongSportsOrder ;
  protected boolean wrongCountryOrder;
    
  protected String rootTransformerName;
  protected boolean countryTransformer;
  protected boolean sportsTransformer;    
  protected String fileLocation;
  protected String fileName;
  
  @Before
  public void beforeSqlEntitiyProcessorTestCase() throws Exception {
    File tmpdir = createTempDir().toFile();
    fileLocation = tmpdir.getPath();
    fileName = "the.properties";
  } 
  
  @After
  public void afterSqlEntitiyProcessorTestCase() throws Exception {
    useSimpleCaches = false;
    countryEntity = false;
    countryCached = false;
    countryZipper = false;
    sportsEntity = false;
    sportsCached = false;
    sportsZipper = false;
    
    wrongPeopleOrder = false;
    wrongSportsOrder = false;
    wrongCountryOrder= false;
    
    rootTransformerName = null;
    countryTransformer = false;
    sportsTransformer = false;
    underlyingDataModified = false;
    
    //If an Assume was tripped while setting up the test, 
    //the file might not ever have been created...
    if(fileLocation!=null) {
      Files.deleteIfExists(new File(fileLocation + File.separatorChar + fileName).toPath());
      Files.deleteIfExists(new File(fileLocation).toPath());
    }
  }
  
  protected void logPropertiesFile() {
    Map<String,String> init = new HashMap<>();
    init.put("filename", fileName);
    init.put("directory", fileLocation);
    SimplePropertiesWriter spw = new SimplePropertiesWriter();
    spw.init(new DataImporter(), init);
    Map<String,Object> props = spw.readIndexerProperties();
    if(props!=null) {
      StringBuilder sb = new StringBuilder();
      sb.append("\ndataimporter.properties: \n");
      for(Map.Entry<String,Object> entry : props.entrySet()) {
        sb.append("  > key=" + entry.getKey() + " / value=" + entry.getValue() + "\n");
      }
      log.debug(sb.toString());
    }
  }
  
  protected abstract String deltaQueriesCountryTable();
  
  protected abstract String deltaQueriesPersonTable();
  
  protected void singleEntity(int numToExpect) throws Exception {
    h.query("/dataimport", generateRequest());
    assertQ("There should be 1 document per person in the database: "
        + totalPeople(), req("*:*"), "//*[@numFound='" + totalPeople() + "']");
    Assert.assertTrue("Expecting " + numToExpect
        + " database calls, but DIH reported " + totalDatabaseRequests(),
        totalDatabaseRequests() == numToExpect);
  }
  
  protected void simpleTransform(int numToExpect) throws Exception {
    rootTransformerName = "AddAColumnTransformer";
    h.query("/dataimport", generateRequest());
    assertQ(
        "There should be 1 document with a transformer-added column per person is the database: "
            + totalPeople(), req("AddAColumn_s:Added"), "//*[@numFound='"
            + totalPeople() + "']");
    Assert.assertTrue("Expecting " + numToExpect
        + " database calls, but DIH reported " + totalDatabaseRequests(),
        totalDatabaseRequests() == numToExpect);
  }
  
  /**
   * A delta update will not clean up documents added by a transformer even if
   * the parent document that the transformer used to base the new documents
   * were deleted
   */
  protected void complexTransform(int numToExpect, int numDeleted)
      throws Exception {
    rootTransformerName = "TripleThreatTransformer";
    h.query("/dataimport", generateRequest());
    int totalDocs = ((totalPeople() * 3) + (numDeleted * 2));
    int totalAddedDocs = (totalPeople() + numDeleted);
    assertQ(
        req("q", "*:*", "rows", "" + (totalPeople() * 3), "sort", "id asc"),
        "//*[@numFound='" + totalDocs + "']");
    assertQ(req("id:TripleThreat-1-*"), "//*[@numFound='" + totalAddedDocs
        + "']");
    assertQ(req("id:TripleThreat-2-*"), "//*[@numFound='" + totalAddedDocs
        + "']");
    if (personNameExists("Michael") && countryCodeExists("NR")) {
      assertQ(
          "Michael and NR are assured to be in the database.  Therefore the transformer should have added leahciM and RN on the same document as id:TripleThreat-1-3",
          req("+id:TripleThreat-1-3 +NAME_mult_s:Michael +NAME_mult_s:leahciM  +COUNTRY_CODES_mult_s:NR +COUNTRY_CODES_mult_s:RN"),
          "//*[@numFound='1']");
    }
    assertQ(req("AddAColumn_s:Added"), "//*[@numFound='" + totalAddedDocs
        + "']");
    Assert.assertTrue("Expecting " + numToExpect
        + " database calls, but DIH reported " + totalDatabaseRequests(),
        totalDatabaseRequests() == numToExpect);
  }
  
  protected void withChildEntities(boolean cached, boolean checkDatabaseRequests)
      throws Exception {
    rootTransformerName = random().nextBoolean() ? null
        : "AddAColumnTransformer";
    int numChildren = random().nextInt(1) + 1;
    int numDatabaseRequests = 1;
    if (underlyingDataModified) {
      if (countryEntity) {
        if (cached) {
          numDatabaseRequests++;
        } else {
          numDatabaseRequests += totalPeople();
        }
      }
      if (sportsEntity) {
        if (cached) {
          numDatabaseRequests++;
        } else {
          numDatabaseRequests += totalPeople();
        }
      }
    } else {
      countryEntity = true;
      sportsEntity = true;
      if(countryZipper||sportsZipper){// zipper tests fully cover nums of children
        countryEntity = countryZipper;
        sportsEntity = sportsZipper;
      }else{// apply default randomization on cached cases
        if (numChildren == 1) {
          countryEntity = random().nextBoolean();
          sportsEntity = !countryEntity;
        }
      }
      if (countryEntity) {
        countryTransformer = random().nextBoolean();
        if (cached) {
          numDatabaseRequests++;
          countryCached = true;
        } else {
          numDatabaseRequests += totalPeople();
        }
      }
      if (sportsEntity) {
        sportsTransformer = random().nextBoolean();
        if (cached) {
          numDatabaseRequests++;
          sportsCached = true;
        } else {
          numDatabaseRequests += totalPeople();
        }
      }
    }
    h.query("/dataimport", generateRequest());
    
    assertQ("There should be 1 document per person in the database: "
        + totalPeople(), req("*:*"), "//*[@numFound='" + (totalPeople()) + "']");
    if (!underlyingDataModified
        && "AddAColumnTransformer".equals(rootTransformerName)) {
      assertQ(
          "There should be 1 document with a transformer-added column per person is the database: "
              + totalPeople(), req("AddAColumn_s:Added"), "//*[@numFound='"
              + (totalPeople()) + "']");
    }
    if (countryEntity) {
      {
        String[] people = getStringsFromQuery("SELECT NAME FROM PEOPLE WHERE DELETED != 'Y'");
        String man = people[random().nextInt(people.length)];
        String[] countryNames = getStringsFromQuery("SELECT C.COUNTRY_NAME FROM PEOPLE P "
            + "INNER JOIN COUNTRIES C ON P.COUNTRY_CODE=C.CODE "
            + "WHERE P.DELETED!='Y' AND C.DELETED!='Y' AND P.NAME='" + man + "'");

        assertQ(req("{!term f=NAME_mult_s}"+ man), "//*[@numFound='1']",
            countryNames.length>0?
             "//doc/str[@name='COUNTRY_NAME_s']='" + countryNames[random().nextInt(countryNames.length)] + "'"
            :"//doc[count(*[@name='COUNTRY_NAME_s'])=0]");
      }
      {
        String[] countryCodes = getStringsFromQuery("SELECT CODE FROM COUNTRIES WHERE DELETED != 'Y'");
        String theCode = countryCodes[random().nextInt(countryCodes.length)];
        int num = numberPeopleByCountryCode(theCode);
        if(num>0){
          String nrName = countryNameByCode(theCode);
          assertQ(req("COUNTRY_CODES_mult_s:"+theCode), "//*[@numFound='" + num + "']",
              "//doc/str[@name='COUNTRY_NAME_s']='" + nrName + "'");
        }else{ // no one lives there anyway
          assertQ(req("COUNTRY_CODES_mult_s:"+theCode), "//*[@numFound='" + num + "']");
        }
      }
      if (countryTransformer && !underlyingDataModified) {
        assertQ(req("countryAdded_s:country_added"), "//*[@numFound='"
            + totalPeople() + "']");
      }
    }
    if (sportsEntity) {
      if (!underlyingDataModified) {
        assertQ(req("SPORT_NAME_mult_s:Sailing"), "//*[@numFound='2']");
      }
      String [] names = getStringsFromQuery("SELECT NAME FROM PEOPLE WHERE DELETED != 'Y'");
      String name = names[random().nextInt(names.length)];
      int personId = getIntFromQuery("SELECT ID FROM PEOPLE WHERE DELETED != 'Y' AND NAME='"+name+"'");
      String[] michaelsSports = sportNamesByPersonId(personId);

      String[] xpath = new String[michaelsSports.length + 1];
      xpath[0] = "//*[@numFound='1']";
      int i = 1;
      for (String ms : michaelsSports) {
        xpath[i] = "//doc/arr[@name='SPORT_NAME_mult_s']/str='"//[" + i + "]='" don't care about particular order
            + ms + "'";
        i++;
      }
      assertQ(req("NAME_mult_s:" + name.replaceAll("\\W", "\\\\$0")),
            xpath);
      if (!underlyingDataModified && sportsTransformer) {
        assertQ(req("sportsAdded_s:sport_added"), "//*[@numFound='"
            + (totalSportsmen()) + "']");
      }
      assertQ("checking orphan sport is absent",
          req("{!term f=SPORT_NAME_mult_s}No Fishing"), "//*[@numFound='0']");
    }
    if (checkDatabaseRequests) {
      Assert.assertTrue("Expecting " + numDatabaseRequests
          + " database calls, but DIH reported " + totalDatabaseRequests(),
          totalDatabaseRequests() == numDatabaseRequests);
    }
  }
  
  protected void simpleCacheChildEntities(boolean checkDatabaseRequests)
      throws Exception {
    useSimpleCaches = true;
    countryEntity = true;
    sportsEntity = true;
    countryCached = true;
    sportsCached = true;
    int dbRequestsMoreThan = 3;
    int dbRequestsLessThan = totalPeople() * 2 + 1;
    h.query("/dataimport", generateRequest());
    assertQ(req("*:*"), "//*[@numFound='" + (totalPeople()) + "']");
    if (!underlyingDataModified
        || (personNameExists("Samantha") && "Nauru"
            .equals(countryNameByCode("NR")))) {
      assertQ(req("NAME_mult_s:Samantha"), "//*[@numFound='1']",
          "//doc/str[@name='COUNTRY_NAME_s']='Nauru'");
    }
    if (!underlyingDataModified) {
      assertQ(req("COUNTRY_CODES_mult_s:NR"), "//*[@numFound='2']",
          "//doc/str[@name='COUNTRY_NAME_s']='Nauru'");
      assertQ(req("SPORT_NAME_mult_s:Sailing"), "//*[@numFound='2']");
    }
    String[] michaelsSports = sportNamesByPersonId(3);
    if (!underlyingDataModified || michaelsSports.length > 0) {
      String[] xpath = new String[michaelsSports.length + 1];
      xpath[0] = "//*[@numFound='1']";
      int i = 1;
      for (String ms : michaelsSports) {
        xpath[i] = "//doc/arr[@name='SPORT_NAME_mult_s']/str[" + i + "]='" + ms
            + "'";
        i++;
      }
      assertQ(req("NAME_mult_s:Michael"), xpath);
    }
    if (checkDatabaseRequests) {
      Assert.assertTrue("Expecting more than " + dbRequestsMoreThan
          + " database calls, but DIH reported " + totalDatabaseRequests(),
          totalDatabaseRequests() > dbRequestsMoreThan);
      Assert.assertTrue("Expecting fewer than " + dbRequestsLessThan
          + " database calls, but DIH reported " + totalDatabaseRequests(),
          totalDatabaseRequests() < dbRequestsLessThan);
    }
  }
  

  private int getIntFromQuery(String query) throws Exception {
    Connection conn = null;
    Statement s = null;
    ResultSet rs = null;
    try {
      conn = newConnection();
      s = conn.createStatement();
      rs = s.executeQuery(query);
      if (rs.next()) {
        return rs.getInt(1);
      }
      return 0;
    } catch (SQLException e) {
      throw e;
    } finally {
      try {
        rs.close();
      } catch (Exception ex) {}
      try {
        s.close();
      } catch (Exception ex) {}
      try {
        conn.close();
      } catch (Exception ex) {}
    }
  }
  
  private String[] getStringsFromQuery(String query) throws Exception {
    Connection conn = null;
    Statement s = null;
    ResultSet rs = null;
    try {
      conn = newConnection();
      s = conn.createStatement();
      rs = s.executeQuery(query);
      List<String> results = new ArrayList<>();
      while (rs.next()) {
        results.add(rs.getString(1));
      }
      return results.toArray(new String[results.size()]);
    } catch (SQLException e) {
      throw e;
    } finally {
      try {
        rs.close();
      } catch (Exception ex) {}
      try {
        s.close();
      } catch (Exception ex) {}
      try {
        conn.close();
      } catch (Exception ex) {}
    }
  }
  
  public int totalCountries() throws Exception {
    return getIntFromQuery("SELECT COUNT(1) FROM COUNTRIES WHERE DELETED != 'Y' ");
  }
  
  public int totalPeople() throws Exception {
    return getIntFromQuery("SELECT COUNT(1) FROM PEOPLE WHERE DELETED != 'Y' ");
  }
  
  public int totalSportsmen() throws Exception {
    return getIntFromQuery("SELECT COUNT(*) FROM PEOPLE WHERE "
        + "EXISTS(SELECT ID FROM PEOPLE_SPORTS WHERE PERSON_ID=PEOPLE.ID AND PEOPLE_SPORTS.DELETED != 'Y')"
        + " AND PEOPLE.DELETED != 'Y'");
  }
  
  public boolean countryCodeExists(String cc) throws Exception {
    return getIntFromQuery("SELECT COUNT(1) country_name FROM COUNTRIES WHERE DELETED != 'Y' AND CODE='"
        + cc + "'") > 0;
  }
  
  public String countryNameByCode(String cc) throws Exception {
    String[] s = getStringsFromQuery("SELECT country_name FROM COUNTRIES WHERE DELETED != 'Y' AND CODE='"
        + cc + "'");
    return s.length == 0 ? null : s[0];
  }
  
  public int numberPeopleByCountryCode(String cc) throws Exception {
    return getIntFromQuery("Select count(1) " + "from people p "
        + "inner join countries c on p.country_code=c.code "
        + "where p.deleted!='Y' and c.deleted!='Y' and c.code='" + cc + "'");
  }
  
  public String[] sportNamesByPersonId(int personId) throws Exception {
    return getStringsFromQuery("SELECT ps.SPORT_NAME "
        + "FROM people_sports ps "
        + "INNER JOIN PEOPLE p ON p.id = ps.person_Id "
        + "WHERE ps.DELETED != 'Y' AND p.DELETED != 'Y' " + "AND ps.person_id="
        + personId + " " + "ORDER BY ps.id");
  }
  
  public boolean personNameExists(String pn) throws Exception {
    return getIntFromQuery("SELECT COUNT(1) FROM PEOPLE WHERE DELETED != 'Y' AND NAME='"
        + pn + "'") > 0;
  }
  
  public String personNameById(int id) throws Exception {
    String[] nameArr = getStringsFromQuery("SELECT NAME FROM PEOPLE WHERE ID="
        + id);
    if (nameArr.length == 0) {
      return null;
    }
    return nameArr[0];
  }
  
  @SuppressForbidden(reason = "Needs currentTimeMillis to set change time for SQL query")
  public IntChanges modifySomePeople() throws Exception {
    underlyingDataModified = true;
    int numberToChange = random().nextInt(people.length + 1);
    Set<Integer> changeSet = new HashSet<>();
    Set<Integer> deleteSet = new HashSet<>();
    Set<Integer> addSet = new HashSet<>();
    Connection conn = null;
    PreparedStatement change = null;
    PreparedStatement delete = null;
    PreparedStatement add = null;
    // One second in the future ensures a change time after the last import (DIH
    // uses second precision only)
    Timestamp theTime = new Timestamp(System.currentTimeMillis() + 1000);
    log.debug("PEOPLE UPDATE USING TIMESTAMP: "
        + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ROOT)
            .format(theTime));
    try {
      conn = newConnection();
      change = conn
          .prepareStatement("update people set name=?, last_modified=? where id=?");
      delete = conn
          .prepareStatement("update people set deleted='Y', last_modified=? where id=?");
      add = conn
          .prepareStatement("insert into people (id,name,country_code,last_modified) values (?,?,'ZZ',?)");
      for (int i = 0; i < numberToChange; i++) {
        int tryIndex = random().nextInt(people.length);
        Integer id = (Integer) people[tryIndex][0];
        if (!changeSet.contains(id) && !deleteSet.contains(id)) {
          boolean changeDontDelete = random().nextBoolean();
          if (changeDontDelete) {
            changeSet.add(id);
            change.setString(1, "MODIFIED " + people[tryIndex][1]);
            change.setTimestamp(2, theTime);
            change.setInt(3, id);
            Assert.assertEquals(1, change.executeUpdate());
          } else {
            deleteSet.add(id);
            delete.setTimestamp(1, theTime);
            delete.setInt(2, id);
            Assert.assertEquals(1, delete.executeUpdate());
          }
        }
      }
      int numberToAdd = random().nextInt(3);
      for (int i = 0; i < numberToAdd; i++) {
        int tryIndex = random().nextInt(people.length);
        Integer id = (Integer) people[tryIndex][0];
        Integer newId = id + 1000;
        String newDesc = "ADDED " + people[tryIndex][1];
        if (!addSet.contains(newId)) {
          addSet.add(newId);
          add.setInt(1, newId);
          add.setString(2, newDesc);
          add.setTimestamp(3, theTime);
          Assert.assertEquals(1, add.executeUpdate());
        }
      }
      conn.commit();
    } catch (SQLException e) {
      throw e;
    } finally {
      try {
        change.close();
      } catch (Exception ex) {}
      try {
        conn.close();
      } catch (Exception ex) {}
    }
    IntChanges c = new IntChanges();
    c.changedKeys = changeSet.toArray(new Integer[changeSet.size()]);
    c.deletedKeys = deleteSet.toArray(new Integer[deleteSet.size()]);
    c.addedKeys = addSet.toArray(new Integer[addSet.size()]);
    return c;
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to set change time for SQL query")
  public String[] modifySomeCountries() throws Exception {
    underlyingDataModified = true;
    int numberToChange = random().nextInt(countries.length + 1);
    Set<String> changeSet = new HashSet<>();
    Connection conn = null;
    PreparedStatement change = null;
    // One second in the future ensures a change time after the last import (DIH
    // uses second precision only)
    Timestamp theTime = new Timestamp(System.currentTimeMillis() + 1000);
    log.debug("COUNTRY UPDATE USING TIMESTAMP: "
        + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ROOT)
            .format(theTime));
    try {
      conn = newConnection();
      change = conn
          .prepareStatement("update countries set country_name=?, last_modified=? where code=?");
      for (int i = 0; i < numberToChange; i++) {
        int tryIndex = random().nextInt(countries.length);
        String code = countries[tryIndex][0];
        if (!changeSet.contains(code)) {
          changeSet.add(code);
          change.setString(1, "MODIFIED " + countries[tryIndex][1]);
          change.setTimestamp(2, theTime);
          change.setString(3, code);
          Assert.assertEquals(1, change.executeUpdate());
          
        }
      }
    } catch (SQLException e) {
      throw e;
    } finally {
      try {
        change.close();
      } catch (Exception ex) {}
      try {
        conn.close();
      } catch (Exception ex) {}
    }
    return changeSet.toArray(new String[changeSet.size()]);
  }

  static class IntChanges {
    public Integer[] changedKeys;
    public Integer[] deletedKeys;
    public Integer[] addedKeys;
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      if(changedKeys!=null) {
        sb.append("changes: ");
        for(int i : changedKeys) {
          sb.append(i).append(" ");
        }
      }
      if(deletedKeys!=null) {
        sb.append("deletes: ");
        for(int i : deletedKeys) {
          sb.append(i).append(" ");
        }
      }
      if(addedKeys!=null) {
        sb.append("adds: ");
        for(int i : addedKeys) {
          sb.append(i).append(" ");
        }
      }
      return sb.toString();
    }
  }
  
  @Override
  protected String generateConfig() {
    String ds = null;
    if (dbToUse == Database.DERBY) {
      ds = "derby";
    } else if (dbToUse == Database.HSQLDB) {
      ds = "hsqldb";
    } else {
      throw new AssertionError("Invalid database to use: " + dbToUse);
    }
    StringBuilder sb = new StringBuilder();
    sb.append("\n<dataConfig> \n");
    sb.append("<propertyWriter type=''SimplePropertiesWriter'' directory=''" + fileLocation + "'' filename=''" + fileName + "'' />\n");
    sb.append("<dataSource name=''hsqldb'' driver=''org.hsqldb.jdbcDriver'' url=''jdbc:hsqldb:mem:.'' /> \n");
    sb.append("<dataSource name=''derby'' driver=''org.apache.derby.jdbc.EmbeddedDriver'' url=''jdbc:derby:memory:derbyDB;territory=en_US'' /> \n");
    sb.append("<document name=''TestSqlEntityProcessor''> \n");
    sb.append("<entity name=''People'' ");
    sb.append("pk=''" + (random().nextBoolean() ? "ID" : "People.ID") + "'' ");
    sb.append("processor=''SqlEntityProcessor'' ");
    sb.append("dataSource=''" + ds + "'' ");
    sb.append(rootTransformerName != null ? "transformer=''"
        + rootTransformerName + "'' " : "");
  
    sb.append("query=''SELECT ID, NAME, COUNTRY_CODE FROM PEOPLE WHERE DELETED != 'Y' "
                    +((sportsZipper||countryZipper?"ORDER BY ID":"")
                     +(wrongPeopleOrder? " DESC":""))+"'' ");

    sb.append(deltaQueriesPersonTable());
    sb.append("> \n");
    
    sb.append("<field column=''NAME'' name=''NAME_mult_s'' /> \n");
    sb.append("<field column=''COUNTRY_CODE'' name=''COUNTRY_CODES_mult_s'' /> \n");
    
    if (countryEntity) {
      sb.append("<entity name=''Countries'' ");
      sb.append("pk=''" + (random().nextBoolean() ? "CODE" : "Countries.CODE")
          + "'' ");
      sb.append("dataSource=''" + ds + "'' ");
      sb.append(countryTransformer ? "transformer=''AddAColumnTransformer'' "
          + "newColumnName=''countryAdded_s'' newColumnValue=''country_added'' "
          : "");
      if (countryCached) {
        sb.append("processor=''SqlEntityProcessor'' cacheImpl=''SortedMapBackedCache'' ");
        if (useSimpleCaches) {
          sb.append("query=''SELECT CODE, COUNTRY_NAME FROM COUNTRIES WHERE DELETED != 'Y' AND CODE='${People.COUNTRY_CODE}' ''>\n");
        } else {
          
          if(countryZipper){// really odd join btw. it sends duped countries 
            sb.append(random().nextBoolean() ? "cacheKey=''ID'' cacheLookup=''People.ID'' "
                : "where=''ID=People.ID'' ");
            sb.append("join=''zipper'' query=''SELECT PEOPLE.ID, CODE, COUNTRY_NAME FROM COUNTRIES"
                + " JOIN PEOPLE ON COUNTRIES.CODE=PEOPLE.COUNTRY_CODE "
                + "WHERE PEOPLE.DELETED != 'Y' ORDER BY PEOPLE.ID "+
                (wrongCountryOrder ? " DESC":"")
                + "'' ");
          }else{
            sb.append(random().nextBoolean() ? "cacheKey=''CODE'' cacheLookup=''People.COUNTRY_CODE'' "
                : "where=''CODE=People.COUNTRY_CODE'' ");
            sb.append("query=''SELECT CODE, COUNTRY_NAME FROM COUNTRIES'' ");
          }
          sb.append("> \n");
        }
      } else {
        sb.append("processor=''SqlEntityProcessor'' query=''SELECT CODE, COUNTRY_NAME FROM COUNTRIES WHERE DELETED != 'Y' AND CODE='${People.COUNTRY_CODE}' '' ");
        sb.append(deltaQueriesCountryTable());
        sb.append("> \n");
      }
      sb.append("<field column=''CODE'' name=''COUNTRY_CODE_s'' /> \n");
      sb.append("<field column=''COUNTRY_NAME'' name=''COUNTRY_NAME_s'' /> \n");
      sb.append("</entity> \n");
    }
    if (sportsEntity) {
      sb.append("<entity name=''Sports'' ");
      sb.append("dataSource=''" + ds + "'' ");
      sb.append(sportsTransformer ? "transformer=''AddAColumnTransformer'' "
          + "newColumnName=''sportsAdded_s'' newColumnValue=''sport_added'' "
          : "");
      if (sportsCached) {
        sb.append("processor=''SqlEntityProcessor'' cacheImpl=''SortedMapBackedCache'' ");
        if (useSimpleCaches) {
          sb.append("query=''SELECT ID, SPORT_NAME FROM PEOPLE_SPORTS WHERE DELETED != 'Y' AND PERSON_ID=${People.ID} ORDER BY ID'' ");
        } else {
          sb.append(random().nextBoolean() ? "cacheKey=''PERSON_ID'' cacheLookup=''People.ID'' "
              : "where=''PERSON_ID=People.ID'' ");
          if(sportsZipper){
              sb.append("join=''zipper'' query=''SELECT ID, PERSON_ID, SPORT_NAME FROM PEOPLE_SPORTS ORDER BY PERSON_ID"
                  + (wrongSportsOrder?" DESC" : "")+
                  "'' ");
            }
          else{
            sb.append("query=''SELECT ID, PERSON_ID, SPORT_NAME FROM PEOPLE_SPORTS ORDER BY ID'' ");
          }
        }
      } else {
        sb.append("processor=''SqlEntityProcessor'' query=''SELECT ID, SPORT_NAME FROM PEOPLE_SPORTS WHERE DELETED != 'Y' AND PERSON_ID=${People.ID} ORDER BY ID'' ");
      }
      sb.append("> \n");
      sb.append("<field column=''SPORT_NAME'' name=''SPORT_NAME_mult_s'' /> \n");
      sb.append("<field column=''id'' name=''SPORT_ID_mult_s'' /> \n");
      sb.append("</entity> \n");
    }
    
    sb.append("</entity> \n");
    sb.append("</document> \n");
    sb.append("</dataConfig> \n");
    String config = sb.toString().replaceAll("[']{2}", "\"");
    log.debug(config);
    return config;
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to set change time for SQL query")
  @Override
  protected void populateData(Connection conn) throws Exception {
    Statement s = null;
    PreparedStatement ps = null;
    Timestamp theTime = new Timestamp(System.currentTimeMillis() - 10000); // 10 seconds ago
    try {
      s = conn.createStatement();
      s.executeUpdate("create table countries(code varchar(3) not null primary key, country_name varchar(50), deleted char(1) default 'N', last_modified timestamp not null)");
      s.executeUpdate("create table people(id int not null primary key, name varchar(50), country_code char(2), deleted char(1) default 'N', last_modified timestamp not null)");
      s.executeUpdate("create table people_sports(id int not null primary key, person_id int, sport_name varchar(50), deleted char(1) default 'N', last_modified timestamp not null)");
      log.debug("INSERTING DB DATA USING TIMESTAMP: "
          + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ROOT)
              .format(theTime));
      ps = conn
          .prepareStatement("insert into countries (code, country_name, last_modified) values (?,?,?)");
      for (String[] country : countries) {
        ps.setString(1, country[0]);
        ps.setString(2, country[1]);
        ps.setTimestamp(3, theTime);
        Assert.assertEquals(1, ps.executeUpdate());
      }
      ps.close();
      
      ps = conn
          .prepareStatement("insert into people (id, name, country_code, last_modified) values (?,?,?,?)");
      for (Object[] person : people) {
        ps.setInt(1, (Integer) person[0]);
        ps.setString(2, (String) person[1]);
        ps.setString(3, (String) person[2]);
        ps.setTimestamp(4, theTime);
        Assert.assertEquals(1, ps.executeUpdate());
      }
      ps.close();
      
      ps = conn
          .prepareStatement("insert into people_sports (id, person_id, sport_name, last_modified) values (?,?,?,?)");
      for (Object[] sport : people_sports) {
        ps.setInt(1, (Integer) sport[0]);
        ps.setInt(2, (Integer) sport[1]);
        ps.setString(3, (String) sport[2]);
        ps.setTimestamp(4, theTime);
        Assert.assertEquals(1, ps.executeUpdate());
      }
      ps.close();
      conn.commit();
      conn.close();
    } catch (Exception e) {
      throw e;
    } finally {
      try {
        ps.close();
      } catch (Exception ex) {}
      try {
        s.close();
      } catch (Exception ex) {}
      try {
        conn.close();
      } catch (Exception ex) {}
    }
  }
  public static final String[][] countries = {
    {"NA",   "Namibia"},
    {"NC",   "New Caledonia"},
    {"NE",   "Niger"},
    {"NF",   "Norfolk Island"},
    {"NG",   "Nigeria"},
    {"NI",   "Nicaragua"},
    {"NL",   "Netherlands"},
    {"NO",   "Norway"},
    {"NP",   "Nepal"},
    {"NR",   "Nauru"},
    {"NU",   "Niue"},
    {"NZ",   "New Zealand"}
  };
  
  public static final Object[][] people = {
    {1,"Jacob","NZ"},
    {2,"Ethan","NU"},
    {3,"Michael","NR"},
    {4,"Jayden","NP"},
    {5,"William","NO"},
    {6,"Alexander","NL"},
    {7,"Noah","NI"},
    {8,"Daniel","NG"},
    {9,"Aiden","NF"},
    
    {21,"Anthony","NE"}, // there is no ID=10 anymore
    
    {11,"Emma","NL"},
    {12,"Grace","NI"},
    {13,"Hailey","NG"},
    {14,"Isabella","NF"},
    {15,"Lily","NE"},
    {16,"Madison","NC"},
    {17,"Mia","NA"},
    {18,"Natalie","NZ"},
    {19,"Olivia","NU"},
    {20,"Samantha","NR"}
  };
  
  public static final Object[][] people_sports = {
    {100, 1, "Swimming"},
    {200, 2, "Triathlon"},
    {300, 3, "Water polo"},
    {310, 3, "Underwater rugby"},
    {320, 3, "Kayaking"},
    {400, 4, "Snorkeling"},
    {500, 5, "Synchronized diving"},
    {600, 6, "Underwater rugby"},
    {700, 7, "Boating"},
    {800, 8, "Bodyboarding"},
    {900, 9, "Canoeing"},
    
    {1000, 10, "No Fishing"}, // orhpaned sport
    //
    
    {1100, 11, "Jet Ski"},
    {1110, 11, "Rowing"},
    {1120, 11, "Sailing"},
    {1200, 12, "Kayaking"},
    {1210, 12, "Canoeing"},
    {1300, 13, "Kite surfing"},
    {1400, 14, "Parasailing"},
    {1500, 15, "Rafting"},
    //{1600, 16, "Rowing"}, Madison has no sport
    {1700, 17, "Sailing"},
    {1800, 18, "White Water Rafting"},
    {1900, 19, "Water skiing"},
    {2000, 20, "Windsurfing"},
    {2100, 21, "Concrete diving"},
    {2110, 21, "Bubble rugby"}
  }; 
}
