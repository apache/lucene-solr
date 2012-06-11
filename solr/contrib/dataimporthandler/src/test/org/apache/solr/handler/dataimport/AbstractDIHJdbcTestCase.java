package org.apache.solr.handler.dataimport;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
/**
 * This sets up an in-memory Derby Sql database with a little sample data.
 * The schema here is poorly-designed to illustrate DIH's ability to 
 * overcome these kinds of challenges.
 */
public abstract class AbstractDIHJdbcTestCase extends AbstractDataImportHandlerTestCase {
  @BeforeClass
  public static void beforeClassDihJdbcTest() throws Exception {
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
    } catch (Exception e) {
      throw e;
    }
    
    Connection conn = null;
    Statement s = null;
    PreparedStatement ps = null;    
    try {    
      conn = DriverManager.getConnection("jdbc:hsqldb:mem:.");    
      s = conn.createStatement();
      s.executeUpdate("create table countries(code char(2) not null primary key, country_name varchar(50))");
      s.executeUpdate("create table people(id int not null primary key, name varchar(50), country_codes varchar(100))");
      s.executeUpdate("create table people_sports(id int not null primary key, person_id int, sport_name varchar(50))");
      
      ps = conn.prepareStatement("insert into countries values (?,?)");
      for(String[] country : countries) {
        ps.setString(1, country[0]);
        ps.setString(2, country[1]);
        Assert.assertEquals(1, ps.executeUpdate());
      }
      ps.close();
            
      ps = conn.prepareStatement("insert into people values (?,?,?)");
      for(Object[] person : people) {
        ps.setInt(1, (Integer) person[0]);
        ps.setString(2, (String) person[1]);
        ps.setString(3, (String) person[2]);
        Assert.assertEquals(1, ps.executeUpdate());
      }
      ps.close(); 
      
      ps = conn.prepareStatement("insert into people_sports values (?,?,?)");
      for(Object[] sport : people_sports) {
        ps.setInt(1, (Integer) sport[0]);
        ps.setInt(2, (Integer) sport[1]);
        ps.setString(3, (String) sport[2]);
        Assert.assertEquals(1, ps.executeUpdate());
      }
      ps.close();
      conn.close();    
    } catch(Exception e) {
      throw e;
    } finally {
      if(s!=null) { s.close(); }
      if(ps!=null) { ps.close(); }
      if(conn!=null) { conn.close(); }
    }
  }
  
  @AfterClass
  public static void afterClassDihJdbcTest() throws Exception {  
    Connection conn = null;
    Statement s = null;
    try {      
      conn = DriverManager.getConnection("jdbc:hsqldb:mem:.");    
      s = conn.createStatement();
      s.executeUpdate("shutdown");
    } catch (SQLException e) {
      throw e;
    } finally {
      if(s!=null) { s.close(); }
      if(conn!=null) { conn.close(); }
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
    {2,"Ethan","NU,NA,NE"},
    {3,"Michael","NR"},
    {4,"Jayden","NP"},
    {5,"William","NO"},
    {6,"Alexander","NL"},
    {7,"Noah","NI"},
    {8,"Daniel","NG"},
    {9,"Aiden","NF"},
    {10,"Anthony","NE"},
    {11,"Emma","NL"},
    {12,"Grace","NI"},
    {13,"Hailey","NG"},
    {14,"Isabella","NF"},
    {15,"Lily","NE"},
    {16,"Madison","NC"},
    {17,"Mia","NA"},
    {18,"Natalie","NP,NR,NU,NZ"},
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
    {1000, 10, "Fishing"},
    {1100, 11, "Jet Ski"},
    {1110, 11, "Rowing"},
    {1120, 11, "Sailing"},
    {1200, 12, "Kayaking"},
    {1210, 12, "Canoeing"},
    {1300, 13, "Kite surfing"},
    {1400, 14, "Parasailing"},
    {1500, 15, "Rafting"},
    {1600, 16, "Rowing"},
    {1700, 17, "Sailing"},
    {1800, 18, "White Water Rafting"},
    {1900, 19, "Water skiing"},
    {2000, 20, "Windsurfing"}
  };  
  
}
