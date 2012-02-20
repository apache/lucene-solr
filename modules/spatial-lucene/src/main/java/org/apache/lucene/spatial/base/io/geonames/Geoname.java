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

package org.apache.lucene.spatial.base.io.geonames;

import java.sql.Date;

public class Geoname {
  public int id;
  public String name; // name of geographical point (utf8) varchar(200)
  public String nameASCII; // name of geographical point in plain ascii characters, varchar(200)
  public String[] alternateNames; // alternatenames, comma separated varchar(5000)
  public double latitude;
  public double longitude;
  public char featureClass;
  public String featureCode; // 10
  public String countryCode; // 2
  public String[] countryCode2; // alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
  public String adminCode1; // fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
  public String adminCode2; // code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
  public String adminCode3; // code for third level administrative division, varchar(20)
  public String adminCode4; // code for fourth level administrative division, varchar(20)
  public Long population;
  public Integer elevation; // in meters, integer
  public Integer gtopo30;   // average elevation of 30'x30' (ca 900mx900m) area in meters, integer
  public String timezone;
  public Date modified;  // date of last modification in yyyy-MM-dd format

  public Geoname(String line) {
    String[] vals = line.split("\t");
    id = Integer.parseInt(vals[0]);
    name = vals[1];
    nameASCII = vals[2];
    alternateNames = vals[3].split(",");
    latitude = Double.parseDouble(vals[4]);
    longitude = Double.parseDouble(vals[5]);
    featureClass = vals[6].length() > 0 ? vals[6].charAt(0) : 'S';
    featureCode = vals[7];
    countryCode = vals[8];
    countryCode2 = vals[9].split(",");
    adminCode1 = vals[10];
    adminCode2 = vals[11];
    adminCode3 = vals[12];
    adminCode4 = vals[13];
    if (vals[14].length() > 0) {
      population = Long.decode(vals[14]);
    }
    if (vals[15].length() > 0) {
      elevation = Integer.decode(vals[15]);
    }
    if (vals[16].length() > 0) {
      gtopo30 = Integer.decode(vals[16]);
    }
    timezone = vals[17];
    if (vals[18].length() > 0) {
      modified = Date.valueOf(vals[18]);
    }
  }
}
