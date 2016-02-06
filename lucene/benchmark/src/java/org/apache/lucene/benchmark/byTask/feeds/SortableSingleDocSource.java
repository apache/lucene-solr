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
package org.apache.lucene.benchmark.byTask.feeds;


import java.util.Properties;
import java.util.Random;

import org.apache.lucene.benchmark.byTask.utils.Config;

/**
 * Adds fields appropriate for sorting: country, random_string and sort_field
 * (int). Supports the following parameters:
 * <ul>
 * <li><b>sort.rng</b> - defines the range for sort-by-int field (default
 * <b>20000</b>).
 * <li><b>rand.seed</b> - defines the seed to initialize Random with (default
 * <b>13</b>).
 * </ul>
 */
public class SortableSingleDocSource extends SingleDocSource {
  
  private static String[] COUNTRIES = new String[] {
    "European Union", "United States", "Japan", "Germany", "China (PRC)", 
    "United Kingdom", "France", "Italy", "Spain", "Canada", "Brazil", "Russia",
    "India", "South Korea", "Australia", "Mexico", "Netherlands", "Turkey", 
    "Sweden", "Belgium", "Indonesia", "Switzerland", "Poland", "Norway", 
    "Republic of China", "Saudi Arabia", "Austria", "Greece", "Denmark", "Iran", 
    "South Africa", "Argentina", "Ireland", "Thailand", "Finland", "Venezuela", 
    "Portugal", "Hong Kong", "United Arab Emirates", "Malaysia", 
    "Czech Republic", "Colombia", "Nigeria", "Romania", "Chile", "Israel", 
    "Singapore", "Philippines", "Pakistan", "Ukraine", "Hungary", "Algeria", 
    "New Zealand", "Egypt", "Kuwait", "Peru", "Kazakhstan", "Slovakia", 
    "Morocco", "Bangladesh", "Vietnam", "Qatar", "Angola", "Libya", "Iraq", 
    "Croatia", "Luxembourg", "Sudan", "Slovenia", "Cuba", "Belarus", "Ecuador", 
    "Serbia", "Oman", "Bulgaria", "Lithuania", "Syria", "Dominican Republic", 
    "Tunisia", "Guatemala", "Azerbaijan", "Sri Lanka", "Kenya", "Latvia", 
    "Turkmenistan", "Costa Rica", "Lebanon", "Uruguay", "Uzbekistan", "Yemen", 
    "Cyprus", "Estonia", "Trinidad and Tobago", "Cameroon", "El Salvador", 
    "Iceland", "Panama", "Bahrain", "Ivory Coast", "Ethiopia", "Tanzania", 
    "Jordan", "Ghana", "Bosnia and Herzegovina", "Macau", "Burma", "Bolivia", 
    "Brunei", "Botswana", "Honduras", "Gabon", "Uganda", "Jamaica", "Zambia", 
    "Senegal", "Paraguay", "Albania", "Equatorial Guinea", "Georgia", 
    "Democratic Republic of the Congo", "Nepal", "Afghanistan", "Cambodia", 
    "Armenia", "Republic of the Congo", "Mozambique", "Republic of Macedonia", 
    "Malta", "Namibia", "Madagascar", "Chad", "Burkina Faso", "Mauritius", 
    "Mali", "The Bahamas", "Papua New Guinea", "Nicaragua", "Haiti", "Benin", 
    "alestinian flag West Bank and Gaza", "Jersey", "Fiji", "Guinea", "Moldova", 
    "Niger", "Laos", "Mongolia", "French Polynesia", "Kyrgyzstan", "Barbados", 
    "Tajikistan", "Malawi", "Liechtenstein", "New Caledonia", "Kosovo", 
    "Rwanda", "Montenegro", "Swaziland", "Guam", "Mauritania", "Guernsey", 
    "Isle of Man", "Togo", "Somalia", "Suriname", "Aruba", "North Korea", 
    "Zimbabwe", "Central African Republic", "Faroe Islands", "Greenland", 
    "Sierra Leone", "Lesotho", "Cape Verde", "Eritrea", "Bhutan", "Belize", 
    "Antigua and Barbuda", "Gibraltar", "Maldives", "San Marino", "Guyana", 
    "Burundi", "Saint Lucia", "Djibouti", "British Virgin Islands", "Liberia", 
    "Seychelles", "The Gambia", "Northern Mariana Islands", "Grenada", 
    "Saint Vincent and the Grenadines", "Saint Kitts and Nevis", "East Timor", 
    "Vanuatu", "Comoros", "Samoa", "Solomon Islands", "Guinea-Bissau", 
    "American Samoa", "Dominica", "Micronesia", "Tonga", "Cook Islands", 
    "Palau", "Marshall Islands", "S�o Tom� and Pr�ncipe", "Anguilla", 
    "Kiribati", "Tuvalu", "Niue" };

  private int sortRange;
  private Random r;

  @Override
  public DocData getNextDocData(DocData docData) throws NoMoreDataException {
    docData = super.getNextDocData(docData);
    Properties props = new Properties();

    // random int
    props.put("sort_field", Integer.toString(r.nextInt(sortRange)));

    // random string
    int len = nextInt(2, 20);
    char[] buffer = new char[len];
    for (int i = 0; i < len; i++) {
      buffer[i] = (char) r.nextInt(0x80); 
    }
    props.put("random_string", new String(buffer));

    // random country
    props.put("country", COUNTRIES[r.nextInt(COUNTRIES.length)]);
    docData.setProps(props);
    return docData;
  }

  private int nextInt(int start, int end) {
    return start + r.nextInt(end - start);
  }

  @Override
  public void setConfig(Config config) {
    super.setConfig(config);
    sortRange = config.get("sort.rng", 20000);
    r = new Random(config.get("rand.seed", 13));
  }
  
}
