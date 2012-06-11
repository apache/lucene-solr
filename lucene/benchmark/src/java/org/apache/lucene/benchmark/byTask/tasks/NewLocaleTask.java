package org.apache.lucene.benchmark.byTask.tasks;

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

import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.lucene.benchmark.byTask.PerfRunData;

/**
 * Set a {@link java.util.Locale} for use in benchmarking.
 * <p>
 * Locales can be specified in the following ways:
 * <ul>
 *  <li><code>de</code>: Language "de"
 *  <li><code>en,US</code>: Language "en", country "US"
 *  <li><code>no,NO,NY</code>: Language "no", country "NO", variant "NY" 
 *  <li><code>ROOT</code>: The root (language-agnostic) Locale
 *  <li>&lt;empty string&gt;: Erase the Locale (null)
 * </ul>
 * </p>
 */
public class NewLocaleTask extends PerfTask {
  private String language;
  private String country;
  private String variant;
  
  /**
   * Create a new {@link java.util.Locale} and set it it in the getRunData() for
   * use by all future tasks.
   */
  public NewLocaleTask(PerfRunData runData) {
    super(runData);
  }

  static Locale createLocale(String language, String country, String variant) {
    if (language == null || language.length() == 0) 
      return null;
    
    String lang = language;
    if (lang.equalsIgnoreCase("ROOT"))
      lang = ""; // empty language is the root locale in the JDK
      
    return new Locale(lang, country, variant);
  }
  
  @Override
  public int doLogic() throws Exception {
    Locale locale = createLocale(language, country, variant);
    getRunData().setLocale(locale);
    System.out.println("Changed Locale to: " + 
        (locale == null ? "null" : 
        (locale.getDisplayName().length() == 0) ? "root locale" : locale));
    return 1;
  }
  
  @Override
  public void setParams(String params) {
    super.setParams(params);
    language = country = variant = "";
    StringTokenizer st = new StringTokenizer(params, ",");
    if (st.hasMoreTokens())
      language = st.nextToken();
    if (st.hasMoreTokens())
      country = st.nextToken();
    if (st.hasMoreTokens())
      variant = st.nextToken();
  }

  @Override
  public boolean supportsParams() {
    return true;
  }
}
