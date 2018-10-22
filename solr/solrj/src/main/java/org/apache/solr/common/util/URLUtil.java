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
package org.apache.solr.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URLUtil {
  
  public final static Pattern URL_PREFIX = Pattern.compile("^([a-z]*?://).*");
  
  public static String removeScheme(String url) {
    Matcher matcher = URL_PREFIX.matcher(url);
    if (matcher.matches()) {
      return url.substring(matcher.group(1).length());
    }
    
    return url;
  }
  
  public static boolean hasScheme(String url) {
    Matcher matcher = URL_PREFIX.matcher(url);
    return matcher.matches();
  }
  
  public static String getScheme(String url) {
    Matcher matcher = URL_PREFIX.matcher(url);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    
    return null;
  }
  
}
