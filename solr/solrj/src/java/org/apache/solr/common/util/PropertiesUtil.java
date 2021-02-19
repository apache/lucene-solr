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

import org.apache.solr.common.SolrException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;


/**
 * Breaking out some utility methods into a separate class as part of SOLR-4196. These utils have nothing to do with
 * the DOM (they came from DomUtils) and it's really confusing to see them in something labeled DOM
 */
public class PropertiesUtil {
  public static String substituteProperty(String value, Properties coreProperties) {
    if(coreProperties == null) return substitute(value, null);
    return substitute(value, coreProperties::getProperty);
  }
  /*
  * This method borrowed from Ant's PropertyHelper.replaceProperties:
  *   http://svn.apache.org/repos/asf/ant/core/trunk/src/main/org/apache/tools/ant/PropertyHelper.java
  */
  public static String substitute(String value, Function<String,String> coreProperties) {
    if (value == null || value.indexOf('$') == -1) {
      return value;
    }

    List<String> fragments = new ArrayList<>();
    List<String> propertyRefs = new ArrayList<>();
    parsePropertyString(value, fragments, propertyRefs);

    StringBuilder sb = new StringBuilder();
    Iterator<String> i = fragments.iterator();
    Iterator<String> j = propertyRefs.iterator();

    while (i.hasNext()) {
      String fragment = i.next();
      if (fragment == null) {
        String propertyName = j.next();
        String defaultValue = null;
        int colon_index = propertyName.indexOf(':');
        if (colon_index > -1) {
          defaultValue = propertyName.substring(colon_index + 1);
          propertyName = propertyName.substring(0, colon_index);
        }
        if (coreProperties != null) {
          fragment = coreProperties.apply(propertyName);
        }
        if (fragment == null) {
          fragment = System.getProperty(propertyName, defaultValue);
        }
        if (fragment == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "No system property or default value specified for " + propertyName + " value:" + value);
        }
      }
      sb.append(fragment);
    }
    return sb.toString();
  }

  /*
   * This method borrowed from Ant's PropertyHelper.parsePropertyStringDefault:
   *   http://svn.apache.org/repos/asf/ant/core/trunk/src/main/org/apache/tools/ant/PropertyHelper.java
   */
  private static void parsePropertyString(String value, List<String> fragments, List<String> propertyRefs) {
    int prev = 0;
    int pos;
    //search for the next instance of $ from the 'prev' position
    while ((pos = value.indexOf("$", prev)) >= 0) {

      //if there was any text before this, add it as a fragment
      //TODO, this check could be modified to go if pos>prev;
      //seems like this current version could stick empty strings
      //into the list
      if (pos > 0) {
        fragments.add(value.substring(prev, pos));
      }
      //if we are at the end of the string, we tack on a $
      //then move past it
      if (pos == (value.length() - 1)) {
        fragments.add("$");
        prev = pos + 1;
      } else if (value.charAt(pos + 1) != '{') {
        //peek ahead to see if the next char is a property or not
        //not a property: insert the char as a literal
              /*
              fragments.addElement(value.substring(pos + 1, pos + 2));
              prev = pos + 2;
              */
        if (value.charAt(pos + 1) == '$') {
          //backwards compatibility two $ map to one mode
          fragments.add("$");
          prev = pos + 2;
        } else {
          //new behaviour: $X maps to $X for all values of X!='$'
          fragments.add(value.substring(pos, pos + 2));
          prev = pos + 2;
        }

      } else {
        //property found, extract its name or bail on a typo
        int endName = value.indexOf('}', pos);
        if (endName < 0) {
          throw new RuntimeException("Syntax error in property: " + value);
        }
        String propertyName = value.substring(pos + 2, endName);
        fragments.add(null);
        propertyRefs.add(propertyName);
        prev = endName + 1;
      }
    }
    //no more $ signs found
    //if there is any tail to the string, append it
    if (prev < value.length()) {
      fragments.add(value.substring(prev));
    }
  }

  /**
   * Parse the given String value as an integer.  If the string cannot
   * be parsed, returns the default
   * @param value    the value to parse
   * @param defValue the default to return if the value cannot be parsed
   * @return an integer version of the passed in value
   */
  public static Integer toInteger(String value, Integer defValue) {
    try {
      return Integer.parseInt(value);
    }
    catch (NumberFormatException e) {
      return defValue;
    }
  }

  public static boolean toBoolean(String value) {
    return "true".equalsIgnoreCase(value) || "on".equalsIgnoreCase(value);
  }

}
