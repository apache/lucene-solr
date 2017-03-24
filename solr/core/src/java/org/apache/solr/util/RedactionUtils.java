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

package org.apache.solr.util;

import java.util.regex.Pattern;

public class RedactionUtils {
  public static final String SOLR_REDACTION_SYSTEM_PATTERN_PROP = "solr.redaction.system.pattern";
  private static Pattern pattern = Pattern.compile(System.getProperty(SOLR_REDACTION_SYSTEM_PATTERN_PROP, ".*password.*"), Pattern.CASE_INSENSITIVE);
  private static final String REDACT_STRING = "--REDACTED--";

  private static boolean redactSystemProperty = Boolean.parseBoolean(System.getProperty("solr.redaction.system.enabled", "false"));

  /**
   * Returns if the given system property should be redacted.
   *
   * @param name The system property that is being checked.
   * @return true if property should be redacted.
   */
  static public boolean isSystemPropertySensitive(String name) {
    return redactSystemProperty && pattern.matcher(name).matches();
  }

  /**
   * @return redaction string to be used instead of the value.
   */
  static public String getRedactString() {
    return REDACT_STRING;
  }

  public static void setRedactSystemProperty(boolean redactSystemProperty) {
    RedactionUtils.redactSystemProperty = redactSystemProperty;
  }


}
