/**
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
package org.apache.solr.hadoop;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.annotations.Beta;


@Beta
public final class Utils {
  
  private static final String LOG_CONFIG_FILE = "hadoop.log4j.configuration";
  
  public static void setLogConfigFile(File file, Configuration conf) {
    conf.set(LOG_CONFIG_FILE, file.getName());
  }

  public static void getLogConfigFile(Configuration conf) {
    String log4jPropertiesFile = conf.get(LOG_CONFIG_FILE);
    if (log4jPropertiesFile != null) {
      PropertyConfigurator.configure(log4jPropertiesFile);
    }
  }

  public static String getShortClassName(Class clazz) {
    return getShortClassName(clazz.getName());
  }
  
  public static String getShortClassName(String className) {
    int i = className.lastIndexOf('.'); // regular class
    int j = className.lastIndexOf('$'); // inner class
    return className.substring(1 + Math.max(i, j));
  }
  
}
