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

package org.apache.lucene.analysis.cn.smart;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Manages analysis data configuration for SmartChineseAnalyzer
 * <p>
 * SmartChineseAnalyzer has a built-in dictionary and stopword list out-of-box.
 * </p>
 * <p>
 * In special circumstances a user may wish to configure SmartChineseAnalyzer with a custom data directory location.
 * </p>
 * AnalyzerProfile is used to determine the location of the data directory containing bigramdict.dct and coredict.dct.
 * The following order is used to determine the location of the data directory:
 * 
 * <ol>
 * <li>System property： -Danalysis.data.dir=/path/to/analysis-data</li>
 * <li>Relative path: analysis-data</li>
 * <li>Relative path: lib/analysis-data</li>
 * <li>Property file: analysis.data.dir property from relative path analysis.properties</li>
 * <li>Property file: analysis.data.dir property from relative path lib/analysis.properties</li>
 * </ol>
 * 
 * Example property file：
 * 
 * <pre>
 * analysis.data.dir=D:/path/to/analysis-data/
 * </pre>
 * <p><font color="#FF0000">
 * WARNING: The status of the analyzers/smartcn <b>analysis.cn</b> package is experimental. 
 * The APIs introduced here might change in the future and will not be 
 * supported anymore in such a case.</font>
 * </p>
 * 
 */
public class AnalyzerProfile {

  /**
   * Global indicating the configured analysis data directory
   */
  public static String ANALYSIS_DATA_DIR = "";

  static {
    init();
  }

  private static void init() {
    String dirName = "analysis-data";
    String propName = "analysis.properties";

    // Try the system property：-Danalysis.data.dir=/path/to/analysis-data
    ANALYSIS_DATA_DIR = System.getProperty("analysis.data.dir", "");
    if (ANALYSIS_DATA_DIR.length() != 0)
      return;

    File[] cadidateFiles = new File[] { new File("./" + dirName),
        new File("./lib/" + dirName), new File("./" + propName),
        new File("./lib/" + propName) };
    for (int i = 0; i < cadidateFiles.length; i++) {
      File file = cadidateFiles[i];
      if (file.exists()) {
        if (file.isDirectory()) {
          ANALYSIS_DATA_DIR = file.getAbsolutePath();
        } else if (file.isFile() && getAnalysisDataDir(file).length() != 0) {
          ANALYSIS_DATA_DIR = getAnalysisDataDir(file);
        }
        break;
      }
    }

    if (ANALYSIS_DATA_DIR.length() == 0) {
      // Dictionary directory cannot be found.
      System.err
          .println("WARNING: Can not find lexical dictionary directory!");
      System.err
          .println("WARNING: This will cause unpredictable exceptions in your application!");
      System.err
          .println("WARNING: Please refer to the manual to download the dictionaries.");
    }

  }

  private static String getAnalysisDataDir(File propFile) {
    Properties prop = new Properties();
    try {
      FileInputStream input = new FileInputStream(propFile);
      prop.load(input);
      String dir = prop.getProperty("analysis.data.dir", "");
      input.close();
      return dir;
    } catch (IOException e) {
    }
    return "";
  }

}
