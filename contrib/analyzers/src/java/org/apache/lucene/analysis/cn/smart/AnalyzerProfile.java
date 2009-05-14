/**
 * Copyright 2009 www.imdict.net
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
 * 在默认情况下，SmartChineseAnalyzer内置有词典库、默认停止词库，已经经过封装，用户可以直接使用。
 * 
 * 特殊情况下，用户需要使用指定的词典库和停止词库，此时需要删除org.apache.lucene.analysis.cn.smart. hhmm下的
 * coredict.mem 和 bigramdict.mem， 然后使用AnalyzerProfile来指定词典库目录。
 * 
 * AnalyzerProfile 用来寻找存放分词词库数据 和停用词数据的目录， 该目录下应该有 bigramdict.dct, coredict.dct,
 * stopwords_utf8.txt, 查找过程依次如下：
 * 
 * <ol>
 * <li>读取系统运行时参数：-Danalysis.data.dir=/path/to/analysis-data，如果没有，继续下一条</li>
 * <li>执行命令的当前目录中是否存在analysis-data目录</li>
 * <li>执行命令的lib/目录中是否存在analysis-data目录</li>
 * <li>执行命令的当前目录中是否存在analysis.properties文件</li>
 * <li>执行命令的lib/目录中是否存在analysis.properties文件</li>
 * </ol>
 * 
 * 其中analysis.properties文件analysis.data.dir指明analysis-data目录所在位置.
 * analysis.properties文件的内容示例：
 * 
 * <pre>
 * analysis.data.dir=D:/path/to/analysis-data/
 * </pre>
 * 
 * 当找不到analysis-data目录时，ANALYSIS_DATA_DIR设置为""，因此在使用前，必须在程序里显式指定data目录，例如：
 * 
 * <pre>
 * AnalyzerProfile.ANALYSIS_DATA_DIR = &quot;/path/to/analysis-data&quot;;
 * </pre>
 * 
 */
public class AnalyzerProfile {

  public static String ANALYSIS_DATA_DIR = "";

  static {
    init();
  }

  private static void init() {
    String dirName = "analysis-data";
    String propName = "analysis.properties";

    // 读取系统设置，在运行时加入参数：-Danalysis.data.dir=/path/to/analysis-data
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
      // 提示用户未找到词典文件夹
      System.err
          .println("WARNING: Can not found lexical dictionary directory!");
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
