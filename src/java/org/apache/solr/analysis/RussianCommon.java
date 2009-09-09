
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

package org.apache.solr.analysis;
import org.apache.lucene.analysis.ru.*;
import java.util.Map;
import java.util.HashMap;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class RussianCommon {
  
  private static Logger logger = LoggerFactory.getLogger(RussianCommon.class);
  
  private static Map<String,char[]> CHARSETS = new HashMap<String,char[]>();
  static {
    CHARSETS.put("UnicodeRussian",RussianCharsets.UnicodeRussian);
    CHARSETS.put("KOI8",RussianCharsets.KOI8);
    CHARSETS.put("CP1251",RussianCharsets.CP1251);
  }
  
  public static char[] getCharset(String name) {
    if (null == name)
      return RussianCharsets.UnicodeRussian;

    char[] charset = CHARSETS.get(name);
    
    if (charset.equals(RussianCharsets.UnicodeRussian))
      logger.warn("Specifying UnicodeRussian is no longer required (default).  "
          + "Use of the charset parameter will cause an error in Solr 1.5");
    else
      logger.warn("Support for this custom encoding is deprecated.  "
          + "Use of the charset parameter will cause an error in Solr 1.5");
    
    if (null == charset) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
                              "Don't understand charset: " + name);
    }
    return charset;
  }
}

