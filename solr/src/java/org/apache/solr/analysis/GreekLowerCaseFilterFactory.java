
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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.el.GreekCharsets;
import org.apache.lucene.analysis.el.GreekLowerCaseFilter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GreekLowerCaseFilterFactory extends BaseTokenFilterFactory 
{
  @Deprecated
  private static Map<String,char[]> CHARSETS = new HashMap<String,char[]>();
  static {
    CHARSETS.put("UnicodeGreek",GreekCharsets.UnicodeGreek);
    CHARSETS.put("ISO",GreekCharsets.ISO);
    CHARSETS.put("CP1253",GreekCharsets.CP1253);
  }
  
  private char[] charset = GreekCharsets.UnicodeGreek;

  private static Logger logger = LoggerFactory.getLogger(GreekLowerCaseFilterFactory.class);
  
  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    String charsetName = args.get("charset");
    if (null != charsetName) {
      charset = CHARSETS.get(charsetName);
      if (charset.equals(GreekCharsets.UnicodeGreek))
        logger.warn("Specifying UnicodeGreek is no longer required (default).  "
            + "Use of the charset parameter will cause an error in Solr 1.5");
      else
        logger.warn("Support for this custom encoding is deprecated.  "
            + "Use of the charset parameter will cause an error in Solr 1.5");
    } else {
      charset = GreekCharsets.UnicodeGreek; /* default to unicode */
    }
    if (null == charset) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
                              "Don't understand charset: " + charsetName);
    }
  }
  public GreekLowerCaseFilter create(TokenStream in) {
    return new GreekLowerCaseFilter(in,charset);
  }
}

