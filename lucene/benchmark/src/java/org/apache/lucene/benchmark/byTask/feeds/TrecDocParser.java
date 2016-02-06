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


import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** 
 * Parser for trec doc content, invoked on doc text excluding &lt;DOC&gt; and &lt;DOCNO&gt;
 * which are handled in TrecContentSource. Required to be stateless and hence thread safe. 
 */
public abstract class TrecDocParser {

  /** Types of trec parse paths, */
  public enum ParsePathType { GOV2, FBIS, FT, FR94, LATIMES }
  
  /** trec parser type used for unknown extensions */
  public static final ParsePathType DEFAULT_PATH_TYPE  = ParsePathType.GOV2;

  static final Map<ParsePathType,TrecDocParser> pathType2parser = new HashMap<>();
  static {
    pathType2parser.put(ParsePathType.GOV2, new TrecGov2Parser());
    pathType2parser.put(ParsePathType.FBIS, new TrecFBISParser());
    pathType2parser.put(ParsePathType.FR94, new TrecFR94Parser());
    pathType2parser.put(ParsePathType.FT, new TrecFTParser());
    pathType2parser.put(ParsePathType.LATIMES, new TrecLATimesParser());
  }

  static final Map<String,ParsePathType> pathName2Type = new HashMap<>();
  static {
    for (ParsePathType ppt : ParsePathType.values()) {
      pathName2Type.put(ppt.name().toUpperCase(Locale.ROOT),ppt);
    }
  }
  
  /** max length of walk up from file to its ancestors when looking for a known path type */ 
  private static final int MAX_PATH_LENGTH = 10;
  
  /**
   * Compute the path type of a file by inspecting name of file and its parents
   */
  public static ParsePathType pathType(Path f) {
    int pathLength = 0;
    while (f != null && f.getFileName() != null && ++pathLength < MAX_PATH_LENGTH) {
      ParsePathType ppt = pathName2Type.get(f.getFileName().toString().toUpperCase(Locale.ROOT));
      if (ppt!=null) {
        return ppt;
      }
      f = f.getParent();
    }
    return DEFAULT_PATH_TYPE;
  }
  
  /** 
   * parse the text prepared in docBuf into a result DocData, 
   * no synchronization is required.
   * @param docData reusable result
   * @param name name that should be set to the result
   * @param trecSrc calling trec content source  
   * @param docBuf text to parse  
   * @param pathType type of parsed file, or null if unknown - may be used by 
   * parsers to alter their behavior according to the file path type. 
   */  
  public abstract DocData parse(DocData docData, String name, TrecContentSource trecSrc, 
      StringBuilder docBuf, ParsePathType pathType) throws IOException;
  
  /** 
   * strip tags from <code>buf</code>: each tag is replaced by a single blank.
   * @return text obtained when stripping all tags from <code>buf</code> (Input StringBuilder is unmodified).
   */
  public static String stripTags(StringBuilder buf, int start) {
    return stripTags(buf.substring(start),0);
  }

  /** 
   * strip tags from input.
   * @see #stripTags(StringBuilder, int)
   */
  public static String stripTags(String buf, int start) {
    if (start>0) {
      buf = buf.substring(0);
    }
    return buf.replaceAll("<[^>]*>", " ");
  }
  
  /**
   * Extract from <code>buf</code> the text of interest within specified tags
   * @param buf entire input text
   * @param startTag tag marking start of text of interest 
   * @param endTag tag marking end of text of interest
   * @param maxPos if &ge; 0 sets a limit on start of text of interest
   * @return text of interest or null if not found
   */
  public static String extract(StringBuilder buf, String startTag, String endTag, int maxPos, String noisePrefixes[]) {
    int k1 = buf.indexOf(startTag);
    if (k1>=0 && (maxPos<0 || k1<maxPos)) {
      k1 += startTag.length();
      int k2 = buf.indexOf(endTag,k1);
      if (k2>=0 && (maxPos<0 || k2<maxPos)) { // found end tag with allowed range
        if (noisePrefixes != null) {
          for (String noise : noisePrefixes) {
            int k1a = buf.indexOf(noise,k1);
            if (k1a>=0 && k1a<k2) {
              k1 = k1a + noise.length();
            }
          }          
        }
        return buf.substring(k1,k2).trim();
      }
    }
    return null;
  }

  //public static void main(String[] args) {
  //  System.out.println(stripTags("is it true that<space>2<<second space>><almost last space>1<one more space>?",0));
  //}

}
