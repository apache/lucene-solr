package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.IOException;
import java.util.Date;

/**
 * Parser for the FT docs in trec disks 4+5 collection format
 */
public class TrecLATimesParser extends TrecDocParser {

  private static final String DATE = "<DATE>";
  private static final String DATE_END = "</DATE>";
  private static final String DATE_NOISE = "day,"; // anything aftre the ',' 

  private static final String SUBJECT = "<SUBJECT>";
  private static final String SUBJECT_END = "</SUBJECT>";
  private static final String HEADLINE = "<HEADLINE>";
  private static final String HEADLINE_END = "</HEADLINE>";
  
  @Override
  public DocData parse(DocData docData, String name, TrecContentSource trecSrc, 
      StringBuilder docBuf, ParsePathType pathType) throws IOException {
    int mark = 0; // that much is skipped

    // date...
    Date date = null;
    String dateStr = extract(docBuf, DATE, DATE_END, -1, null);
    if (dateStr != null) {
      int d2a = dateStr.indexOf(DATE_NOISE);
      if (d2a > 0) {
        dateStr = dateStr.substring(0,d2a+3); // we need the "day" part
      }
      dateStr = stripTags(dateStr,0).toString();
      date = trecSrc.parseDate(dateStr.trim());
    }
     
    // title... first try with SUBJECT, them with HEADLINE
    String title = extract(docBuf, SUBJECT, SUBJECT_END, -1, null);
    if (title==null) {
      title = extract(docBuf, HEADLINE, HEADLINE_END, -1, null);
    }
    if (title!=null) {
      title = stripTags(title,0).toString().trim();
    }
    
    docData.clear();
    docData.setName(name);
    docData.setDate(date);
    docData.setTitle(title);
    docData.setBody(stripTags(docBuf, mark).toString());
    return docData;
  }

}
