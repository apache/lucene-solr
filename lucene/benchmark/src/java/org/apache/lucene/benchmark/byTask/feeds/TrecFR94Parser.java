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
import java.util.Date;

/**
 * Parser for the FR94 docs in trec disks 4+5 collection format
 */
public class TrecFR94Parser extends TrecDocParser {

  private static final String TEXT = "<TEXT>";
  private static final int TEXT_LENGTH = TEXT.length();
  private static final String TEXT_END = "</TEXT>";
  
  private static final String DATE = "<DATE>";
  private static final String[] DATE_NOISE_PREFIXES = {
    "DATE:",
    "date:", //TODO improve date extraction for this format
    "t.c.",
  };
  private static final String DATE_END = "</DATE>";
  
  //TODO can we also extract title for this format?
  
  @Override
  public DocData parse(DocData docData, String name, TrecContentSource trecSrc, 
      StringBuilder docBuf, ParsePathType pathType) throws IOException {
    int mark = 0; // that much is skipped
    // optionally skip some of the text, set date (no title?)
    Date date = null;
    int h1 = docBuf.indexOf(TEXT);
    if (h1>=0) {
      int h2 = docBuf.indexOf(TEXT_END,h1);
      mark = h1+TEXT_LENGTH;
      // date...
      String dateStr = extract(docBuf, DATE, DATE_END, h2, DATE_NOISE_PREFIXES);
      if (dateStr != null) {
        dateStr = stripTags(dateStr,0).toString();
        date = trecSrc.parseDate(dateStr.trim());
      }
    }
    docData.clear();
    docData.setName(name);
    docData.setDate(date);
    docData.setBody(stripTags(docBuf, mark).toString());
    return docData;
  }

}
