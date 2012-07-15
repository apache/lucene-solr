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
 * Parser for the FBIS docs in trec disks 4+5 collection format
 */
public class TrecFBISParser extends TrecDocParser {

  private static final String HEADER = "<HEADER>";
  private static final String HEADER_END = "</HEADER>";
  private static final int HEADER_END_LENGTH = HEADER_END.length();
  
  private static final String DATE1 = "<DATE1>";
  private static final String DATE1_END = "</DATE1>";
  
  private static final String TI = "<TI>";
  private static final String TI_END = "</TI>";

  @Override
  public DocData parse(DocData docData, String name, TrecContentSource trecSrc, 
      StringBuilder docBuf, ParsePathType pathType) throws IOException {
    int mark = 0; // that much is skipped
    // optionally skip some of the text, set date, title
    Date date = null;
    String title = null;
    int h1 = docBuf.indexOf(HEADER);
    if (h1>=0) {
      int h2 = docBuf.indexOf(HEADER_END,h1);
      mark = h2+HEADER_END_LENGTH;
      // date...
      String dateStr = extract(docBuf, DATE1, DATE1_END, h2, null);
      if (dateStr != null) {
        date = trecSrc.parseDate(dateStr);
      }
      // title...
      title = extract(docBuf, TI, TI_END, h2, null);
    }
    docData.clear();
    docData.setName(name);
    docData.setDate(date);
    docData.setTitle(title);
    docData.setBody(stripTags(docBuf, mark).toString());
    return docData;
  }

}
