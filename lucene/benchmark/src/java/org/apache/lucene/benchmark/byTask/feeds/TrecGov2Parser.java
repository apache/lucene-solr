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
import java.io.Reader;
import java.util.Date;

/**
 * Parser for the GOV2 collection format
 */
public class TrecGov2Parser extends TrecDocParser {

  private static final String DATE = "Date: ";
  private static final String DATE_END = TrecContentSource.NEW_LINE;
  
  private static final String DOCHDR = "<DOCHDR>";
  private static final String TERMINATING_DOCHDR = "</DOCHDR>";
  private static final int TERMINATING_DOCHDR_LENGTH = TERMINATING_DOCHDR.length();

  @Override
  public DocData parse(DocData docData, String name, TrecContentSource trecSrc, 
      StringBuilder docBuf, ParsePathType pathType) throws IOException, InterruptedException {
    // Set up a (per-thread) reused Reader over the read content, reset it to re-read from docBuf
    Reader r = trecSrc.getTrecDocReader(docBuf);

    // skip some of the text, optionally set date
    Date date = null;
    int h1 = docBuf.indexOf(DOCHDR);
    if (h1>=0) {
      int h2 = docBuf.indexOf(TERMINATING_DOCHDR,h1);
      String dateStr = extract(docBuf, DATE, DATE_END, h2, null);
      if (dateStr != null) {
        date = trecSrc.parseDate(dateStr);
      }
      r.mark(h2+TERMINATING_DOCHDR_LENGTH);
    }

    r.reset();
    HTMLParser htmlParser = trecSrc.getHtmlParser();
    return htmlParser.parse(docData, name, date, null, r, null);
  }
  
}
