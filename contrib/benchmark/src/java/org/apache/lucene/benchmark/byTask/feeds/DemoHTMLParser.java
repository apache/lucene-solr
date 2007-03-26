package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

/**
 * HTML Parser that is based on Lucene's demo HTML parser.
 */
public class DemoHTMLParser implements org.apache.lucene.benchmark.byTask.feeds.HTMLParser {

  DateFormat dateFormat;
  
  public DemoHTMLParser () {
    dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy kk:mm:ss ",Locale.US);  //Tue, 09 Dec 2003 22:39:08 GMT
    dateFormat.setLenient(true);
  }

  /*
   *  (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.HTMLParser#parse(java.lang.String, java.util.Date, java.io.Reader, java.text.DateFormat)
   */
  public DocData parse(String name, Date date, Reader reader, DateFormat dateFormat) throws IOException, InterruptedException {
    org.apache.lucene.demo.html.HTMLParser p = new org.apache.lucene.demo.html.HTMLParser(reader);
    
    // title
    String title = p.getTitle();
    // properties 
    Properties props = p.getMetaTags(); 
    // body
    Reader r = p.getReader();
    char c[] = new char[1024];
    StringBuffer bodyBuf = new StringBuffer();
    int n;
    while ((n = r.read(c)) >= 0) {
      if (n>0) {
        bodyBuf.append(c,0,n);
      }
    }
    r.close();
    if (date == null && props.getProperty("date")!=null) {
      try {
        date = dateFormat.parse(props.getProperty("date").trim());
      } catch (ParseException e) {
        // do not fail test just because a date could not be parsed
        System.out.println("ignoring date parse exception (assigning 'now') for: "+props.getProperty("date"));
        date = new Date(); // now 
      }
    }
      
    return new DocData(name, bodyBuf.toString(), title, props, date);
  }

  public DocData parse(String name, Date date, StringBuffer inputText, DateFormat dateFormat) throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return parse(name, date, new StringReader(inputText.toString()), dateFormat);
  }

}
