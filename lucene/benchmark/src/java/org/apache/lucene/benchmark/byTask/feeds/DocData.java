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

import java.util.Date;
import java.util.Properties;
import org.apache.lucene.document.DateTools;

/** Output of parsing (e.g. HTML parsing) of an input document. */
public class DocData {

  private String name;
  private String body;
  private String title;
  private String date;
  private int id;
  private Properties props;

  public void clear() {
    name = null;
    body = null;
    title = null;
    date = null;
    props = null;
    id = -1;
  }

  public String getBody() {
    return body;
  }

  /**
   * @return the date. If the ctor with Date was called, then the String returned is the output of
   *     {@link DateTools#dateToString(Date, org.apache.lucene.document.DateTools.Resolution)} .
   *     Otherwise it's the String passed to the other ctor.
   */
  public String getDate() {
    return date;
  }

  public String getName() {
    return name;
  }

  public int getID() {
    return id;
  }

  public Properties getProps() {
    return props;
  }

  public String getTitle() {
    return title;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public void setDate(Date date) {
    if (date != null) {
      setDate(DateTools.dateToString(date, DateTools.Resolution.SECOND));
    } else {
      this.date = null;
    }
  }

  public void setDate(String date) {
    this.date = date;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setID(int id) {
    this.id = id;
  }

  public void setProps(Properties props) {
    this.props = props;
  }

  public void setTitle(String title) {
    this.title = title;
  }
}
