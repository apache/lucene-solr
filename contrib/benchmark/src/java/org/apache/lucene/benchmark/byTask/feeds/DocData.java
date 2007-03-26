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

import java.util.Date;
import java.util.Properties;

/**
 * Output of parsing (e.g. HTML parsing) of an input document.
 */

public class DocData {
  
  private String name;
  private String body;
  private String title;
  private Date date;
  private Properties props;
  
  public DocData(String name, String body, String title, Properties props, Date date) {
    this.name = name;
    this.body = body;
    this.title = title;
    this.date = date;
    this.props = props;
  }

  /**
   * @return Returns the name.
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return Returns the props.
   */
  public Properties getProps() {
    return props;
  }

  /**
   * @param props The props to set.
   */
  public void setProps(Properties props) {
    this.props = props;
  }

  /**
   * @return Returns the body.
   */
  public String getBody() {
    return body;
  }

  /**
   * @param body The body to set.
   */
  public void setBody(String body) {
    this.body = body;
  }

  /**
   * @return Returns the title.
   */
  public String getTitle() {
    return title;
  }

  /**
   * @param title The title to set.
   */
  public void setTitle(String title) {
    this.title = title;
  }

  /**
   * @return Returns the date.
   */
  public Date getDate() {
    return date;
  }

  /**
   * @param date The date to set.
   */
  public void setDate(Date date) {
    this.date = date;
  }

}
