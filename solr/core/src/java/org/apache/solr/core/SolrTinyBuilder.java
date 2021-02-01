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
package org.apache.solr.core;

import net.sf.saxon.event.PipelineConfiguration;
import net.sf.saxon.om.AttributeInfo;
import net.sf.saxon.tree.tiny.TinyBuilder;
import org.apache.solr.util.PropertiesUtil;

import java.util.HashMap;
import java.util.Properties;

public class SolrTinyBuilder extends TinyBuilder  {
  private final Properties substituteProps;
  private final HashMap sysProperties;

  /**
   * Create a TinyTree builder
   *
   * @param pipe information about the pipeline leading up to this Builder
   * @param substituteProps
   */
  public SolrTinyBuilder(PipelineConfiguration pipe, Properties substituteProps) {
    super(pipe);
    this.substituteProps = substituteProps;
    this.sysProperties = new HashMap(System.getProperties());
  }

  protected int makeTextNode(CharSequence chars, int len) {
    String sub = PropertiesUtil
        .substituteProperty(chars.subSequence(0, len).toString(),
            substituteProps, sysProperties);

    return super.makeTextNode(sub, sub.length());
  }

  protected String getAttValue(AttributeInfo att) {
    String sub = PropertiesUtil
        .substituteProperty(att.getValue(),
            substituteProps, sysProperties);
    return sub;
  }

}
