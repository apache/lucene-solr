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
package org.apache.solr.highlight;

import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;

/**
 * Use the {@link org.apache.lucene.search.highlight.SimpleHTMLFormatter}
 */
public class HtmlFormatter extends HighlightingPluginBase implements SolrFormatter 
{
  @Override
  public Formatter getFormatter(String fieldName, SolrParams params ) 
  {
    numRequests.inc();
    params = SolrParams.wrapDefaults(params, defaults);

    return new SimpleHTMLFormatter(
        params.getFieldParam(fieldName, HighlightParams.SIMPLE_PRE,  "<em>" ), 
        params.getFieldParam(fieldName, HighlightParams.SIMPLE_POST, "</em>"));
  }

  ///////////////////////////////////////////////////////////////////////
  //////////////////////// SolrInfoMBeans methods ///////////////////////
  ///////////////////////////////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "HtmlFormatter";
  }
}
