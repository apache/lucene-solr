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
package org.apache.solr.response.transform;

import java.io.IOException;

import org.apache.lucene.search.Explanation;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add query explain info directly to Document
 *
 * @version $Id: JSONResponseWriter.java 1065304 2011-01-30 15:10:15Z rmuir $
 * @since solr 4.0
 */
public class ExplainAugmenter extends TransformerWithContext
{
  public static enum Style {
    NL,
    TEXT,
    HTML
  };
  
  final String name;
  final Style style;
  
  public ExplainAugmenter( String display )
  {
    this( display, Style.TEXT );
  }

  public ExplainAugmenter( String display, Style style )
  {
    this.name = display;
    this.style = style;
  }

  @Override
  public void transform(SolrDocument doc, int docid) {
    if( context != null && context.query != null ) {
      try {
        Explanation exp = context.searcher.explain(context.query, docid);
        if( style == Style.NL ) {
          doc.setField( name, SolrPluginUtils.explanationToNamedList(exp) );
        }
        else if( style == Style.HTML ) {
          doc.setField( name, exp.toHtml() );
        }
        else {
          doc.setField( name, exp.toString() );
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
