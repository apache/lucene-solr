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
package org.apache.solr.response.transform;

import java.io.IOException;

import org.apache.lucene.search.Explanation;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.SolrPluginUtils;

/**
 *
 * @since solr 4.0
 */
public class ExplainAugmenterFactory extends TransformerFactory
{
  public enum Style {
    nl,
    text,
    html
  }

  protected Style defaultStyle = Style.text;

  @Override
  public void init(NamedList args) {
    super.init(args);
    if( defaultUserArgs != null ) {
      defaultStyle = getStyle( defaultUserArgs );
    }
  }

  public static Style getStyle( String str ) {
    try {
      return Style.valueOf( str );
    }
    catch( Exception ex ) {
      throw new SolrException( ErrorCode.BAD_REQUEST,
          "Unknown Explain Style: "+str );
    }
  }

  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    String s = params.get("style");
    Style style = (s == null) ? defaultStyle : getStyle(s);
    return new ExplainAugmenter( field, style );
  }

  /** Render an explanation as HTML. */
  public static String toHtml(Explanation explanation) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<ul>\n");

    buffer.append("<li>");
    buffer.append(explanation.getValue()).append(" = ").append(explanation.getDescription());
    buffer.append("<br />\n");

    Explanation[] details = explanation.getDetails();
    for (int i = 0 ; i < details.length; i++) {
      buffer.append(toHtml(details[i]));
    }

    buffer.append("</li>\n");
    buffer.append("</ul>\n");

    return buffer.toString();
  }

  static class ExplainAugmenter extends DocTransformer {
    final String name;
    final Style style;

    public ExplainAugmenter( String display, Style style )
    {
      this.name = display;
      this.style = style;
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public void transform(SolrDocument doc, int docid) throws IOException {
      if( context != null && context.getQuery() != null ) {
        Explanation exp = context.getSearcher().explain(context.getQuery(), docid);
        if( style == Style.nl ) {
          doc.setField( name, SolrPluginUtils.explanationToNamedList(exp) );
        }
        else if( style == Style.html ) {
          doc.setField( name, toHtml(exp));
        }
        else {
          doc.setField( name, exp.toString() );
        }
      }
    }
  }
}
