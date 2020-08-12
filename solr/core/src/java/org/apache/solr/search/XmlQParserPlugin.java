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
package org.apache.solr.search;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.Query;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;

/**
 * The {@link XmlQParserPlugin} extends the {@link QParserPlugin} and supports the creation of queries from XML.
 *<p>
 * Example:
 * <pre>
&lt;BooleanQuery fieldName="description"&gt;
   &lt;Clause occurs="must"&gt;
      &lt;TermQuery&gt;shirt&lt;/TermQuery&gt;
   &lt;/Clause&gt;
   &lt;Clause occurs="mustnot"&gt;
      &lt;TermQuery&gt;plain&lt;/TermQuery&gt;
   &lt;/Clause&gt;
   &lt;Clause occurs="should"&gt;
      &lt;TermQuery&gt;cotton&lt;/TermQuery&gt;
   &lt;/Clause&gt;
   &lt;Clause occurs="must"&gt;
      &lt;BooleanQuery fieldName="size"&gt;
         &lt;Clause occurs="should"&gt;
            &lt;TermsQuery&gt;S M L&lt;/TermsQuery&gt;
         &lt;/Clause&gt;
      &lt;/BooleanQuery&gt;
   &lt;/Clause&gt;
&lt;/BooleanQuery&gt;
</pre>
 * You can configure your own custom query builders for additional XML elements.
 * The custom builders need to extend the {@link SolrQueryBuilder} or the
 * {@link SolrSpanQueryBuilder} class.
 *<p>
 * Example solrconfig.xml snippet:
 * <pre>&lt;queryParser name="xmlparser" class="XmlQParserPlugin"&gt;
  &lt;str name="MyCustomQuery"&gt;com.mycompany.solr.search.MyCustomQueryBuilder&lt;/str&gt;
&lt;/queryParser&gt;
</pre>
 */
public class XmlQParserPlugin extends QParserPlugin {
  public static final String NAME = "xmlparser";

  @SuppressWarnings({"rawtypes"})
  private NamedList args;

  @Override
  public void init( @SuppressWarnings({"rawtypes"})NamedList args ) {
    super.init(args);
    this.args = args;
  }

  private class XmlQParser extends QParser {

    public XmlQParser(String qstr, SolrParams localParams,
        SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    public Query parse() throws SyntaxError {
      final String qstr = getString();
      if (qstr == null || qstr.isEmpty()) {
        return null;
      }
      final IndexSchema schema = req.getSchema();
      final String defaultField = getParam(CommonParams.DF);
      final Analyzer analyzer = schema.getQueryAnalyzer();

      final SolrCoreParser solrParser = new SolrCoreParser(defaultField, analyzer, req);
      solrParser.init(args);
      try {
        return solrParser.parse(new ByteArrayInputStream(qstr.getBytes(StandardCharsets.UTF_8)));
      } catch (ParserException e) {
        throw new SyntaxError(e.getMessage() + " in " + req.toString());
      }
    }

  }

  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    return new XmlQParser(qstr, localParams, params, req);
  }

}
