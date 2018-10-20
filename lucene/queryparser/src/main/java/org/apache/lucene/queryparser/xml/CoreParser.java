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
package org.apache.lucene.queryparser.xml;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.xml.builders.*;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.InputStream;
import java.util.Locale;

/**
 * Assembles a QueryBuilder which uses only core Lucene Query objects
 */
public class CoreParser implements QueryBuilder, SpanQueryBuilder {

  protected String defaultField;
  protected Analyzer analyzer;
  protected QueryParser parser;
  protected QueryBuilderFactory queryFactory;
  final protected SpanQueryBuilderFactory spanFactory;


  /**
   * Construct an XML parser that uses a single instance QueryParser for handling
   * UserQuery tags - all parse operations are synchronised on this parser
   *
   * @param parser A QueryParser which will be synchronized on during parse calls.
   */
  public CoreParser(Analyzer analyzer, QueryParser parser) {
    this(null, analyzer, parser);
  }

  /**
   * Constructs an XML parser that creates a QueryParser for each UserQuery request.
   *
   * @param defaultField The default field name used by QueryParsers constructed for UserQuery tags
   */
  public CoreParser(String defaultField, Analyzer analyzer) {
    this(defaultField, analyzer, null);
  }

  protected CoreParser(String defaultField, Analyzer analyzer, QueryParser parser) {
    this.defaultField = defaultField;
    this.analyzer = analyzer;
    this.parser = parser;

    queryFactory = new QueryBuilderFactory();
    spanFactory = new SpanQueryBuilderFactory();

    queryFactory.addBuilder("TermQuery", new TermQueryBuilder());
    queryFactory.addBuilder("TermsQuery", new TermsQueryBuilder(analyzer));
    queryFactory.addBuilder("MatchAllDocsQuery", new MatchAllDocsQueryBuilder());
    queryFactory.addBuilder("BooleanQuery", new BooleanQueryBuilder(queryFactory));
    queryFactory.addBuilder("PointRangeQuery", new PointRangeQueryBuilder());
    queryFactory.addBuilder("RangeQuery", new RangeQueryBuilder());
    queryFactory.addBuilder("DisjunctionMaxQuery", new DisjunctionMaxQueryBuilder(queryFactory));
    if (parser != null) {
      queryFactory.addBuilder("UserQuery", new UserInputQueryBuilder(parser));
    } else {
      queryFactory.addBuilder("UserQuery", new UserInputQueryBuilder(defaultField, analyzer));
    }
    queryFactory.addBuilder("ConstantScoreQuery", new ConstantScoreQueryBuilder(queryFactory));

    SpanNearBuilder snb = new SpanNearBuilder(spanFactory);
    spanFactory.addBuilder("SpanNear", snb);
    queryFactory.addBuilder("SpanNear", snb);

    BoostingTermBuilder btb = new BoostingTermBuilder();
    spanFactory.addBuilder("BoostingTermQuery", btb);
    queryFactory.addBuilder("BoostingTermQuery", btb);

    SpanTermBuilder snt = new SpanTermBuilder();
    spanFactory.addBuilder("SpanTerm", snt);
    queryFactory.addBuilder("SpanTerm", snt);

    SpanOrBuilder sot = new SpanOrBuilder(spanFactory);
    spanFactory.addBuilder("SpanOr", sot);
    queryFactory.addBuilder("SpanOr", sot);

    SpanOrTermsBuilder sots = new SpanOrTermsBuilder(analyzer);
    spanFactory.addBuilder("SpanOrTerms", sots);
    queryFactory.addBuilder("SpanOrTerms", sots);

    SpanFirstBuilder sft = new SpanFirstBuilder(spanFactory);
    spanFactory.addBuilder("SpanFirst", sft);
    queryFactory.addBuilder("SpanFirst", sft);

    SpanNotBuilder snot = new SpanNotBuilder(spanFactory);
    spanFactory.addBuilder("SpanNot", snot);
    queryFactory.addBuilder("SpanNot", snot);
  }

  /**
   * Parses the given stream as XML file and returns a {@link Query}.
   * By default this disallows external entities for security reasons.
   */
  public Query parse(InputStream xmlStream) throws ParserException {
    return getQuery(parseXML(xmlStream).getDocumentElement());
  }

  // for test use
  SpanQuery parseAsSpanQuery(InputStream xmlStream) throws ParserException {
    return getSpanQuery(parseXML(xmlStream).getDocumentElement());
  }

  public void addQueryBuilder(String nodeName, QueryBuilder builder) {
    queryFactory.addBuilder(nodeName, builder);
  }

  public void addSpanBuilder(String nodeName, SpanQueryBuilder builder) {
    spanFactory.addBuilder(nodeName, builder);
  }

  public void addSpanQueryBuilder(String nodeName, SpanQueryBuilder builder) {
    queryFactory.addBuilder(nodeName, builder);
    spanFactory.addBuilder(nodeName, builder);
  }

  /**
   * Returns a SAX {@link EntityResolver} to be used by {@link DocumentBuilder}.
   * By default this returns {@link #DISALLOW_EXTERNAL_ENTITY_RESOLVER}, which disallows the
   * expansion of external entities (for security reasons). To restore legacy behavior,
   * override this method to return {@code null}.
   */
  protected EntityResolver getEntityResolver() {
    return DISALLOW_EXTERNAL_ENTITY_RESOLVER;
  }

  /**
   * Subclass and override to return a SAX {@link ErrorHandler} to be used by {@link DocumentBuilder}.
   * By default this returns {@code null} so no error handler is used.
   * This method can be used to redirect XML parse errors/warnings to a custom logger.
   */
  protected ErrorHandler getErrorHandler() {
    return null;
  }

  private Document parseXML(InputStream pXmlFile) throws ParserException {
    final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setValidating(false);
    try {
      dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    } catch (ParserConfigurationException e) {
      // ignore since all implementations are required to support the
      // {@link javax.xml.XMLConstants#FEATURE_SECURE_PROCESSING} feature
    }
    final DocumentBuilder db;
    try {
      db = dbf.newDocumentBuilder();
    } catch (Exception se) {
      throw new ParserException("XML Parser configuration error.", se);
    }
    try {
      db.setEntityResolver(getEntityResolver());
      db.setErrorHandler(getErrorHandler());
      return db.parse(pXmlFile);
    } catch (Exception se) {
      throw new ParserException("Error parsing XML stream: " + se, se);
    }
  }

  public Query getQuery(Element e) throws ParserException {
    return queryFactory.getQuery(e);
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    return spanFactory.getSpanQuery(e);
  }

  public static final EntityResolver DISALLOW_EXTERNAL_ENTITY_RESOLVER = (String publicId, String systemId) -> {
    throw new SAXException(String.format(Locale.ENGLISH,
        "External Entity resolving unsupported:  publicId=\"%s\" systemId=\"%s\"",
        publicId, systemId));
  };

}
