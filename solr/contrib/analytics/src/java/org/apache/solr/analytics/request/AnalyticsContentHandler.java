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
package org.apache.solr.analytics.request;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.solr.analytics.request.FieldFacetRequest.FacetSortDirection;
import org.apache.solr.analytics.request.FieldFacetRequest.FacetSortSpecification;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.schema.IndexSchema;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 * Handles the parsing of the AnalysisRequestEnvelope elements if passed in through XML.
 */
public class AnalyticsContentHandler implements ContentHandler {
  // XML Element/Attribute Name Constants
  public static final String ANALYTICS_REQUEST_ENVELOPE="analyticsRequestEnvelope";
  
  public static final String ANALYTICS_REQUEST="analyticsRequest";
  public static final String NAME="name";
  
  public static final String STATISTIC="statistic";
  public static final String EXPRESSION="expression";
  
  public static final String FIELD_FACET="fieldFacet";
  public static final String FIELD="field";
  public static final String SHOW_MISSING="showMissing";
  public static final String LIMIT="limit";
  public static final String MIN_COUNT="minCount";
  
  public static final String SORT_SPECIFICATION="sortSpecification";
  public static final String STAT_NAME="statName";
  public static final String DIRECTION="direction";
  
  public static final String RANGE_FACET="rangeFacet";
  public static final String START="start";
  public static final String END="end";
  public static final String GAP="gap";
  public static final String INCLUDE_BOUNDARY="includeBoundary";
  public static final String OTHER_RANGE="otherRange";
  public static final String HARD_END="hardend";
  
  public static final String QUERY_FACET="queryFacet";
  public static final String QUERY="query";
  
  // Default Values
  public static final int DEFAULT_FACET_LIMIT = -1;
  public static final boolean DEFAULT_FACET_HARDEND = false;
  public static final int DEFAULT_FACET_MINCOUNT = 0;
  public static final boolean DEFAULT_FACET_FIELD_SHOW_MISSING = false;

  boolean inEnvelope = false;
  boolean inRequest = false;
  boolean inStatistic = false;
  boolean inFieldFacet = false;
  boolean inSortSpecification = false;
  boolean inQueryFacet = false;
  boolean inRangeFacet = false;
  
  private final IndexSchema schema;
  
  // Objects to use while building the Analytics Requests
  
  String currentElementText;
  
  List<AnalyticsRequest> requests;
  
  AnalyticsRequest analyticsRequest;
  List<ExpressionRequest> expressionList;
  List<FieldFacetRequest> fieldFacetList;
  List<RangeFacetRequest> rangeFacetList;
  List<QueryFacetRequest> queryFacetList;
  
  ExpressionRequest expression;
  
  FieldFacetRequest fieldFacet;
  int limit;
  int minCount;
  boolean showMissing;
  FacetSortSpecification sortSpecification;
  
  RangeFacetRequest rangeFacet;
  boolean hardend;
  List<String> gaps;
  EnumSet<FacetRangeInclude> includeBoundaries;
  EnumSet<FacetRangeOther> otherRanges;
  
  String queryName;
  List<String> queries;
  
  public AnalyticsContentHandler(IndexSchema schema) {
    this.schema = schema;
  }

  @Override
  public void setDocumentLocator(Locator locator) { }

  @Override
  public void startDocument() throws SAXException { }

  @Override
  public void endDocument() throws SAXException { }

  @Override
  public void startPrefixMapping(String prefix, String uri) throws SAXException { }

  @Override
  public void endPrefixMapping(String prefix) throws SAXException { }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
    currentElementText = "";
    if (inEnvelope) {
      if (inRequest) {
        if (localName.equals(STATISTIC)) {
          // Start a Statistic Request
          inStatistic = true;
        } else if (inFieldFacet) {
          if (localName.equals(SORT_SPECIFICATION)) {
            // Start a Sort Specification
            inSortSpecification = true;
            sortSpecification = new FacetSortSpecification();
          }
        } else if (localName.equals(FIELD_FACET)) {
          // Start a Field Facet Request
          // Get attributes (limit, minCount, showMissing)
          String att = atts.getValue(uri,LIMIT);
          if (att!=null) {
            limit = Integer.parseInt(att);
          } else {
            limit = DEFAULT_FACET_LIMIT;
          }
          att = atts.getValue(uri,MIN_COUNT);
          if (att!=null) {
            minCount = Integer.parseInt(att);
          } else {
            minCount = DEFAULT_FACET_MINCOUNT;
          }
          att = atts.getValue(uri,SHOW_MISSING);
          if (att!=null) {
            showMissing = Boolean.parseBoolean(att);
          } else {
            showMissing = DEFAULT_FACET_FIELD_SHOW_MISSING;
          }
          
          inFieldFacet = true;
        } else if (localName.equals(RANGE_FACET)) {
          // Start a Range Facet Request
          // Get attributes (hardEnd)
          String att = atts.getValue(uri,HARD_END);
          if (att!=null) {
            hardend = Boolean.parseBoolean(att);
          } else {
            hardend = false;
          }
          
          // Initiate Range Facet classes
          gaps = new ArrayList<>();
          includeBoundaries = EnumSet.noneOf(FacetRangeInclude.class);
          otherRanges = EnumSet.noneOf(FacetRangeOther.class);
          inRangeFacet = true;
        } else if (localName.equals(QUERY_FACET)) {
          // Start a Query Facet Request
          queries = new ArrayList<>();
          inQueryFacet = true;
        }
      } else if (localName.equals(ANALYTICS_REQUEST)){
        // Start an Analytics Request
        
        // Renew each list.
        fieldFacetList = new ArrayList<>();
        rangeFacetList = new ArrayList<>();
        queryFacetList = new ArrayList<>();
        expressionList = new ArrayList<>();
        inRequest = true;
      }
    } else if (localName.equals(ANALYTICS_REQUEST_ENVELOPE)){
      //Begin the parsing of the Analytics Requests
      requests = new ArrayList<>();
      inEnvelope = true;
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (inEnvelope) {
      if (inRequest) {
        if (inStatistic) {
          if (localName.equals(EXPRESSION)) {
            expression = new ExpressionRequest(currentElementText,currentElementText);
          } else if (localName.equals(NAME)) {
            expression.setName(currentElementText);
          } else if (localName.equals(STATISTIC)) {
            // Finished Parsing the Statistic Request
            expressionList.add(expression);
            inStatistic = false;
          } 
        } else if (inFieldFacet) {
          if (inSortSpecification) {
            if (localName.equals(STAT_NAME)) {
              sortSpecification.setStatistic(currentElementText);
            } else if (localName.equals(DIRECTION)) {
              sortSpecification.setDirection(FacetSortDirection.fromExternal(currentElementText));
            } else if (localName.equals(SORT_SPECIFICATION)) {
              // Finished Parsing the Sort Specification
              fieldFacet.setSort(sortSpecification);
              inSortSpecification = false;
            } 
          } else if (localName.equals(FIELD)) {
            fieldFacet = new FieldFacetRequest(schema.getField(currentElementText));
          } else if (localName.equals(FIELD_FACET)) {
            // Finished Parsing the Field Facet Request
            fieldFacet.setLimit(limit);
            fieldFacet.showMissing(showMissing);
            fieldFacetList.add(fieldFacet);
            inFieldFacet = false;
          } 
        } else if (inRangeFacet) {
          if (localName.equals(FIELD)) {
            rangeFacet = new RangeFacetRequest(schema.getField(currentElementText), "", "", new String[1]);
          } else if (localName.equals(START)) {
            rangeFacet.setStart(currentElementText);
          } else if (localName.equals(END)) {
            rangeFacet.setEnd(currentElementText);
          } else if (localName.equals(GAP)) {
            gaps.add(currentElementText);
          } else if (localName.equals(INCLUDE_BOUNDARY)) {
            includeBoundaries.add(FacetRangeInclude.get(currentElementText));
          } else if (localName.equals(OTHER_RANGE)) {
            otherRanges.add(FacetRangeOther.get(currentElementText));
          } else if (localName.equals(RANGE_FACET)) {
            // Finished Parsing the Range Facet Request
            rangeFacet.setHardEnd(hardend);
            rangeFacet.setGaps(gaps.toArray(new String[1]));
            rangeFacet.setInclude(includeBoundaries);
            rangeFacet.setOthers(otherRanges);
            inRangeFacet = false;
            rangeFacetList.add(rangeFacet);
          } 
        } else if (inQueryFacet) {
          if (localName.equals(NAME)) {
            queryName = currentElementText;
          } else if (localName.equals(QUERY)) {
            queries.add(currentElementText);
          } else if (localName.equals(QUERY_FACET)) {
            // Finished Parsing the Query Facet Request
            QueryFacetRequest temp = new QueryFacetRequest(queryName);
            temp.setQueries(queries);
            queryFacetList.add(temp);
            inQueryFacet = false;
          }
        } else if (localName.equals(NAME)) {
          analyticsRequest = new AnalyticsRequest(currentElementText);
        } else if (localName.equals(ANALYTICS_REQUEST)){
          // Finished Parsing the Analytics Request
          analyticsRequest.setExpressions(expressionList);
          analyticsRequest.setFieldFacets(fieldFacetList);
          analyticsRequest.setRangeFacets(rangeFacetList);
          analyticsRequest.setQueryFacets(queryFacetList);
          requests.add(analyticsRequest);
          inRequest = false;
        }
      } else if (localName.equals(ANALYTICS_REQUEST_ENVELOPE)){
        // Finished Parsing
        inEnvelope = false;
      }
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    currentElementText += new String(ch,start,length);
  }

  @Override
  public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException { }

  @Override
  public void processingInstruction(String target, String data) throws SAXException { }

  @Override
  public void skippedEntity(String name) throws SAXException { }
  
  /**
   * Returns the list of Analytics Requests built during parsing.
   * 
   * @return List of {@link AnalyticsRequest} objects specified by the given XML file
   */
  public List<AnalyticsRequest> getAnalyticsRequests() {
    return requests;
  }

}
