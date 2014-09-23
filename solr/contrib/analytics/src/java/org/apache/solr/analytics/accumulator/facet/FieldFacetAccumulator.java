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

package org.apache.solr.analytics.accumulator.facet;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.analytics.accumulator.FacetingAccumulator;
import org.apache.solr.analytics.accumulator.ValueAccumulator;
import org.apache.solr.analytics.util.AnalyticsParsers;
import org.apache.solr.analytics.util.AnalyticsParsers.NumericParser;
import org.apache.solr.analytics.util.AnalyticsParsers.Parser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * An Accumulator that manages the faceting for fieldFacets.
 * Collects the field facet values.
 */
public class FieldFacetAccumulator extends ValueAccumulator {
  protected final Parser parser;
  protected final FacetValueAccumulator parent;
  protected final String name;
  protected final SolrIndexSearcher searcher;
  protected final SchemaField schemaField;
  protected final boolean multiValued;
  protected final boolean numField;
  protected final boolean dateField;
  protected SortedSetDocValues setValues;
  protected SortedDocValues sortValues; 
  protected NumericDocValues numValues; 
  protected Bits numValuesBits; 
  
  public FieldFacetAccumulator(SolrIndexSearcher searcher, FacetValueAccumulator parent, SchemaField schemaField) throws IOException {  
    if( !schemaField.hasDocValues() ){
      throw new SolrException(ErrorCode.BAD_REQUEST, "Field '"+schemaField.getName()+"' does not have docValues");
    }
    this.searcher = searcher;
    this.schemaField = schemaField;
    this.name = schemaField.getName();
    if (!schemaField.hasDocValues()) {
      throw new IOException(name+" does not have docValues and therefore cannot be faceted over.");
    }
    this.multiValued = schemaField.multiValued();
    this.numField = schemaField.getType().getNumericType()!=null;
    this.dateField = schemaField.getType().getClass().equals(TrieDateField.class);
    this.parent = parent;  
    this.parser = AnalyticsParsers.getParser(schemaField.getType().getClass());
  }

  public static FieldFacetAccumulator create(SolrIndexSearcher searcher, FacetValueAccumulator parent, SchemaField facetField) throws IOException{
    return new FieldFacetAccumulator(searcher,parent,facetField);
  }

  /**
   * Move to the next set of documents to add to the field facet.
   */
  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    if (multiValued) {
      setValues = context.reader().getSortedSetDocValues(name);
    } else {
      if (numField) {
        numValues = context.reader().getNumericDocValues(name);
        numValuesBits = context.reader().getDocsWithField(name);
      } else {
        sortValues = context.reader().getSortedDocValues(name);
      }
    }
  }

  /**
   * Tell the FacetingAccumulator to collect the doc with the 
   * given fieldFacet and value(s).
   */
  @Override
  public void collect(int doc) throws IOException {
    if (multiValued) {
      boolean exists = false;
      if (setValues!=null) {
        setValues.setDocument(doc);
        int term;
        while ((term = (int)setValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          exists = true;
          final BytesRef value = setValues.lookupOrd(term);
          parent.collectField(doc, name, parser.parse(value) );
        }
      }
      if (!exists) {
        parent.collectField(doc, name, FacetingAccumulator.MISSING_VALUE );
      }
    } else {
      if(numField){
        if(numValues != null) {
          long v = numValues.get(doc);
          if( v != 0 || numValuesBits.get(doc) ){
            parent.collectField(doc, name, ((NumericParser)parser).parseNum(v));
          } else {
            parent.collectField(doc, name, FacetingAccumulator.MISSING_VALUE );
          }
        } else {
          parent.collectField(doc, name, FacetingAccumulator.MISSING_VALUE );
        }
      } else {
        if(sortValues != null) {
          final int ord = sortValues.getOrd(doc);
          if (ord < 0) {
            parent.collectField(doc, name, FacetingAccumulator.MISSING_VALUE );
          } else {
            parent.collectField(doc, name, parser.parse(sortValues.lookupOrd(ord)) );
          }
        } else {
          parent.collectField(doc, name, FacetingAccumulator.MISSING_VALUE );
        }
      }
    }
  }

  @Override
  public void compute() {}
 
  @Override
  public NamedList<?> export() { return null; }

}
