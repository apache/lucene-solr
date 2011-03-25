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
package org.apache.solr.response;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocIdAugmenter;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.DocTransformers;
import org.apache.solr.response.transform.ExplainAugmenter;
import org.apache.solr.response.transform.ScoreAugmenter;
import org.apache.solr.response.transform.ValueAugmenter;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class representing the return fields
 * 
 * @version $Id: JSONResponseWriter.java 1065304 2011-01-30 15:10:15Z rmuir $
 * @since solr 4.0
 */
public class ReturnFields
{
  static final Logger log = LoggerFactory.getLogger( ReturnFields.class );
  
  public static final String SCORE = "score";
  public static final String DOCID = "_docid_";
  public static final String SHARD = "_shard_";
  public static final String EXPLAIN = "_explain_";

  private Set<String> fields; // includes 'augment' names or null
  private DocTransformer transformer;
  private boolean wantsScore = false;


  public static ReturnFields getReturnFields(SolrQueryRequest req)
  {
    return getReturnFields( req.getParams().get(CommonParams.FL), req );
  }

  public static ReturnFields getReturnFields(String fl, SolrQueryRequest req)
  {
    ReturnFields rf = new ReturnFields();
    rf.wantsScore = false;
    rf.fields = new LinkedHashSet<String>(); // order is important for CSVResponseWriter
    boolean allFields = false;

    DocTransformers augmenters = new DocTransformers();
    if (fl != null) {
      // TODO - this could become more efficient if widely used.
      String[] flst = SolrPluginUtils.split(fl);
      if (flst.length > 0 && !(flst.length==1 && flst[0].length()==0)) {
        IndexSchema schema = req.getSchema();
        for (String name : flst) {
          if( "*".equals( name ) ) {
            allFields = true;
          }
          else if( SCORE.equals( name ) ) {
            rf.fields.add( name );
            rf.wantsScore = true;
            augmenters.addTransformer( new ScoreAugmenter( SCORE ) );
          }
          else {
            rf.fields.add( name );

            // Check if it is a real score
            SchemaField sf = schema.getFieldOrNull( name );
            if( sf == null ) {
              // not a field name, but possibly return value
              if( DOCID.equals( name ) ) {
                augmenters.addTransformer( new DocIdAugmenter( DOCID ) );
              }
              else if( SHARD.equals( name ) ) {
                String id = "getshardid???";
                augmenters.addTransformer( new ValueAugmenter( SHARD, id ) );
              }
              else if( EXPLAIN.equals( name ) ) {
                augmenters.addTransformer( new ExplainAugmenter( EXPLAIN ) );
              }
              else if( name.startsWith( "{!func}") ) {
                // help?  not sure how to parse a ValueSorce
                // -- not to mention, we probably want to reuse existing ones!
                augmenters.addTransformer( new ValueAugmenter( name, "TODO:"+name ) );
//                try {
//                  String func = name.substring( "{!func}".length() );
//                  SolrParams local = null;
//                  FunctionQParser p = new FunctionQParser( func, local, req.getParams(), req );
//                  Query q = p.parse();
//                  ValueSource vs = p.parseValueSource();
//                  AtomicReaderContext ctx = new AtomicReaderContext( req.getSearcher().getIndexReader() );
//                  Map mmm = null; // ?????
//                  DocValues values = p.parseValueSource().getValues( mmm, ctx );
//                  augmenters.addAugmenter( new DocValuesAugmenter( name, values ) );
//                }
//                catch( Exception ex ) {
//                  throw new SolrException( org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST,
//                      "Unable to parse augmented field: "+name, ex );
//                }
              }
              else { 
                // maybe throw an exception?
//                throw new SolrException( org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST,
//                    "Unknown Return Field: "+name );
              }
            }
          }
        }
      }
    }
    
    // Legacy behavior? "score" == "*,score"
    if( rf.fields.size() == 1 && rf.wantsScore ) {
      allFields = true;
    }
    
    if( allFields || rf.fields.isEmpty() ) {
      rf.fields = null;
    }
    
    if( augmenters.size() == 1 ) {
      rf.transformer = augmenters.getTransformer(0);
    }
    else if( augmenters.size() > 1 ) {
      rf.transformer = augmenters;
    }
    return rf;
  }

  public Set<String> getFieldNames()
  {
    return fields;
  }

  public boolean getWantsScore()
  {
    return wantsScore;
  }

  public boolean contains( String name )
  {
    return fields==null || fields.contains( name );
  }

  public DocTransformer getTransformer()
  {
    return transformer;
  }
}
