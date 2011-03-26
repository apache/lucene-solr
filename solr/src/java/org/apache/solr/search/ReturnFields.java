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
package org.apache.solr.search;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocIdAugmenter;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.DocTransformers;
import org.apache.solr.response.transform.ExplainAugmenter;
import org.apache.solr.response.transform.RenameFieldsTransformer;
import org.apache.solr.response.transform.ScoreAugmenter;
import org.apache.solr.response.transform.ValueAugmenter;
import org.apache.solr.response.transform.ValueSourceAugmenter;
import org.apache.solr.search.function.FunctionQuery;
import org.apache.solr.search.function.QueryValueSource;
import org.apache.solr.search.function.ValueSource;
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
  
  // Special Field Keys
  public static final String SCORE = "score";
  // some special syntax for these guys?
  // Should these have factories... via plugin utils...
  public static final String DOCID = "_docid_";
  public static final String SHARD = "_shard_";
  public static final String EXPLAIN = "_explain_";

  private final List<String> globs = new ArrayList<String>(1);
  private final Set<String> fields = new LinkedHashSet<String>(); // order is important for CSVResponseWriter
  private Set<String> okFieldNames = new HashSet<String>(); // Collection of everything that could match
  
  private DocTransformer transformer;
  private boolean _wantsScore = false;
  private boolean _wantsAllFields = false;

  public ReturnFields() {
    _wantsAllFields = true;
  }

  public ReturnFields(SolrQueryRequest req) {
    this( req.getParams().get(CommonParams.FL), req );
  }

  public ReturnFields(String fl, SolrQueryRequest req) {
//    this( (fl==null)?null:SolrPluginUtils.split(fl), req );
    if( fl == null ) {
      parseFieldList((String[])null, req);
    }
    else {
      if( fl.trim().length() == 0 ) {
        // legacy thing to support fl='  ' => fl=*,score!
        // maybe time to drop support for this?
        // See ConvertedLegacyTest
        _wantsScore = true;
        _wantsAllFields = true;
        transformer = new ScoreAugmenter(SCORE);
      }
      else {
        parseFieldList( new String[]{fl}, req);
      }
    }
    req.getCore().log.info("fields=" + fields + "\t globs="+globs + "\t transformer="+transformer);  
  }

  public ReturnFields(String[] fl, SolrQueryRequest req) {
    parseFieldList(fl, req);
    req.getCore().log.info("fields=" + fields + "\t globs="+globs + "\t transformer="+transformer);  
  }

  private void parseFieldList(String[] fl, SolrQueryRequest req) {
    _wantsScore = false;
    _wantsAllFields = false;
    if (fl == null || fl.length == 0 || fl.length == 1 && fl[0].length()==0) {
      _wantsAllFields = true;
      return;
    }

    NamedList<String> rename = new NamedList<String>();
    DocTransformers augmenters = new DocTransformers();
    for (String fieldList : fl) {
      add(fieldList,rename,augmenters,req);
    }
    if( rename.size() > 0 ) {
      for( int i=0; i<rename.size(); i++ ) {
        okFieldNames.add( rename.getVal(i) );
      }
      augmenters.addTransformer( new RenameFieldsTransformer( rename ) );
    }
    
    // Legacy behavior? "score" == "*,score"  Distributed tests for this
    if( fields.size() == 1 && _wantsScore ) {
      _wantsAllFields = true;
    }
    
    if( !_wantsAllFields ) {
      if( !globs.isEmpty() ) {
        // TODO??? need to fill up the fields with matching field names in the index
        // and add them to okFieldNames?
        // maybe just get all fields?
        // this would disable field selection optimization... i think thatis OK
        fields.clear(); // this will get all fields, and use wantsField to limit
      }
      okFieldNames.addAll( fields );
    }
        
    if( augmenters.size() == 1 ) {
      transformer = augmenters.getTransformer(0);
    }
    else if( augmenters.size() > 1 ) {
      transformer = augmenters;
    }
  }

  private void add(String fl, NamedList<String> rename, DocTransformers augmenters, SolrQueryRequest req) {
    if( fl == null ) {
      return;
    }
    try {
      QueryParsing.StrParser sp = new QueryParsing.StrParser(fl);

      for(;;) {
        sp.opt(',');
        sp.eatws();
        if (sp.pos >= sp.end) break;

        int start = sp.pos;

        // short circuit test for a really simple field name
        String key = null;
        String field = sp.getId(null);
        char ch = sp.ch();

        if (field != null) {
          if (sp.opt('=')) {
            // this was a key, not a field name
            key = field;
            field = null;
            sp.eatws();
            start = sp.pos;
          } else {
            if (ch==' ' || ch == ',' || ch==0) {
              String disp = (key==null) ? field : key; 
              fields.add( field ); // need to put in the map to maintain order for things like CSVResponseWriter
              okFieldNames.add( field );
              okFieldNames.add( key );
              // a valid field name
              if(SCORE.equals(field)) {
                _wantsScore = true;
                augmenters.addTransformer( new ScoreAugmenter( disp ) );
              }
              else if( DOCID.equals( field ) ) {
                augmenters.addTransformer( new DocIdAugmenter( disp ) );
              }
              else if( SHARD.equals( field ) ) {
                String id = "TODO! getshardid???";
                augmenters.addTransformer( new ValueAugmenter( disp, id ) );
              }
              else if( EXPLAIN.equals( field ) ) {
                // TODO? pass params to transformers?
                augmenters.addTransformer( new ExplainAugmenter( disp, ExplainAugmenter.Style.NL ) );
              }
              continue;
            }
            // an invalid field name... reset the position pointer to retry
            sp.pos = start;
            field = null;
          }
        }

        if (key != null) {
          // we read "key = "
          field = sp.getId(null);
          ch = sp.ch();
          if (field != null && (ch==' ' || ch == ',' || ch==0)) {
            rename.add(field, key);
            okFieldNames.add( field );
            okFieldNames.add( key );
            continue;
          }
          // an invalid field name... reset the position pointer to retry
          sp.pos = start;
          field = null;
        }

        if (field == null) {
          // We didn't find a simple name, so let's see if it's a globbed field name.
          // Globbing only works with recommended field names.

          field = sp.getGlobbedId(null);
          ch = sp.ch();
          if (field != null && (ch==' ' || ch == ',' || ch==0)) {
            // "*" looks and acts like a glob, but we give it special treatment
            if ("*".equals(field)) {
              _wantsAllFields = true;
            } else {
              globs.add(field);
            }
            continue;
          }

          // an invalid glob
          sp.pos = start;
        }

        // let's try it as a function instead
        String funcStr = sp.val.substring(start);

        QParser parser = QParser.getParser(funcStr, FunctionQParserPlugin.NAME, req);
        Query q = null;
        ValueSource vs = null;

        try {
          if (parser instanceof FunctionQParser) {
            FunctionQParser fparser = (FunctionQParser)parser;
            fparser.setParseMultipleSources(false);
            fparser.setParseToEnd(false);

            q = fparser.getQuery();

            if (fparser.localParams != null) {
              if (fparser.valFollowedParams) {
                // need to find the end of the function query via the string parser
                int leftOver = fparser.sp.end - fparser.sp.pos;
                sp.pos = sp.end - leftOver;   // reset our parser to the same amount of leftover
              } else {
                // the value was via the "v" param in localParams, so we need to find
                // the end of the local params themselves to pick up where we left off
                sp.pos = start + fparser.localParamsEnd;
              }
            } else {
              // need to find the end of the function query via the string parser
              int leftOver = fparser.sp.end - fparser.sp.pos;
              sp.pos = sp.end - leftOver;   // reset our parser to the same amount of leftover
            }
          } else {
            // A QParser that's not for function queries.
            // It must have been specified via local params.
            q = parser.getQuery();

            assert parser.getLocalParams() != null;
            sp.pos = start + parser.localParamsEnd;
          }


          if (q instanceof FunctionQuery) {
            vs = ((FunctionQuery)q).getValueSource();
          } else {
            vs = new QueryValueSource(q, 0.0f);
          }

          if (key==null) {
            SolrParams localParams = parser.getLocalParams();
            if (localParams != null) {
              key = localParams.get("key");
            }
            if (key == null) {
              // use the function name itself as the field name
              key = sp.val.substring(start, sp.pos);
            }
          }
          
          augmenters.addTransformer( new ValueSourceAugmenter( key, parser, vs ) );
        }
        catch (ParseException e) {
          // try again, simple rules for a field name with no whitespace
          sp.pos = start;
          field = sp.getSimpleString();

          if (req.getSchema().getFieldOrNull(field) != null) {
            // OK, it was an oddly named field
            fields.add(field);
            if( key != null ) {
              rename.add(field, key);
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing fieldname: " + e.getMessage(), e);
          }
        }

       // end try as function

      } // end for(;;)
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing fieldname", e);
    }
  }
  
  public Set<String> getLuceneFieldNames()
  {
    return (_wantsAllFields || fields.isEmpty()) ? null : fields;
  }

  public boolean wantsAllFields()
  {
    return _wantsAllFields;
  }
  
  public boolean wantsScore()
  {
    return _wantsScore;
  }

  public boolean wantsField( String name )
  {
    if( _wantsAllFields || okFieldNames.contains( name ) ){
      return true;
    }
    for( String s : globs ) {
      // TODO something better?
      if( FilenameUtils.wildcardMatch( name, s ) ) {
        return true;
      }
    }
    return false;
  }

  public DocTransformer getTransformer()
  {
    return transformer;
  }
}
