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

import java.util.*;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.DocTransformers;
import org.apache.solr.response.transform.RenameFieldsTransformer;
import org.apache.solr.response.transform.ScoreAugmenter;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.response.transform.ValueSourceAugmenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class representing the return fields
 *
 *
 * @since solr 4.0
 */
public class ReturnFields
{
  static final Logger log = LoggerFactory.getLogger( ReturnFields.class );

  // Special Field Keys
  public static final String SCORE = "score";

  private final List<String> globs = new ArrayList<String>(1);
  
  // The lucene field names to request from the SolrIndexSearcher
  // Order is important for CSVResponseWriter
  private final Set<String> fields = new LinkedHashSet<String>();
  
  // Field names that are OK to include in the response.
  // This will include pseudo fields, lucene fields, and matching globs
  private Set<String> okFieldNames = new HashSet<String>(); 
  
  private DocTransformer transformer;
  private boolean _wantsScore = false;
  private boolean _wantsAllFields = false;

  public ReturnFields() {
    _wantsAllFields = true;
  }

  public ReturnFields(SolrQueryRequest req) {
    this( req.getParams().getParams(CommonParams.FL), req );
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
  }

  public ReturnFields(String[] fl, SolrQueryRequest req) {
    parseFieldList(fl, req);
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

    // Legacy behavior: "score" == "*,score"  
    if( fields.size() == 1 && _wantsScore && augmenters.size() == 1 && globs.isEmpty() ) {
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
          if (sp.opt(':')) {
            // this was a key, not a field name
            key = field;
            field = null;
            sp.eatws();
            start = sp.pos;
          } else {
            if (ch==' ' || ch == ',' || ch==0) {
              addField( field, key, augmenters, req );
              continue;
            }
            // an invalid field name... reset the position pointer to retry
            sp.pos = start;
            field = null;
          }
        }

        if (key != null) {
          // we read "key : "
          field = sp.getId(null);
          ch = sp.ch();
          if (field != null && (ch==' ' || ch == ',' || ch==0)) {
            rename.add(field, key);
            addField( field, key, augmenters, req );
            continue;
          }
          // an invalid field name... reset the position pointer to retry
          sp.pos = start;
          field = null;
        }

        if (field == null) {
          // We didn't find a simple name, so let's see if it's a globbed field name.
          // Globbing only works with field names of the recommended form (roughly like java identifiers)

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

        String funcStr = sp.val.substring(start);

        // Is it an augmenter of the form [augmenter_name foo=1 bar=myfield]?
        // This is identical to localParams syntax except it uses [] instead of {!}

        if (funcStr.startsWith("[")) {
          Map<String,String> augmenterArgs = new HashMap<String,String>();
          int end = QueryParsing.parseLocalParams(funcStr, 0, augmenterArgs, req.getParams(), "[", ']');
          sp.pos += end;
          
          // [foo] is short for [type=foo] in localParams syntax
          String augmenterName = augmenterArgs.remove("type"); 
          String disp = key;
          if( disp == null ) {
            disp = '['+augmenterName+']';
          }

          TransformerFactory factory = req.getCore().getTransformerFactory( augmenterName );
          if( factory != null ) {
            MapSolrParams augmenterParams = new MapSolrParams( augmenterArgs );
            augmenters.addTransformer( factory.create(disp, augmenterParams, req) );
          }
          else {
            // unknown transformer?
          }
          addField(field, disp, augmenters, req);
          continue;
        }


        // let's try it as a function instead
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


          if (key==null) {
            key = funcStr;
          }
          okFieldNames.add( key );
          okFieldNames.add( funcStr );
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

  private void addField( String field, String key, DocTransformers augmenters, SolrQueryRequest req )
  {
    String disp = (key==null) ? field : key;
    fields.add(field); // need to put in the map to maintain order for things like CSVResponseWriter
    okFieldNames.add( field );
    okFieldNames.add( key );
    // a valid field name
    if(SCORE.equals(field)) {
      _wantsScore = true;
      augmenters.addTransformer( new ScoreAugmenter( disp ) );
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
        okFieldNames.add(name); // Don't calculate it again
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
