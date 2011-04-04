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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.DocTransformers;
import org.apache.solr.response.transform.RenameFieldsTransformer;
import org.apache.solr.response.transform.ScoreAugmenter;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.response.transform.ValueSourceAugmenter;
import org.apache.solr.search.function.FunctionQuery;
import org.apache.solr.search.function.QueryValueSource;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class representing the return fields
 *
 * @version $Id$
 * @since solr 4.0
 */
public class ReturnFields
{
  static final Logger log = LoggerFactory.getLogger( ReturnFields.class );

  // Special Field Keys
  public static final String SCORE = "score";

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
    this( req.getParams().getParams(CommonParams.FL), req );
  }
  
  // This is only used in testing?
  public ReturnFields(String fl, SolrQueryRequest req) {
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

    boolean checkPseudoFields = req.getParams().getBool(CommonParams.PSEUDO_FL, false);
    NamedList<String> rename = new NamedList<String>();
    DocTransformers augmenters = new DocTransformers();
    for (String fieldList : fl) {
      addFieldParam(fieldList,checkPseudoFields,rename,augmenters,req);
    }
    if( rename.size() > 0 ) {
      for( int i=0; i<rename.size(); i++ ) {
        okFieldNames.add( rename.getVal(i) );
      }
      augmenters.addTransformer( new RenameFieldsTransformer( rename ) );
    }

    // Legacy behavior? "score" == "*,score"  Distributed tests for this
    if( _wantsScore && fields.size() == 1 && augmenters.size() == 1 ) {
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

  /**
   * TODO, this should be a fancy parser that splits on ',' but respects quoted things
   */
  private void addFieldParam(String fls, boolean checkPseudoFields, NamedList<String> rename, DocTransformers augmenters, SolrQueryRequest req) {
    if( fls != null ) {
      String[] fields = fls.split(",");
      for( String fl : fields ) {
        String as = null;
        int idx = fl.indexOf( ':' );
        if( idx > 0 ) {
          as = fl.substring(0,idx).trim();
          fl = fl.substring(idx+1).trim();
        }
        else {
          fl = fl.trim();
        }
        
        if( checkPseudoFields ) {
          String pseudo = req.getParams().get( CommonParams.PSEUDO_FL+'.'+fl );
          if( pseudo != null ) {
            if( as == null ) {
              as = fl;  // use the original
            }
            
            // Just replace the input text
            okFieldNames.add( fl );
            fl = pseudo;
          }
        }
        
        addField( fl, as, rename, augmenters, req );
      }
    }
  }
  
  private void addField(String fl, String display, NamedList<String> rename, DocTransformers augmenters, SolrQueryRequest req) {
    // Maybe it is everything
    if( "*".equals( fl ) ) {
      if( display != null ) {
        throw new SolrException( ErrorCode.BAD_REQUEST, "* can not use an 'AS' request" );
      }
      _wantsAllFields = true;
      return;
    }
    
    // maybe it is a Transformer (starts and ends with [])
    if( fl.charAt( 0 ) == '[' && fl.charAt( fl.length()-1 ) == ']' ) {
      String name = null;
      String args = null;
      int idx = fl.indexOf( ' ' );
      if( idx > 0 ) {
        name = fl.substring(1,idx);
        args = fl.substring(idx+1,fl.length()-1);
      }
      else {
        name = fl.substring(1,fl.length()-1 );
      }

      TransformerFactory factory = req.getCore().getTransformerFactory( name );
      if( factory != null ) {
        augmenters.addTransformer( factory.create(display==null?fl:display, args, req) );
        return;
      }
      else {
        // unknown field?  field that starts with [ and ends with ]?
      }
    }
    
    // If it has a ( it may be a FunctionQuery
    else if( StringUtils.contains(fl, '(' ) ) {
      try {
        QParser parser = QParser.getParser(fl, FunctionQParserPlugin.NAME, req);
        Query q = null;
        ValueSource vs = null;

        if (parser instanceof FunctionQParser) {
          FunctionQParser fparser = (FunctionQParser)parser;
          fparser.setParseMultipleSources(false);
          fparser.setParseToEnd(false);

          q = fparser.getQuery();
        } else {
          // A QParser that's not for function queries.
          // It must have been specified via local params.
          q = parser.getQuery();
          assert parser.getLocalParams() != null;
        }

        if (q instanceof FunctionQuery) {
          vs = ((FunctionQuery)q).getValueSource();
        } else {
          vs = new QueryValueSource(q, 0.0f);
        }
        
        okFieldNames.add( fl );
        okFieldNames.add( display );
        augmenters.addTransformer( new ValueSourceAugmenter( display==null?fl:display, parser, vs ) );
        return;
      }
      catch (Exception e) {
        // Its OK... could just be a wierd field name
      }
    }
    
    // TODO? support fancy globs?
    else if( fl.endsWith( "*" ) || fl.startsWith( "*" ) ) {
      globs.add( fl );
      return;
    }

    fields.add( fl ); // need to put in the map to maintain order for things like CSVResponseWriter
    okFieldNames.add( fl );
    okFieldNames.add( display );
    
    if( SCORE.equals(fl)) {
      _wantsScore = true;
      augmenters.addTransformer( new ScoreAugmenter( display==null?fl:display ) );
    }
    else {
      // it is a normal field
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
