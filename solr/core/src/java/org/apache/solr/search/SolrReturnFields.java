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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.DocTransformers;
import org.apache.solr.response.transform.RenameFieldTransformer;
import org.apache.solr.response.transform.ScoreAugmenter;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.response.transform.ValueSourceAugmenter;
import org.apache.solr.search.SolrDocumentFetcher.RetrieveFieldsOptimizer;

/**
 * The default implementation of return fields parsing for Solr.
 */
public class SolrReturnFields extends ReturnFields {
  // Special Field Keys
  public static final String SCORE = "score";

  private final List<String> globs = new ArrayList<>(1);

  // The lucene field names to request from the SolrIndexSearcher
  // This *may* include fields that will not be in the final response
  private final Set<String> fields = new HashSet<>();

  // Field names that are OK to include in the response.
  // This will include pseudo fields, lucene fields, and matching globs
  private Set<String> okFieldNames = new HashSet<>();

  // The list of explicitly requested fields
  // Order is important for CSVResponseWriter
  private Set<String> reqFieldNames = null;
  
  protected DocTransformer transformer;
  protected boolean _wantsScore = false;
  protected boolean _wantsAllFields = false;
  protected Map<String,String> renameFields = Collections.emptyMap();

  // Only set currently with the SolrDocumentFetcher.solrDoc method. Primarily used
  // at this time for testing to ensure we get fields from the expected places.
  public enum FIELD_SOURCES {
      NOT_SET, ALL_FROM_DV, ALL_FROM_STORED, MIXED_SOURCES
  }

  public FIELD_SOURCES getFieldSources() {
    return fieldSources;
  }

  public void setFieldSources(FIELD_SOURCES fieldSources) {
    this.fieldSources = fieldSources;
  }

  private FIELD_SOURCES fieldSources = FIELD_SOURCES.NOT_SET;
  // For each individual result list, we need to have a separate fetch optimizer
  // to use. It's particularly important to keep this list separated during, say,
  // sub-query transformations.
  //
  private RetrieveFieldsOptimizer fetchOptimizer = null;

  public SolrReturnFields() {
    _wantsAllFields = true;
  }

  public SolrReturnFields(SolrQueryRequest req) {
    this( req.getParams().getParams(CommonParams.FL), req );
  }

  public SolrReturnFields(String fl, SolrQueryRequest req) {
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

  public SolrReturnFields(String[] fl, SolrQueryRequest req) {
    parseFieldList(fl, req);
  }

  /**
   * For pre-parsed simple field list with optional transformer.
   * Does not support globs or the score.
   * This constructor is more for internal use; not for parsing user input.
   *
   * @param plainFields simple field list; nothing special. If null, equivalent to all-fields.
   * @param docTransformer optional transformer.
   */
  public SolrReturnFields(Collection<String> plainFields, DocTransformer docTransformer) {
    if (plainFields != null) {
      _wantsAllFields = false;
      for (String field : plainFields) {
        assert field.indexOf('*') == -1 && !field.equals(SCORE);
        addField(field, null, null, false);
      }
    } else {
      _wantsAllFields = true;
    }
    if (docTransformer != null) {
      transformer = docTransformer;
      // doc transformer can request extra fields.
      String[] extraRequestFields = docTransformer.getExtraRequestFields();
      if (extraRequestFields != null) {
        Collections.addAll(fields, extraRequestFields); // do NOT call addField
      }
    }
  }

  public RetrieveFieldsOptimizer getFetchOptimizer(Supplier<RetrieveFieldsOptimizer> supplier) {
    if (fetchOptimizer == null) {
      fetchOptimizer = supplier.get();
    }
    return fetchOptimizer;
  }


  private void parseFieldList(String[] fl, SolrQueryRequest req) {
    _wantsScore = false;
    _wantsAllFields = false;
    if (fl == null || fl.length == 0 || fl.length == 1 && fl[0].length()==0) {
      _wantsAllFields = true;
      return;
    }

    NamedList<String> rename = new NamedList<>();
    DocTransformers augmenters = new DocTransformers();
    for (String fieldList : fl) {
      add(fieldList,rename,augmenters,req);
    }
    for( int i=0; i<rename.size(); i++ ) {
      String from = rename.getName(i);
      String to = rename.getVal(i);
      okFieldNames.add( to );
      boolean copy = (reqFieldNames!=null && reqFieldNames.contains(from));
      if(!copy) {
        // Check that subsequent copy/rename requests have the field they need to copy
        for(int j=i+1; j<rename.size(); j++) {
          if(from.equals(rename.getName(j))) {
            rename.setName(j, to); // copy from the current target
            if(reqFieldNames==null) {
              reqFieldNames = new LinkedHashSet<>();
            }
            reqFieldNames.add(to); // don't rename our current target
          }
        }
      }
      augmenters.addTransformer( new RenameFieldTransformer( from, to, copy ) );
    }
    if (rename.size() > 0 ) {
      renameFields = rename.asShallowMap();
    }
    if( !_wantsAllFields && !globs.isEmpty() ) {
      // TODO??? need to fill up the fields with matching field names in the index
      // and add them to okFieldNames?
      // maybe just get all fields?
      // this would disable field selection optimization... i think that is OK
      fields.clear(); // this will get all fields, and use wantsField to limit
    }

    if( augmenters.size() == 1 ) {
      transformer = augmenters.getTransformer(0);
    }
    else if( augmenters.size() > 1 ) {
      transformer = augmenters;
    }
  }

  @Override
  public Map<String,String> getFieldRenames() {
    return renameFields;
  }

  // like getId, but also accepts dashes for legacy fields
  public static String getFieldName(StrParser sp) {
    sp.eatws();
    int id_start = sp.pos;
    char ch;
    if (sp.pos < sp.end && (ch = sp.val.charAt(sp.pos)) != '$' && Character.isJavaIdentifierStart(ch)) {
      sp.pos++;
      while (sp.pos < sp.end) {
        ch = sp.val.charAt(sp.pos);
        if (!Character.isJavaIdentifierPart(ch) && ch != '.' && ch != '-') {
          break;
        }
        sp.pos++;
      }
      return sp.val.substring(id_start, sp.pos);
    }

    return null;
  }

  private void add(String fl, NamedList<String> rename, DocTransformers augmenters, SolrQueryRequest req) {
    if( fl == null ) {
      return;
    }
    try {
      StrParser sp = new StrParser(fl);

      for(;;) {
        sp.opt(',');
        sp.eatws();
        if (sp.pos >= sp.end) break;

        int start = sp.pos;

        // short circuit test for a really simple field name
        String key = null;
        String field = getFieldName(sp);
        char ch = sp.ch();

        if (field != null) {
          if (sp.opt(':')) {
            // this was a key, not a field name
            key = field;
            field = null;
            sp.eatws();
            start = sp.pos;
          } else {
            if (Character.isWhitespace(ch) || ch == ',' || ch==0) {
              addField(field, key, augmenters, false);
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
          if (field != null && (Character.isWhitespace(ch) || ch == ',' || ch==0)) {
            rename.add(field, key);
            addField(field, key, augmenters, false);
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
          if (field != null && (Character.isWhitespace(ch) || ch == ',' || ch==0)) {
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
          ModifiableSolrParams augmenterParams = new ModifiableSolrParams();
          int end = QueryParsing.parseLocalParams(funcStr, 0, augmenterParams, req.getParams(), "[", ']');
          sp.pos += end;

          // [foo] is short for [type=foo] in localParams syntax
          String augmenterName = augmenterParams.get("type");
          augmenterParams.remove("type");
          String disp = key;
          if( disp == null ) {
            disp = '['+augmenterName+']';
          }

          TransformerFactory factory = req.getCore().getTransformerFactory( augmenterName );
          if( factory != null ) {
            DocTransformer t = factory.create(disp, augmenterParams, req);
            if(t!=null) {
              if(!_wantsAllFields) {
                String[] extra = t.getExtraRequestFields();
                if(extra!=null) {
                  for(String f : extra) {
                    fields.add(f); // also request this field from IndexSearcher
                  }
                }
              }
              augmenters.addTransformer( t );
            }
          }
          else {
            //throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown DocTransformer: "+augmenterName);
          }
          addField(field, disp, augmenters, true);
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
          funcStr = sp.val.substring(start, sp.pos);


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
          }

          if (key==null) {
            key = funcStr;
          }
          addField(funcStr, key, augmenters, true);
          augmenters.addTransformer( new ValueSourceAugmenter( key, parser, vs ) );
        }
        catch (SyntaxError e) {
          // try again, simple rules for a field name with no whitespace
          sp.pos = start;
          field = sp.getSimpleString();

          if (req.getSchema().getFieldOrNull(field) != null) {
            // OK, it was an oddly named field
            addField(field, key, augmenters, false);
            if( key != null ) {
              rename.add(field, key);
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing fieldname: " + e.getMessage(), e);
          }
        }

        // end try as function

      } // end for(;;)
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing fieldname", e);
    }
  }

  private void addField(String field, String key, DocTransformers augmenters, boolean isPseudoField)
  {
    if(reqFieldNames==null) {
      reqFieldNames = new LinkedHashSet<>();
    }
    
    if(key==null) {
      reqFieldNames.add(field);
    }
    else {
      reqFieldNames.add(key);
    }

    if ( ! isPseudoField) {
      // fields is returned by getLuceneFieldNames(), to be used to select which real fields
      // to return, so pseudo-fields should not be added
      fields.add(field);
    }

    okFieldNames.add( field );
    okFieldNames.add( key );
    // a valid field name
    if(SCORE.equals(field)) {
      _wantsScore = true;

      String disp = (key==null) ? field : key;
      augmenters.addTransformer( new ScoreAugmenter( disp ) );
    }
  }

  @Override
  public Set<String> getLuceneFieldNames()
  {
    return getLuceneFieldNames(false);
  }

  @Override
  public Set<String> getLuceneFieldNames(boolean ignoreWantsAll)
  {
    if (ignoreWantsAll)
      return fields;
    else
      return (_wantsAllFields || fields.isEmpty()) ? null : fields;
  }

  @Override
  public Set<String> getRequestedFieldNames() {
    if(_wantsAllFields || reqFieldNames == null || reqFieldNames.isEmpty()) {
      return null;
    }
    return reqFieldNames;
  }

  @Override
  public Set<String> getExplicitlyRequestedFieldNames() {
    if (reqFieldNames == null || reqFieldNames.isEmpty()) {
      return null;
    }
    return reqFieldNames;
  }
  
  @Override
  public boolean hasPatternMatching() {
    return !globs.isEmpty();
  }

  @Override
  public boolean wantsField(String name)
  {
    if( _wantsAllFields || okFieldNames.contains( name ) ){
      return true;
    }
    for( String s : globs ) {
      // TODO something better?
      if( FilenameUtils.wildcardMatch(name, s) ) {
        okFieldNames.add(name); // Don't calculate it again
        return true;
      }
    }
    return false;
  }
  
  @Override
  public boolean wantsAllFields()
  {
    return _wantsAllFields;
  }

  @Override
  public boolean wantsScore()
  {
    return _wantsScore;
  }

  @Override
  public DocTransformer getTransformer()
  {
    return transformer;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SolrReturnFields=(");
    sb.append("globs="); sb.append(globs);
    sb.append(",fields="); sb.append(fields);
    sb.append(",okFieldNames="); sb.append(okFieldNames);
    sb.append(",reqFieldNames="); sb.append(reqFieldNames);
    sb.append(",transformer="); sb.append(transformer);
    sb.append(",wantsScore="); sb.append(_wantsScore);
    sb.append(",wantsAllFields="); sb.append(_wantsAllFields);
    sb.append(')');
    return sb.toString();
  }
}
