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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.FixedBitSetCachingWrapperFilter;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResponseWriterUtil;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

/**
 *
 * @since solr 4.9
 *
 * This transformer returns all descendants of each parent document in a flat list nested inside the parent document.
 *
 *
 * The "parentFilter" parameter is mandatory.
 * Optionally you can provide a "childFilter" param to filter out which child documents should be returned and a
 * "limit" param which provides an option to specify the number of child documents
 * to be returned per parent document. By default it's set to 10.
 *
 * Examples -
 * [child parentFilter="fieldName:fieldValue"]
 * [child parentFilter="fieldName:fieldValue" childFilter="fieldName:fieldValue"]
 * [child parentFilter="fieldName:fieldValue" childFilter="fieldName:fieldValue" limit=20]
 */
public class ChildDocTransformerFactory extends TransformerFactory {

  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    SchemaField uniqueKeyField = req.getSchema().getUniqueKeyField();
    if(uniqueKeyField == null) {
      throw new SolrException( ErrorCode.BAD_REQUEST,
          " ChildDocTransformer requires the schema to have a uniqueKeyField." );
    }

    String parentFilter = params.get( "parentFilter" );
    if( parentFilter == null ) {
      throw new SolrException( ErrorCode.BAD_REQUEST, "Parent filter should be sent as parentFilter=filterCondition" );
    }

    String childFilter = params.get( "childFilter" );
    int limit = params.getInt( "limit", 10 );

    Filter parentsFilter = null;
    try {
      Query parentFilterQuery = QParser.getParser( parentFilter, null, req).getQuery();
      parentsFilter = new FixedBitSetCachingWrapperFilter(new QueryWrapperFilter(parentFilterQuery));
    } catch (SyntaxError syntaxError) {
      throw new SolrException( ErrorCode.BAD_REQUEST, "Failed to create correct parent filter query" );
    }

    Query childFilterQuery = null;
    if(childFilter != null) {
      try {
        childFilterQuery = QParser.getParser( childFilter, null, req).getQuery();
      } catch (SyntaxError syntaxError) {
        throw new SolrException( ErrorCode.BAD_REQUEST, "Failed to create correct child filter query" );
      }
    }

    return new ChildDocTransformer( field, parentsFilter, uniqueKeyField, req.getSchema(), childFilterQuery, limit);
  }
}

class ChildDocTransformer extends TransformerWithContext {
  private final String name;
  private final SchemaField idField;
  private final IndexSchema schema;
  private Filter parentsFilter;
  private Query childFilterQuery;
  private int limit;

  public ChildDocTransformer( String name, final Filter parentsFilter, 
                              final SchemaField idField, IndexSchema schema,
                              final Query childFilterQuery, int limit) {
    this.name = name;
    this.idField = idField;
    this.schema = schema;
    this.parentsFilter = parentsFilter;
    this.childFilterQuery = childFilterQuery;
    this.limit = limit;
  }

  @Override
  public String getName()  {
    return name;
  }

  @Override
  public void transform(SolrDocument doc, int docid) {

    FieldType idFt = idField.getType();
    Object parentIdField = doc.getFirstValue(idField.getName());
    
    String parentIdExt = parentIdField instanceof IndexableField
      ? idFt.toExternal((IndexableField)parentIdField)
      : parentIdField.toString();

    try {
      Query parentQuery = idFt.getFieldQuery(null, idField, parentIdExt);
      Query query = new ToChildBlockJoinQuery(parentQuery, parentsFilter, false);
      DocList children = context.searcher.getDocList(query, childFilterQuery, new Sort(), 0, limit);
      if(children.matches() > 0) {
        DocIterator i = children.iterator();
        while(i.hasNext()) {
          Integer childDocNum = i.next();
          Document childDoc = context.searcher.doc(childDocNum);
          SolrDocument solrChildDoc = ResponseWriterUtil.toSolrDocument(childDoc, schema);

          // TODO: future enhancement...
          // support an fl local param in the transformer, which is used to build
          // a private ReturnFields instance that we use to prune unwanted field 
          // names from solrChildDoc
          doc.addChildDocument(solrChildDoc);
        }
      }
      
    } catch (IOException e) {
      doc.put(name, "Could not fetch child Documents");
    }
  }
}

