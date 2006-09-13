/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;

// TODO: implement the analysis of simple fields with
// FieldType.toInternal() instead of going through the
// analyzer.  Should lead to faster query parsing.

/**
 * @author yonik
 */
public class SolrQueryParser extends QueryParser {
  protected final IndexSchema schema;

  /**
   *
   * @param schema Used for default search field name if defaultField is null and field information is used for analysis
   * @param defaultField default field used for unspecified search terms.  if null, the schema default field is used
   */
  public SolrQueryParser(IndexSchema schema, String defaultField) {
    super(defaultField == null ? schema.getDefaultSearchFieldName() : defaultField, schema.getQueryAnalyzer());
    this.schema = schema;
    setLowercaseExpandedTerms(false);
  }

  protected Query getFieldQuery(String field, String queryText) throws ParseException {
    // intercept magic field name of "_" to use as a hook for our
    // own functions.
    if (field.equals("_val_")) {
      return QueryParsing.parseFunction(queryText, schema);
    }

    // default to a normal field query
    return super.getFieldQuery(field, queryText);
  }

  protected Query getRangeQuery(String field, String part1, String part2, boolean inclusive) throws ParseException {
    FieldType ft = schema.getFieldType(field);
    return new ConstantScoreRangeQuery(
      field,
      "*".equals(part1) ? null : ft.toInternal(part1),
      "*".equals(part2) ? null : ft.toInternal(part2),
      inclusive, inclusive);
  }

  protected Query getPrefixQuery(String field, String termStr) throws ParseException {
    if (getLowercaseExpandedTerms()) {
      termStr = termStr.toLowerCase();
    }

    // TODO: toInternal() won't necessarily work on partial
    // values, so it looks like i need a getPrefix() function
    // on fieldtype?  Or at the minimum, a method on fieldType
    // that can tell me if I should lowercase or not...
    // Schema could tell if lowercase filter is in the chain,
    // but a more sure way would be to run something through
    // the first time and check if it got lowercased.

    // TODO: throw exception of field type doesn't support prefixes?
    // (sortable numeric types don't do prefixes, but can do range queries)
    Term t = new Term(field, termStr);
    return new ConstantScorePrefixQuery(t);
  }

}
