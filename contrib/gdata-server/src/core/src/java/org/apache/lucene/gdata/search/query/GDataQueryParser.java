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

package org.apache.lucene.gdata.search.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.queryParser.QueryParser;

/**
 *
 * 
 */
public class GDataQueryParser extends QueryParser {

    /**
     * Creates a new QueryParser instance and sets the default operator to
     * {@link Operator#AND}
     * 
     * @param field -
     *            the parser field
     * @param analyzer -
     *            the parser analyzer
     */
    public GDataQueryParser(String field, Analyzer analyzer) {
        super(field, analyzer);
        this.setDefaultOperator(Operator.AND);
    }

    /**
     * Creates a new QueryParser instance and sets the default operator to
     * {@link Operator#AND}. The parser will use
     * {@link IndexSchema#getDefaultSearchField} as the field and
     * {@link IndexSchema#getSchemaAnalyzer()} as the analyzer.
     * 
     * @param schema -
     *            the schema to set the default fields
     */
    public GDataQueryParser(IndexSchema schema) {
        this(schema.getDefaultSearchField(), schema.getSchemaAnalyzer());

    }

}
