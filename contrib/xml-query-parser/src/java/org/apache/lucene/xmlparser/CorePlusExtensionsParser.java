package org.apache.lucene.xmlparser;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.xmlparser.builders.BooleanFilterBuilder;
import org.apache.lucene.xmlparser.builders.BoostingQueryBuilder;
import org.apache.lucene.xmlparser.builders.DuplicateFilterBuilder;
import org.apache.lucene.xmlparser.builders.FuzzyLikeThisQueryBuilder;
import org.apache.lucene.xmlparser.builders.LikeThisQueryBuilder;
import org.apache.lucene.xmlparser.builders.TermsFilterBuilder;
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
public class CorePlusExtensionsParser extends CoreParser
{

	public CorePlusExtensionsParser(Analyzer analyzer, QueryParser parser)
	{
		super(analyzer, parser);
		filterFactory.addBuilder("TermsFilter",new TermsFilterBuilder(analyzer));
		filterFactory.addBuilder("BooleanFilter",new BooleanFilterBuilder(filterFactory));
		filterFactory.addBuilder("DuplicateFilter",new DuplicateFilterBuilder());
		String fields[]={"contents"};
		queryFactory.addBuilder("LikeThisQuery",new LikeThisQueryBuilder(analyzer,fields));
		queryFactory.addBuilder("BoostingQuery", new BoostingQueryBuilder(queryFactory));
		queryFactory.addBuilder("FuzzyLikeThisQuery", new FuzzyLikeThisQueryBuilder(analyzer));
		
	}


}
