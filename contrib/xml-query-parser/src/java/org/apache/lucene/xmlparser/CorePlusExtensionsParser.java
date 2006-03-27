package org.apache.lucene.xmlparser;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.xmlparser.builders.BooleanFilterBuilder;
import org.apache.lucene.xmlparser.builders.BoostingQueryBuilder;
import org.apache.lucene.xmlparser.builders.FuzzyLikeThisQueryBuilder;
import org.apache.lucene.xmlparser.builders.LikeThisQueryBuilder;
import org.apache.lucene.xmlparser.builders.TermsFilterBuilder;

public class CorePlusExtensionsParser extends CoreParser
{

	public CorePlusExtensionsParser(Analyzer analyzer, QueryParser parser)
	{
		super(analyzer, parser);
		filterFactory.addBuilder("TermsFilter",new TermsFilterBuilder(analyzer));
		filterFactory.addBuilder("BooleanFilter",new BooleanFilterBuilder(filterFactory));
		String fields[]={"contents"};
		queryFactory.addBuilder("LikeThisQuery",new LikeThisQueryBuilder(analyzer,fields));
		queryFactory.addBuilder("BoostingQuery", new BoostingQueryBuilder(queryFactory));
		queryFactory.addBuilder("FuzzyLikeThisQuery", new FuzzyLikeThisQueryBuilder(analyzer));
		
	}


}
