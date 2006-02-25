/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;



/**
 * @author maharwood
 */
public class UserInputQueryBuilder implements QueryBuilder {

	QueryParser parser;
	
	/**
	 * @param parser
	 */
	public UserInputQueryBuilder(QueryParser parser) {
		this.parser = parser;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
	 */
	public Query getQuery(Element e) throws ParserException {
		String text=DOMUtils.getText(e);
		try {
			Query q = parser.parse(text);
			q.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
			return q;
		} catch (ParseException e1) {
			throw new ParserException(e1.getMessage());
		}
	}

}
