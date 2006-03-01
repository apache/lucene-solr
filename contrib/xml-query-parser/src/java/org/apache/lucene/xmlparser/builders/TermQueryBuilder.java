/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;


/**
 * @author maharwood
 */
public class TermQueryBuilder implements QueryBuilder {

	public Query getQuery(Element e) throws ParserException {
		
        String field=DOMUtils.getAttributeWithInheritanceOrFail(e,"fieldName");
 		String value=DOMUtils.getNonBlankTextOrFail(e);
  		TermQuery tq = new TermQuery(new Term(field,value));
  		tq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));

  		return tq;
		
	}

}
