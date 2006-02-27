package org.apache.lucene.xmlparser;

import org.apache.lucene.search.Query;
import org.w3c.dom.Element;

/**
 * Implemented by objects that produce Lucene Query objects from XML streams. Implementations are
 * expected to be thread-safe so that they can be used to simultaneously parse multiple XML documents.
 * @author maharwood
 */
public interface QueryBuilder {
	
	public Query getQuery(Element e) throws ParserException;

}
