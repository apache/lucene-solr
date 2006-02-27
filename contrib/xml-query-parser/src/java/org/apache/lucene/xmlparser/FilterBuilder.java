/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser;

import org.apache.lucene.search.Filter;
import org.w3c.dom.Element;

/**
 * @author maharwood
 */
public interface FilterBuilder {
	 public Filter getFilter(Element e) throws ParserException;
}
