package org.apache.solr.highlight;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.Config;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.util.SolrPluginUtils;

public abstract class SolrHighlighter 
{
	public static Logger log = Logger.getLogger(SolrHighlighter.class.getName());

	// Thread safe registry
	protected final Map<String,SolrFormatter> formatters = 
		Collections.synchronizedMap( new HashMap<String, SolrFormatter>() );

	// Thread safe registry
	protected final Map<String,SolrFragmenter> fragmenters = 
		Collections.synchronizedMap( new HashMap<String, SolrFragmenter>() );

	public abstract void initalize( final Config config );


	/**
	 * Check whether Highlighting is enabled for this request.
	 * @param params The params controlling Highlighting
	 * @return <code>true</code> if highlighting enabled, <code>false</code> if not.
	 */
	public boolean isHighlightingEnabled(SolrParams params) {
		return params.getBool(HighlightParams.HIGHLIGHT, false);
	}

	/**
	 * Return a String array of the fields to be highlighted.
	 * Falls back to the programatic defaults, or the default search field if the list of fields
	 * is not specified in either the handler configuration or the request.
	 * @param query The current Query
	 * @param request The current SolrQueryRequest
	 * @param defaultFields Programmatic default highlight fields, used if nothing is specified in the handler config or the request.
	 */
	public String[] getHighlightFields(Query query, SolrQueryRequest request, String[] defaultFields) {
		String fields[] = request.getParams().getParams(HighlightParams.FIELDS);

		// if no fields specified in the request, or the handler, fall back to programmatic default, or default search field.
		if(emptyArray(fields)) {
			// use default search field if highlight fieldlist not specified.
			if (emptyArray(defaultFields)) {
				String defaultSearchField = request.getSchema().getSolrQueryParser(null).getField();
				fields = null == defaultSearchField ? new String[]{} : new String[]{defaultSearchField};
			}  
			else {
				fields = defaultFields;
			}
		}
		else if (fields.length == 1) {
			// if there's a single request/handler value, it may be a space/comma separated list
			fields = SolrPluginUtils.split(fields[0]);
		}

		return fields;
	}

	protected boolean emptyArray(String[] arr) {
		return (arr == null || arr.length == 0 || arr[0] == null || arr[0].trim().length() == 0);
	}

	/**
	 * Generates a list of Highlighted query fragments for each item in a list
	 * of documents, or returns null if highlighting is disabled.
	 *
	 * @param docs query results
	 * @param query the query
	 * @param req the current request
	 * @param defaultFields default list of fields to summarize
	 *
	 * @return NamedList containing a NamedList for each document, which in 
	 * turns contains sets (field, summary) pairs.
	 */
	@SuppressWarnings("unchecked")
	public abstract NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException;
}
