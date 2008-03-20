package org.apache.solr.highlight;

import java.io.IOException;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.Config;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;

public class DummyHighlighter extends SolrHighlighter {

	@Override
	public NamedList<Object> doHighlighting(DocList docs, Query query,
			SolrQueryRequest req, String[] defaultFields) throws IOException {
		NamedList fragments = new SimpleOrderedMap();
		fragments.add("dummy", "thing1");
		return fragments;
	}

	@Override
	public void initalize(Config config) {
		// do nothing
	}

}
