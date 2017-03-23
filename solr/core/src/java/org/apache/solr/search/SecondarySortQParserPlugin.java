package org.apache.solr.search;

import org.apache.lucene.search.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.sorting.SecondarySortCollector;
import org.apache.solr.search.sorting.TBGAwareCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SecondarySortQParserPlugin extends QParserPlugin {
    public static final String NAME = "sesort";

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new SecondarySortQParser(qstr, localParams, params, req);
    }

    private class SecondarySortQParser extends QParser {

        String qstr;
        SolrParams localParams;
        SolrParams params;
        SolrQueryRequest req;

        private SecondarySortQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
            super(qstr, localParams, params, req);
            this.qstr = qstr;
            this.localParams = localParams;
            this.params = params;
            this.req = req;
        }

        @Override
        public Query parse() {

            String sortStr = localParams.get(CommonParams.SORT);
            String[] sortAlgs;
            if(sortStr == null) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Need to provide sort algorithms to perform secondary sort.");
            } else {
                sortAlgs = sortStr.split(",");
                if(sortAlgs.length == 0) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Need to provide valid sort algorithms to perform secondary sort.");
                }
            }

            SolrCore core = req.getCore();
            Map<String, TBGAwareCollector> secondaySortCollectorMap = core.getLatestSchema().getSecondarySortCollectorMap();
            List<TBGAwareCollector> collectors = new ArrayList<>();

            for(String sortAlg : sortAlgs) {
                if(secondaySortCollectorMap.containsKey(sortAlg)) {
                    collectors.add(secondaySortCollectorMap.get(sortAlg));
                }
            }

            return new RankQuery() {
                Query mainQuery;

                @Override
                public TopDocsCollector getTopDocsCollector(int len, QueryCommand cmd, IndexSearcher searcher) throws IOException {
                    return SecondarySortCollector.create(len, collectors, null ,false, false, false);
                }

                @Override
                public MergeStrategy getMergeStrategy() {
                    return null;
                }

                @Override
                public RankQuery wrap(Query mainQuery) {
                    this.mainQuery = mainQuery;
                    return this;
                }

                @Override
                public boolean equals(Object o) {
                    return false;
                }

                @Override
                public int hashCode() {
                    return 0;
                }
            };
        }
    }
}
