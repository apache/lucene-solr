package org.apache.solr.search.sorting;

import org.apache.lucene.search.ScoreDoc;

import java.util.List;

/**
 * Created by bkinla201 on 3/21/17.
 */
public class TieBreakerGroup {
    int start;
    List<ScoreDoc> docs;

    public TieBreakerGroup(int start, List<ScoreDoc> docs) {
        this.start = start;
        this.docs = docs;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return start + docs.size() - 1;
    }

    public List<ScoreDoc> getDocs() {
        return docs;
    }

    public void setDocs(List<ScoreDoc> docs) {
        this.docs = docs;
    }
}
