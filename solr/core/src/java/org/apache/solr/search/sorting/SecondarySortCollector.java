package org.apache.solr.search.sorting;


import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import com.google.common.collect.RangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeMap;

/**
 * This collector will be used to chain together a dynamic list of collectors,
 * to perform sorting and ranking on the result set. The collectors passed in are
 * expected to implement the TieBreakerGroupAware interface, which requires the collector
 * to return any
 */
public class SecondarySortCollector extends TopDocsCollector {

    private boolean fillFields;
    private boolean trackDocScores;
    private boolean trackMaxScore;
    private int numHits;
    private FieldDoc after;
    private TBGAwareCollector initCollector;
    private ListIterator<TBGAwareCollector> collectorsIterator;
    private LeafReaderContext singleLeafReaderContext;
    private RangeMap<Integer, LeafReaderContext> contextRangeMap;

    public SecondarySortCollector(PriorityQueue<?> priorityQueue,
                                  List<TBGAwareCollector> collectors,
                                  int numHits, FieldDoc after,
                                  boolean fillFields,
                                  boolean trackDocScores,
                                  boolean trackMaxScore) {
        super(priorityQueue);
        this.collectorsIterator = collectors.listIterator();
        this.fillFields = fillFields;
        this.trackDocScores = trackMaxScore;
        this.trackMaxScore = trackMaxScore;
        this.numHits = numHits;
        this.after = after;
        this.initCollector = this.collectorsIterator.next();
    }

    public static TopDocsCollector<?> create(int numHits, List<TBGAwareCollector> collectors, FieldDoc after, boolean fillFields, boolean trackDocScores, boolean trackMaxScore) {
        return new SecondarySortCollector(null, collectors, numHits, after, fillFields, trackDocScores, trackMaxScore);
    }


    public TopDocs topDocs(int start, int howMany) {
        TopDocs topDocs = this.initCollector.topDocs(start, howMany);
        return rankDocs(topDocs, start, howMany, 1, this.initCollector);
    }

    protected TopDocs rankDocs(TopDocs topDocs, int start, int pageSize, int factoryCount, TBGAwareCollector currentCollector) {

        List<TieBreakerGroup> tbGroups = currentCollector.getTieBreakerGroups();

        if (null == tbGroups || tbGroups.isEmpty()) {
            return topDocs;
        }

        for (TieBreakerGroup tbGroup : tbGroups) {
            ScoreDoc[] scoreDocs = rankScoreDocs(tbGroup.getDocs(), start, pageSize, factoryCount, currentCollector);
            // set the correctly sorted docs back on the tieBreakerGroup object
            tbGroup.setDocs(Arrays.asList(scoreDocs));
        }

        // After the groups have been sorted, add them back to the original
        // TopDocs in sorted order
        mergeRankedTieBreakerGroups(topDocs, tbGroups);
        return topDocs;
    }

    public ScoreDoc[] rankScoreDocs(List<ScoreDoc> scoreDocs, int start, int pageSize, int factoryCount, TBGAwareCollector currentCollector) {
        // if there are tie breaker groups, set currentCollector to the next
        // collector that can be created from the list of collectors
        // and used that collector to break the ties
        currentCollector = getNextCollector();

        if (currentCollector == null) {
            return scoreDocs.toArray(new ScoreDoc[]{});
        }

        for (ScoreDoc scoreDoc : scoreDocs) {
            // use currCollector to collect the docs in the tieBreakerGroup
            int doc = scoreDoc.doc;
            try {
                LeafCollector currLeafCollector = currentCollector.getLeafCollector((this.singleLeafReaderContext != null) ? this.singleLeafReaderContext : this.contextRangeMap.get(doc));
                currLeafCollector.collect(doc);
            } catch (IOException e) {
            }
        }

        TopDocs tbTopDocs = currentCollector.topDocs(start, pageSize);
        TopDocs sortedDocs = rankDocs(tbTopDocs, start, pageSize, factoryCount + 1, currentCollector);
        return sortedDocs.scoreDocs;
    }

    /**
     * This method loops through each tieBreakerGroup and adds the hits in the
     * group back to the scoreDocs in the initial {@link TopDocs} in the
     * correctly sorted order. Sometimes the tieBreakerGroup may be larger than
     * the size of the documents that need to be replaced in the scoreDocs. This
     * will occur if a collector produced a TieBreakerGroup that falls at the
     * top or at the bottom of that page, and multiple documents outside the
     * paging had the same score. In this case a secondary sort was needed to
     * determine what actually falls within the page and the postion or start
     * valued in {@link TieBreakerGroup} is used to determine which documents
     * should be added back to the original topDocs.
     */
    protected void mergeRankedTieBreakerGroups(TopDocs topDocs, List<TieBreakerGroup> tbGroups) {
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        int start = 0;
        int end = scoreDocs.length - 1;

        for (TieBreakerGroup group : tbGroups) {
            int tbStart = group.getStart();
            int tbEnd = group.getDocs().size();
            int grpCounter = 0;

            // Make sure if start position is negative skip negative indexes and
            // increment the counter until you reach zero,
            // and, if the tieBreakerGroup size is larger the the size of the
            // scoreDocs, only add the values that will fit in the scoreDocs
            // appropriately
            while ((tbStart <= end) && (grpCounter < tbEnd)) {
                if ((tbStart >= start) && (tbStart <= end)) {
                    scoreDocs[tbStart] = group.getDocs().get(grpCounter);
                }
                grpCounter++;
                tbStart++;
            }
        }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (this.singleLeafReaderContext != null && this.contextRangeMap == null) {
            if (context.reader().leaves().size() == 1 && this.singleLeafReaderContext == null) {
                this.singleLeafReaderContext = context;
            } else if (context.reader().leaves().size() > 1) {
                ImmutableRangeMap.Builder<Integer, LeafReaderContext> builder = ImmutableRangeMap.builder();
                for (LeafReaderContext ctx : context.reader().leaves()) {
                    int lowerBound = ctx.docBase;
                    int upperBound = ctx.docBase + ctx.reader().maxDoc();
                    Range<Integer> range = Range.range(lowerBound, BoundType.CLOSED, upperBound, BoundType.OPEN);
                    builder.put(range, ctx);
                }
                this.contextRangeMap = builder.build();
            }
        }
        return initCollector.getLeafCollector(context);
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    private TBGAwareCollector getNextCollector() {
        if (!collectorsIterator.hasNext()) {
            return null;
        }

        TBGAwareCollector nextCollector = collectorsIterator.next();
        return nextCollector.create(this.numHits, this.fillFields, this.trackDocScores, this.trackMaxScore);
    }
}

