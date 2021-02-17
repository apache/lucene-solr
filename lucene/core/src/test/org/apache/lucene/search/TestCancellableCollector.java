package org.apache.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;

public class TestCancellableCollector extends LuceneTestCase {
    Directory dir;
    IndexReader reader;
    IndexSearcher searcher;
    ExecutorService executor = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();

        RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
        Random random = random();
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(newStringField("field", Integer.toString(i), Field.Store.NO));
            doc.add(newStringField("field2", Boolean.toString(i % 2 == 0), Field.Store.NO));
            doc.add(new SortedDocValuesField("field2", new BytesRef(Boolean.toString(i % 2 == 0))));
            iw.addDocument(doc);

            if (random.nextBoolean()) {
                iw.commit();
            }
        }
        reader = iw.getReader();
        iw.close();

        searcher = new IndexSearcher(reader);

        executor =  new ThreadPoolExecutor(
                4,
                4,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("TestIndexSearcher"));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
        dir.close();

        if (executor != null) {
            executor.shutdown();
        }

        executor = null;
    }

    private CancellableCollector buildCancellableCollector(final int numHits, boolean delayStart, boolean delayCollection) {
        TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(numHits, null, 1);
        CancellableCollector collector = new CancellableCollector(topScoreDocCollector);

        DummyCancellableCollector dummyCancellableCollector = new DummyCancellableCollector(collector, delayStart, delayCollection);

        return dummyCancellableCollector;
    }

    private void executeSearchTest(IndexSearcher searcher, Query query,
                                   CancellableCollector cancellableCollector, int numHits) throws Exception {
        TopDocs topDocs = searcher.search(query, numHits);

        searcher.search(query, cancellableCollector);

        CancellableCollector internalCancellableCollector = (CancellableCollector) cancellableCollector.getInternalCollector();
        TopScoreDocCollector topScoreDocCollector = (TopScoreDocCollector) internalCancellableCollector.getInternalCollector();

        assertEquals(topDocs.totalHits.value, topScoreDocCollector.totalHits);
    }

    private void cancelQuery(CancellableCollector cancellableCollector, final int sleepTime) {
        executor.submit(() -> {

            // Wait for some time to let the query start
            try {
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }

                cancellableCollector.cancelTask();
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
        });
    }

    public void testSearchWithoutCancellation() throws Exception {
        CancellableCollector cancellableCollector = buildCancellableCollector(50, false, false);

        Query query = new TermQuery(new Term("field", "1"));

        executeSearchTest(searcher, query, cancellableCollector, 50);

        query = new MatchAllDocsQuery();

        cancellableCollector = buildCancellableCollector(100, false, false);

        executeSearchTest(searcher, query, cancellableCollector, 50);
    }

    public void testSearchWithCancellationBeforeActualDocumentCollection() {
        Query query = new MatchAllDocsQuery();

        CancellableCollector cancellableCollector = buildCancellableCollector(5000, true, false);

        expectThrows(
                CancellableCollector.QueryCancelledException.class,
                () -> {
                    // Cancel the query before the document collection starts
                    cancelQuery(cancellableCollector, 0);

                    executeSearchTest(searcher, query, cancellableCollector, 5000);
                });
    }

    public void testSearchWithCancellationBetweenActualDocumentCollection() {
        Query query = new MatchAllDocsQuery();

        CancellableCollector cancellableCollector = buildCancellableCollector(5000, false, true);

        expectThrows(
                CancellableCollector.QueryCancelledException.class,
                () -> {
                    // Cancel the query before the document collection starts
                    cancelQuery(cancellableCollector, 0);

                    executeSearchTest(searcher, query, cancellableCollector, 5000);
                });
    }

    public class DummyCancellableCollector extends CancellableCollector {
        private CancellableCollector collector;
        private boolean delayStart;
        private boolean delayCollection;

        public DummyCancellableCollector(CancellableCollector cancellableCollector, boolean delayStart,
                                         boolean delayCollection) {
            super(cancellableCollector);

            this.collector = cancellableCollector;
            this.delayStart = delayStart;
            this.delayCollection = delayCollection;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

            if (delayStart) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }

            return new FilterLeafCollector(collector.getLeafCollector(context)) {

                @Override
                public void collect(int doc) throws IOException {
                    if (delayCollection) {
                        try {
                            Thread.sleep(30);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e.getMessage());
                        }
                    }

                    in.collect(doc);
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return collector.scoreMode();
        }

        @Override
        public void cancelTask() {
            collector.cancelTask();
        }
    }
}
