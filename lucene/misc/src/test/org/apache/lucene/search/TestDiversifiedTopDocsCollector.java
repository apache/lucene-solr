/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.LegacyFloatField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Demonstrates an application of the {@link DiversifiedTopDocsCollector} in
 * assembling a collection of top results but without over-representation of any
 * one source (in this case top-selling singles from the 60s without having them
 * all be Beatles records...). Results are ranked by the number of weeks a
 * single is top of the charts and de-duped by the artist name.
 * 
 */
public class TestDiversifiedTopDocsCollector extends LuceneTestCase {

  public void testNonDiversifiedResults() throws Exception {
    int numberOfTracksOnCompilation = 10;
    int expectedMinNumOfBeatlesHits = 5;
    TopDocs res = searcher.search(getTestQuery(), numberOfTracksOnCompilation);
    assertEquals(numberOfTracksOnCompilation, res.scoreDocs.length);
    // due to randomization of segment merging in tests the exact number of Beatles hits 
    // selected varies between 5 and 6 but we prove the point they are over-represented
    // in our result set using a standard search.
    assertTrue(getMaxNumRecordsPerArtist(res.scoreDocs) >= expectedMinNumOfBeatlesHits);
  }

  public void testFirstPageDiversifiedResults() throws Exception {
    // Using a diversified collector we can limit the results from
    // any one artist.
    int requiredMaxHitsPerArtist = 2;
    int numberOfTracksOnCompilation = 10;
    DiversifiedTopDocsCollector tdc = doDiversifiedSearch(
        numberOfTracksOnCompilation, requiredMaxHitsPerArtist);
    ScoreDoc[] sd = tdc.topDocs(0).scoreDocs;
    assertEquals(numberOfTracksOnCompilation, sd.length);
    assertTrue(getMaxNumRecordsPerArtist(sd) <= requiredMaxHitsPerArtist);
  }

  public void testSecondPageResults() throws Exception {
    int numberOfTracksPerCompilation = 10;
    int numberOfCompilations = 2;
    int requiredMaxHitsPerArtist = 1;

    // Volume 2 of our hits compilation - start at position 10
    DiversifiedTopDocsCollector tdc = doDiversifiedSearch(
        numberOfTracksPerCompilation * numberOfCompilations,
        requiredMaxHitsPerArtist);
    ScoreDoc[] volume2 = tdc.topDocs(numberOfTracksPerCompilation,
        numberOfTracksPerCompilation).scoreDocs;
    assertEquals(numberOfTracksPerCompilation, volume2.length);
    assertTrue(getMaxNumRecordsPerArtist(volume2) <= requiredMaxHitsPerArtist);

  }

  public void testInvalidArguments() throws Exception {
    int numResults = 5;
    DiversifiedTopDocsCollector tdc = doDiversifiedSearch(numResults, 15);

    // start < 0
    assertEquals(0, tdc.topDocs(-1).scoreDocs.length);

    // start > pq.size()
    assertEquals(0, tdc.topDocs(numResults + 1).scoreDocs.length);

    // start == pq.size()
    assertEquals(0, tdc.topDocs(numResults).scoreDocs.length);

    // howMany < 0
    assertEquals(0, tdc.topDocs(0, -1).scoreDocs.length);

    // howMany == 0
    assertEquals(0, tdc.topDocs(0, 0).scoreDocs.length);

  }

  // Diversifying collector that looks up de-dup keys using SortedDocValues
  // from a top-level Reader
  private static final class DocValuesDiversifiedCollector extends
      DiversifiedTopDocsCollector {
    private final SortedDocValues sdv;

    public DocValuesDiversifiedCollector(int size, int maxHitsPerKey,
        SortedDocValues sdv) {
      super(size, maxHitsPerKey);
      this.sdv = sdv;
    }

    @Override
    protected NumericDocValues getKeys(final LeafReaderContext context) {

      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          // Keys are always expressed as a long so we obtain the
          // ordinal for our String-based artist name here
          return sdv.getOrd(context.docBase + docID);
        }
      };
    }
  }

  // Alternative, faster implementation for converting String keys to longs
  // but with the potential for hash collisions
  private static final class HashedDocValuesDiversifiedCollector extends
      DiversifiedTopDocsCollector {

    private final String field;
    private BinaryDocValues vals;

    public HashedDocValuesDiversifiedCollector(int size, int maxHitsPerKey,
        String field) {
      super(size, maxHitsPerKey);
      this.field = field;
    }

    @Override
    protected NumericDocValues getKeys(LeafReaderContext context) {
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return vals == null ? -1 : vals.get(docID).hashCode();
        }
      };
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context)
        throws IOException {
      this.vals = DocValues.getBinary(context.reader(), field);
      return super.getLeafCollector(context);
    }
  }

  // Test data - format is artist, song, weeks at top of charts
  private static String[] hitsOfThe60s = {
      "1966\tSPENCER DAVIS GROUP\tKEEP ON RUNNING\t1",
      "1966\tOVERLANDERS\tMICHELLE\t3",
      "1966\tNANCY SINATRA\tTHESE BOOTS ARE MADE FOR WALKIN'\t4",
      "1966\tWALKER BROTHERS\tTHE SUN AIN'T GONNA SHINE ANYMORE\t4",
      "1966\tSPENCER DAVIS GROUP\tSOMEBODY HELP ME\t2",
      "1966\tDUSTY SPRINGFIELD\tYOU DON'T HAVE TO SAY YOU LOVE ME\t1",
      "1966\tMANFRED MANN\tPRETTY FLAMINGO\t3",
      "1966\tROLLING STONES\tPAINT IT, BLACK\t1",
      "1966\tFRANK SINATRA\tSTRANGERS IN THE NIGHT\t3",
      "1966\tBEATLES\tPAPERBACK WRITER\t5",
      "1966\tKINKS\tSUNNY AFTERNOON\t2",
      "1966\tGEORGIE FAME AND THE BLUE FLAMES\tGETAWAY\t1",
      "1966\tCHRIS FARLOWE\tOUT OF TIME\t1",
      "1966\tTROGGS\tWITH A GIRL LIKE YOU\t2",
      "1966\tBEATLES\tYELLOW SUBMARINE/ELEANOR RIGBY\t4",
      "1966\tSMALL FACES\tALL OR NOTHING\t1",
      "1966\tJIM REEVES\tDISTANT DRUMS\t5",
      "1966\tFOUR TOPS\tREACH OUT I'LL BE THERE\t3",
      "1966\tBEACH BOYS\tGOOD VIBRATIONS\t2",
      "1966\tTOM JONES\tGREEN GREEN GRASS OF HOME\t4",
      "1967\tMONKEES\tI'M A BELIEVER\t4",
      "1967\tPETULA CLARK\tTHIS IS MY SONG\t2",
      "1967\tENGELBERT HUMPERDINCK\tRELEASE ME\t4",
      "1967\tNANCY SINATRA AND FRANK SINATRA\tSOMETHIN' STUPID\t2",
      "1967\tSANDIE SHAW\tPUPPET ON A STRING\t3",
      "1967\tTREMELOES\tSILENCE IS GOLDEN\t3",
      "1967\tPROCOL HARUM\tA WHITER SHADE OF PALE\t4",
      "1967\tBEATLES\tALL YOU NEED IS LOVE\t7",
      "1967\tSCOTT MCKENZIE\tSAN FRANCISCO (BE SURE TO WEAR SOME FLOWERS INYOUR HAIR)\t4",
      "1967\tENGELBERT HUMPERDINCK\tTHE LAST WALTZ\t5",
      "1967\tBEE GEES\tMASSACHUSETTS (THE LIGHTS WENT OUT IN)\t4",
      "1967\tFOUNDATIONS\tBABY NOW THAT I'VE FOUND YOU\t2",
      "1967\tLONG JOHN BALDRY\tLET THE HEARTACHES BEGIN\t2",
      "1967\tBEATLES\tHELLO GOODBYE\t5",
      "1968\tGEORGIE FAME\tTHE BALLAD OF BONNIE AND CLYDE\t1",
      "1968\tLOVE AFFAIR\tEVERLASTING LOVE\t2",
      "1968\tMANFRED MANN\tMIGHTY QUINN\t2",
      "1968\tESTHER AND ABI OFARIM\tCINDERELLA ROCKEFELLA\t3",
      "1968\tDAVE DEE, DOZY, BEAKY, MICK AND TICH\tTHE LEGEND OF XANADU\t1",
      "1968\tBEATLES\tLADY MADONNA\t2",
      "1968\tCLIFF RICHARD\tCONGRATULATIONS\t2",
      "1968\tLOUIS ARMSTRONG\tWHAT A WONDERFUL WORLD/CABARET\t4",
      "1968\tGARRY PUCKETT AND THE UNION GAP\tYOUNG GIRL\t4",
      "1968\tROLLING STONES\tJUMPING JACK FLASH\t2",
      "1968\tEQUALS\tBABY COME BACK\t3", "1968\tDES O'CONNOR\tI PRETEND\t1",
      "1968\tTOMMY JAMES AND THE SHONDELLS\tMONY MONY\t2",
      "1968\tCRAZY WORLD OF ARTHUR BROWN\tFIRE!\t1",
      "1968\tTOMMY JAMES AND THE SHONDELLS\tMONY MONY\t1",
      "1968\tBEACH BOYS\tDO IT AGAIN\t1",
      "1968\tBEE GEES\tI'VE GOTTA GET A MESSAGE TO YOU\t1",
      "1968\tBEATLES\tHEY JUDE\t8",
      "1968\tMARY HOPKIN\tTHOSE WERE THE DAYS\t6",
      "1968\tJOE COCKER\tWITH A LITTLE HELP FROM MY FRIENDS\t1",
      "1968\tHUGO MONTENEGRO\tTHE GOOD THE BAD AND THE UGLY\t4",
      "1968\tSCAFFOLD\tLILY THE PINK\t3",
      "1969\tMARMALADE\tOB-LA-DI, OB-LA-DA\t1",
      "1969\tSCAFFOLD\tLILY THE PINK\t1",
      "1969\tMARMALADE\tOB-LA-DI, OB-LA-DA\t2",
      "1969\tFLEETWOOD MAC\tALBATROSS\t1", "1969\tMOVE\tBLACKBERRY WAY\t1",
      "1969\tAMEN CORNER\t(IF PARADISE IS) HALF AS NICE\t2",
      "1969\tPETER SARSTEDT\tWHERE DO YOU GO TO (MY LOVELY)\t4",
      "1969\tMARVIN GAYE\tI HEARD IT THROUGH THE GRAPEVINE\t3",
      "1969\tDESMOND DEKKER AND THE ACES\tTHE ISRAELITES\t1",
      "1969\tBEATLES\tGET BACK\t6", "1969\tTOMMY ROE\tDIZZY\t1",
      "1969\tBEATLES\tTHE BALLAD OF JOHN AND YOKO\t3",
      "1969\tTHUNDERCLAP NEWMAN\tSOMETHING IN THE AIR\t3",
      "1969\tROLLING STONES\tHONKY TONK WOMEN\t5",
      "1969\tZAGER AND EVANS\tIN THE YEAR 2525 (EXORDIUM AND TERMINUS)\t3",
      "1969\tCREEDENCE CLEARWATER REVIVAL\tBAD MOON RISING\t3",
      "1969\tJANE BIRKIN AND SERGE GAINSBOURG\tJE T'AIME... MOI NON PLUS\t1",
      "1969\tBOBBIE GENTRY\tI'LL NEVER FALL IN LOVE AGAIN\t1",
      "1969\tARCHIES\tSUGAR, SUGAR\t4" };

  private static final Map<String, Record> parsedRecords = new HashMap<String, Record>();
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
  private SortedDocValues artistDocValues;

  static class Record {
    String year;
    String artist;
    String song;
    float weeks;
    String id;

    public Record(String id, String year, String artist, String song,
        float weeks) {
      super();
      this.id = id;
      this.year = year;
      this.artist = artist;
      this.song = song;
      this.weeks = weeks;
    }

    @Override
    public String toString() {
      return "Record [id=" + id + ", artist=" + artist + ", weeks=" + weeks
          + ", year=" + year + ", song=" + song + "]";
    }

  }

  private DiversifiedTopDocsCollector doDiversifiedSearch(int numResults,
      int maxResultsPerArtist) throws IOException {
    // Alternate between implementations used for key lookups 
    if (random().nextBoolean()) {
      // Faster key lookup but with potential for collisions on larger datasets
      return doFuzzyDiversifiedSearch(numResults, maxResultsPerArtist);
    } else {
      // Slower key lookup but 100% accurate
      return doAccurateDiversifiedSearch(numResults, maxResultsPerArtist);
    }
  }

  private DiversifiedTopDocsCollector doFuzzyDiversifiedSearch(int numResults,
      int maxResultsPerArtist) throws IOException {
    DiversifiedTopDocsCollector tdc = new HashedDocValuesDiversifiedCollector(
        numResults, maxResultsPerArtist, "artist");
    searcher.search(getTestQuery(), tdc);
    return tdc;
  }

  private DiversifiedTopDocsCollector doAccurateDiversifiedSearch(
      int numResults, int maxResultsPerArtist) throws IOException {
    DiversifiedTopDocsCollector tdc = new DocValuesDiversifiedCollector(
        numResults, maxResultsPerArtist, artistDocValues);
    searcher.search(getTestQuery(), tdc);
    return tdc;
  }

  private Query getTestQuery() {
    BooleanQuery.Builder testQuery = new BooleanQuery.Builder();
    testQuery.add(new BooleanClause(new TermQuery(new Term("year", "1966")),
        Occur.SHOULD));
    testQuery.add(new BooleanClause(new TermQuery(new Term("year", "1967")),
        Occur.SHOULD));
    testQuery.add(new BooleanClause(new TermQuery(new Term("year", "1968")),
        Occur.SHOULD));
    testQuery.add(new BooleanClause(new TermQuery(new Term("year", "1969")),
        Occur.SHOULD));
    return testQuery.build();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // populate an index with documents - artist, song and weeksAtNumberOne
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();

    Field yearField = newTextField("year", "", Field.Store.NO);
    SortedDocValuesField artistField = new SortedDocValuesField("artist",
        new BytesRef(""));
    Field weeksAtNumberOneField = new FloatDocValuesField("weeksAtNumberOne",
        0.0F);
    Field weeksStoredField = new LegacyFloatField("weeks", 0.0F, Store.YES);
    Field idField = newStringField("id", "", Field.Store.YES);
    Field songField = newTextField("song", "", Field.Store.NO);
    Field storedArtistField = newTextField("artistName", "", Field.Store.NO);

    doc.add(idField);
    doc.add(weeksAtNumberOneField);
    doc.add(storedArtistField);
    doc.add(songField);
    doc.add(weeksStoredField);
    doc.add(yearField);
    doc.add(artistField);

    parsedRecords.clear();
    for (int i = 0; i < hitsOfThe60s.length; i++) {
      String cols[] = hitsOfThe60s[i].split("\t");
      Record record = new Record(String.valueOf(i), cols[0], cols[1], cols[2],
          Float.parseFloat(cols[3]));
      parsedRecords.put(record.id, record);
      idField.setStringValue(record.id);
      yearField.setStringValue(record.year);
      storedArtistField.setStringValue(record.artist);
      artistField.setBytesValue(new BytesRef(record.artist));
      songField.setStringValue(record.song);
      weeksStoredField.setFloatValue(record.weeks);
      weeksAtNumberOneField.setFloatValue(record.weeks);
      writer.addDocument(doc);
      if (i % 10 == 0) {
        // Causes the creation of multiple segments for our test
        writer.commit();
      }
    }
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
    artistDocValues = MultiDocValues.getSortedValues(reader, "artist");

    // All searches sort by song popularity 
    final Similarity base = searcher.getSimilarity(true);
    searcher.setSimilarity(new DocValueSimilarity(base, "weeksAtNumberOne"));
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    dir = null;
    super.tearDown();
  }

  private int getMaxNumRecordsPerArtist(ScoreDoc[] sd) throws IOException {
    int result = 0;
    HashMap<String, Integer> artistCounts = new HashMap<String, Integer>();
    for (int i = 0; i < sd.length; i++) {
      Document doc = reader.document(sd[i].doc);
      Record record = parsedRecords.get(doc.get("id"));
      Integer count = artistCounts.get(record.artist);
      int newCount = 1;
      if (count != null) {
        newCount = count.intValue() + 1;
      }
      result = Math.max(result, newCount);
      artistCounts.put(record.artist, newCount);
    }
    return result;
  }

  /**
   * Similarity that wraps another similarity and replaces the final score
   * according to whats in a docvalues field.
   * 
   * @lucene.experimental
   */
  static class DocValueSimilarity extends Similarity {
    private final Similarity sim;
    private final String scoreValueField;

    public DocValueSimilarity(Similarity sim, String scoreValueField) {
      this.sim = sim;
      this.scoreValueField = scoreValueField;
    }

    @Override
    public long computeNorm(FieldInvertState state) {
      return sim.computeNorm(state);
    }

    @Override
    public SimWeight computeWeight(
        CollectionStatistics collectionStats, TermStatistics... termStats) {
      return sim.computeWeight(collectionStats, termStats);
    }

    @Override
    public SimScorer simScorer(SimWeight stats, LeafReaderContext context)
        throws IOException {
      final SimScorer sub = sim.simScorer(stats, context);
      final NumericDocValues values = DocValues.getNumeric(context.reader(),
          scoreValueField);

      return new SimScorer() {
        @Override
        public float score(int doc, float freq) {
          return Float.intBitsToFloat((int) values.get(doc));
        }

        @Override
        public float computeSlopFactor(int distance) {
          return sub.computeSlopFactor(distance);
        }

        @Override
        public float computePayloadFactor(int doc, int start, int end,
            BytesRef payload) {
          return sub.computePayloadFactor(doc, start, end, payload);
        }

        @Override
        public Explanation explain(int doc, Explanation freq) {
          return Explanation.match(Float.intBitsToFloat((int) values.get(doc)),
              "indexDocValue(" + scoreValueField + ")");
        }
      };
    }
  }

}
