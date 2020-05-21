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

package org.apache.solr.search.facet;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageInputStreamImpl;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.spatial.prefix.HeatmapFacetCounter;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.AbstractSpatialPrefixTreeFieldType;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.RptWithGeometrySpatialField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SpatialRecursivePrefixTreeFieldType;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.DistanceUnits;
import org.apache.solr.util.SpatialUtils;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON Facet API request for a 2D spatial summary of a rectangular region.
 *
 * @see HeatmapFacetCounter
 * @version 7.5.0
 */
@SuppressWarnings("WeakerAccess")
public class FacetHeatmap extends FacetRequest {

  // note: much of this code was moved from SpatialHeatmapFacets (SimpleFacets API)

  /** @see org.apache.solr.common.params.FacetParams#FACET_HEATMAP_GEOM */
  public static final String GEOM_PARAM = "geom";

  /** @see org.apache.solr.common.params.FacetParams#FACET_HEATMAP_LEVEL */
  public static final String LEVEL_PARAM = "gridLevel";

  /** @see org.apache.solr.common.params.FacetParams#FACET_HEATMAP_DIST_ERR_PCT */
  public static final String DIST_ERR_PCT_PARAM = "distErrPct";

  /** @see org.apache.solr.common.params.FacetParams#FACET_HEATMAP_DIST_ERR */
  public static final String DIST_ERR_PARAM = "distErr";

  /** @see org.apache.solr.common.params.FacetParams#FACET_HEATMAP_MAX_CELLS */
  public static final String MAX_CELLS_PARAM = "maxCells";

  /** @see org.apache.solr.common.params.FacetParams#FACET_HEATMAP_FORMAT */
  public static final String FORMAT_PARAM = "format";

  public static final String FORMAT_PNG = "png";
  public static final String FORMAT_INTS2D = "ints2D";
  //note: if we change or add more formats, remember to update the javadoc on the format param
  //TODO for more format ideas, see formatCountsVal()

  public static final double DEFAULT_DIST_ERR_PCT = 0.15;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static class Parser extends FacetParser<FacetHeatmap> {
    @SuppressWarnings({"rawtypes"})
    Parser(FacetParser parent, String key) {
      super(parent, key);
    }

    public FacetHeatmap parse(Object argsObj) {
      assert facet == null;

      if (!(argsObj instanceof Map)) {
        throw err("Missing heatmap arguments");
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> argsMap = (Map<String, Object>) argsObj;
      String fieldName = getField(argsMap);

      //get the strategy from the field type
      final SchemaField schemaField = getSchema().getField(fieldName);
      final FieldType type = schemaField.getType();

      final PrefixTreeStrategy strategy;
      final DistanceUnits distanceUnits;
      // note: the two instanceof conditions is not ideal, versus one. If we start needing to add more then refactor.
      if ((type instanceof AbstractSpatialPrefixTreeFieldType)) {
        @SuppressWarnings({"rawtypes"})
        AbstractSpatialPrefixTreeFieldType rptType = (AbstractSpatialPrefixTreeFieldType) type;
        strategy = (PrefixTreeStrategy) rptType.getStrategy(fieldName);
        distanceUnits = rptType.getDistanceUnits();
      } else if (type instanceof RptWithGeometrySpatialField) {
        RptWithGeometrySpatialField rptSdvType  = (RptWithGeometrySpatialField) type;
        strategy = rptSdvType.getStrategy(fieldName).getIndexStrategy();
        distanceUnits = rptSdvType.getDistanceUnits();
      } else {
        //FYI we support the term query one too but few people use that one
        throw err("heatmap field needs to be of type " + SpatialRecursivePrefixTreeFieldType.class + " or " + RptWithGeometrySpatialField.class);
      }

      final SpatialContext ctx = strategy.getSpatialContext();

      //get the bbox (query Rectangle)
      String geomStr = getString(argsMap, GEOM_PARAM, null);
      final Shape boundsShape = geomStr == null ? ctx.getWorldBounds() : SpatialUtils.parseGeomSolrException(geomStr, ctx);

      //get the grid level (possibly indirectly via distErr or distErrPct)
      final int gridLevel;
      final Long gridLevelObj = getLongOrNull(argsMap, LEVEL_PARAM, false);
      final int maxGridLevel = strategy.getGrid().getMaxLevels();
      if (gridLevelObj != null) {
        gridLevel = gridLevelObj.intValue();
        if (gridLevel <= 0 || gridLevel > maxGridLevel) {
          throw err(LEVEL_PARAM +" should be > 0 and <= " + maxGridLevel);
        }
      } else {
        //SpatialArgs has utility methods to resolve a 'distErr' from optionally set distErr & distErrPct. Arguably that
        // should be refactored to feel less weird than using it like this.
        SpatialArgs spatialArgs = new SpatialArgs(SpatialOperation.Intersects/*ignored*/,
            boundsShape == null ? ctx.getWorldBounds() : boundsShape);
        final Double distErrObj = getDoubleOrNull(argsMap, DIST_ERR_PARAM, false);
        if (distErrObj != null) {
          // convert distErr units based on configured units
          spatialArgs.setDistErr(distErrObj * distanceUnits.multiplierFromThisUnitToDegrees());
        }
        spatialArgs.setDistErrPct(getDoubleOrNull(argsMap, DIST_ERR_PCT_PARAM, false));
        double distErr = spatialArgs.resolveDistErr(ctx, DEFAULT_DIST_ERR_PCT);
        if (distErr <= 0) {
          throw err(DIST_ERR_PCT_PARAM + " or " + DIST_ERR_PARAM
                  + " should be > 0 or instead provide " + LEVEL_PARAM + "=" + maxGridLevel
                  + " if you insist on maximum detail");
        }
        //The SPT (grid) can lookup a grid level satisfying an error distance constraint
        gridLevel = strategy.getGrid().getLevelForDistance(distErr);
      }

      final int maxCells = (int) getLong(argsMap, MAX_CELLS_PARAM, 100_000);// will throw later if exceeded

      final String format = getString(argsMap, FORMAT_PARAM, FORMAT_INTS2D);
      if (!format.equals(FORMAT_INTS2D) && !format.equals(FORMAT_PNG)) {
        throw err("format should be " + FORMAT_INTS2D + " or " + FORMAT_PNG);
      }

      this.facet = new FacetHeatmap(argsMap, strategy, boundsShape, gridLevel, maxCells, format);

      parseCommonParams(argsObj); // e.g. domain change

      return this.facet;
    }

  }//class Parser

  private final Map<String, Object> argsMap;
  private final PrefixTreeStrategy strategy;
  private final Shape boundsShape;
  private final int gridLevel;
  private final int maxCells;
  private final String format;

  FacetHeatmap(Map<String, Object> argsMap, PrefixTreeStrategy strategy, Shape boundsShape, int gridLevel, int maxCells, String format) {
    this.argsMap = argsMap;
    this.strategy = strategy;
    this.boundsShape = boundsShape;
    this.gridLevel = gridLevel;
    this.maxCells = maxCells;
    this.format = format;
  }

  //TODO perhaps all FacetRequest objs should have this?
  @Override
  public Map<String, Object> getFacetDescription() {
    return argsMap;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    return new FacetHeatmapProcessor(fcontext);
  }

  // don't use an anonymous class since the getSimpleName() isn't friendly in debug output
  @SuppressWarnings({"rawtypes"})
  private class FacetHeatmapProcessor extends FacetProcessor {
    @SuppressWarnings({"unchecked"})
    public FacetHeatmapProcessor(FacetContext fcontext) {
      super(fcontext, FacetHeatmap.this);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void process() throws IOException {
      super.process(); // handles domain changes

      //Compute!
      final HeatmapFacetCounter.Heatmap heatmap;
      try {
        heatmap = HeatmapFacetCounter.calcFacets(
            strategy,
            fcontext.searcher.getTopReaderContext(),
            getTopAcceptDocs(fcontext.base, fcontext.searcher), // turn DocSet into Bits
            boundsShape,
            gridLevel,
            maxCells);
      } catch (IllegalArgumentException e) {//e.g. too many cells
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.toString(), e);
      }

      //Populate response
      response = new SimpleOrderedMap<>();
      response.add("gridLevel", gridLevel);
      response.add("columns", heatmap.columns);
      response.add("rows", heatmap.rows);
      response.add("minX", heatmap.region.getMinX());
      response.add("maxX", heatmap.region.getMaxX());
      response.add("minY", heatmap.region.getMinY());
      response.add("maxY", heatmap.region.getMaxY());

      //A shard request will always be a PNG
      String format = fcontext.isShard() ? FORMAT_PNG : FacetHeatmap.this.format;

      response.add("counts_" + format, formatCountsVal(format, heatmap.columns, heatmap.rows, heatmap.counts, fcontext.getDebugInfo()));

      // note: we do not call processStats or processSubs as it's not supported yet
    }

    //TODO this is a general utility that should go elsewhere?  DocSetUtil?  Then should DocSetBase.getBits go away?
    private Bits getTopAcceptDocs(DocSet docSet, SolrIndexSearcher searcher) throws IOException {
      if (docSet.size() == searcher.numDocs()) {
        return null; // means match everything (all live docs). This can speedup things a lot.
      } else if (docSet.size() == 0) {
        return new Bits.MatchNoBits(searcher.maxDoc()); // can speedup things a lot
      } else if (docSet instanceof BitDocSet) {
        return ((BitDocSet) docSet).getBits();
      } else {
        // TODO DocSetBase.getBits ought to be at DocSet level?  Though it doesn't know maxDoc but it could?
        FixedBitSet bits = new FixedBitSet(searcher.maxDoc());
        for (DocIterator iter = docSet.iterator(); iter.hasNext();) {
          bits.set(iter.nextDoc());
        }
        return bits;
      }
    }

  }

  private static Object formatCountsVal(String format, int columns, int rows, int[] counts, FacetDebugInfo debugInfo) {
    if (counts == null) {
      return null;
    }
    boolean hasNonZero = false;
    for (int count : counts) {
      if (count > 0) {
        hasNonZero = true;
        break;
      }
    }
    if (!hasNonZero) {
      return null;
    }

    switch (format) {
      case FORMAT_INTS2D: //A List of List of Integers. Good for small heatmaps and ease of consumption
        return asInts2D(columns, rows, counts);
      case FORMAT_PNG: //A PNG graphic; compressed.  Good for large & dense heatmaps; hard to consume.
        return asPngBytes(columns, rows, counts, debugInfo);

      //TODO  case UTFGRID  https://github.com/mapbox/utfgrid-spec
      //TODO  case skipList: //A sequence of values; negative values are actually how many 0's to insert.
      //            Good for small or large but sparse heatmaps.
      //TODO    auto choose png or skipList; use skipList when < ~25% full or <= ~512 cells
      //  remember to augment error list below when we add more formats.
      default:
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown format: " + format);
    }
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetMerger() {
      NamedList<Object> mergedResult; // except counts, which we add in when done
      int[] counts;

      // note: there appears to be no mechanism to modify the shard requests in this API.  If we could, we'd
      //  change the format to png.  Instead, we have the facet processor recognize it's a shard request and ignore
      //  the requested format, which seems like a hack.

      @SuppressWarnings("unchecked")
      @Override
      public void merge(Object facetResult, Context mcontext) {
        NamedList<Object> facetResultNL = (NamedList<Object>) facetResult;
        counts = addPngToIntArray((byte[]) facetResultNL.remove("counts_" + FORMAT_PNG), counts);
        if (mergedResult == null) {
          mergedResult = facetResultNL;
        }
      }

      @Override
      public void finish(Context mcontext) {
        //nothing to do; we have no sub-facets
      }

      @Override
      public Object getMergedResult() {
        mergedResult.add("counts_" + format, formatCountsVal(
            format, (Integer) mergedResult.get("columns"), (Integer) mergedResult.get("rows"), counts, null));//TODO where debugInfo?
        return mergedResult;
      }
    };
  }

  @VisibleForTesting
  static int[] addPngToIntArray(byte[] pngBytes, int[] counts) {
    if (pngBytes == null) {
      return counts;
    }
    //read PNG
    final BufferedImage image = PngHelper.readImage(pngBytes);
    int columns = image.getWidth();
    int rows = image.getHeight();
    if (counts == null) {
      counts = new int[columns * rows];
    } else {
      assert counts.length == columns * rows;
    }
    for (int c = 0; c < columns; c++) {
      for (int r = 0; r < rows; r++) {
        counts[c * rows + r] += PngHelper.getCountAtColumnRow(image, rows, c, r);
      }
    }
    return counts;
  }

  @VisibleForTesting
  static List<List<Integer>> asInts2D(final int columns, final int rows, final int[] counts) {
    //Returns a view versus returning a copy. This saves memory.
    //The data is oriented naturally for human/developer viewing: one row at a time top-down
    return new AbstractList<List<Integer>>() {
      @Override
      public List<Integer> get(final int rowIdx) {//top-down remember; the heatmap.counts is bottom up
        //check if all zeroes and return null if so
        boolean hasNonZero = false;
        int y = rows - rowIdx - 1;//flip direction for 'y'
        for (int c = 0; c < columns; c++) {
          if (counts[c * rows + y] > 0) {
            hasNonZero = true;
            break;
          }
        }
        if (!hasNonZero) {
          return null;
        }

        return new AbstractList<Integer>() {
          @Override
          public Integer get(int columnIdx) {
            return counts[columnIdx * rows + y];
          }

          @Override
          public int size() {
            return columns;
          }
        };
      }

      @Override
      public int size() {
        return rows;
      }
    };
  }

  @VisibleForTesting
  static byte[] asPngBytes(final int columns, final int rows, final int[] counts, FacetDebugInfo debugInfo) {
    long startTimeNano = System.nanoTime();
    BufferedImage image = PngHelper.newImage(columns, rows);
    for (int c = 0; c < columns; c++) {
      for (int r = 0; r < rows; r++) {
        PngHelper.writeCountAtColumnRow(image, rows, c, r, counts[c * rows + r]);
      }
    }
    byte[] bytes = PngHelper.writeImage(image);
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNano);
    if (log.isDebugEnabled()) {
      log.debug("heatmap nativeSize={} pngSize={} pngTime={}", (counts.length * 4), bytes.length, durationMs);
    }
    if (debugInfo != null) {
      debugInfo.putInfoItem("heatmap png timing", durationMs);
    }
    return bytes;
  }

  @VisibleForTesting
  static class PngHelper {

    static final ImageReaderSpi imageReaderSpi;//thread-safe
    static {
      final Iterator<ImageReader> imageReaders = ImageIO.getImageReadersByFormatName("png");
      if (!imageReaders.hasNext()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't find png image reader, neaded for heatmaps!");
      }
      ImageReader imageReader = imageReaders.next();
      imageReaderSpi = imageReader.getOriginatingProvider();
    }

    static BufferedImage readImage(final byte[] bytes) {
      // Wrap ImageInputStream around the bytes.  We could use MemoryCacheImageInputStream but it will
      // cache the data which is quite unnecessary given we have it all in-memory already.
      ImageInputStream imageInputStream = new ImageInputStreamImpl() {
        //TODO re-use this instance; superclass has 8KB buffer.

        @Override
        public int read() throws IOException {
          checkClosed();
          bitOffset = 0;
          if (streamPos >= bytes.length) {
            return -1;
          } else {
            return bytes[(int) streamPos++];
          }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          checkClosed();
          bitOffset = 0;
          if (streamPos >= bytes.length) {
            return -1;
          } else {
            int copyLen = Math.min(len, bytes.length - (int)streamPos);
            System.arraycopy(bytes, (int)streamPos, b, off, copyLen);
            streamPos += copyLen;
            return copyLen;
          }
        }

        @Override
        public long length() {
          return bytes.length;
        }

        @Override
        public boolean isCached() {
          return true;
        }

        @Override
        public boolean isCachedMemory() {
          return true;
        }
      };
      try {
        //TODO can/should we re-use an imageReader instance on FacetInfo?
        ImageReader imageReader = imageReaderSpi.createReaderInstance();

        imageReader.setInput(imageInputStream,
            false,//forwardOnly
            true);//ignoreMetadata
        return imageReader.read(0);//read first & only image
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Problem reading png heatmap: " + e);
      }
    }

    static byte[] writeImage(BufferedImage image) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(
          // initialize to roughly 1/4th the size a native int would take per-pixel
          image.getWidth() * image.getHeight() + 1024
      );
      try {
        ImageIO.write(image, FORMAT_PNG, baos);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "While generating PNG: " + e);
      }
      //too bad we can't access the raw byte[]; this copies to a new one
      return baos.toByteArray();
    }

    // We abuse the image for storing integers (4 bytes), and so we need a 4-byte ABGR.
    // first (low) byte is blue, next byte is green, next byte red, and last (high) byte is alpha.
    static BufferedImage newImage(int columns, int rows) {
      return new BufferedImage(columns, rows, BufferedImage.TYPE_4BYTE_ABGR);
    }

    // 'y' dimension goes top-down, so invert.
    // Alpha chanel is high byte; 0 means transparent. So XOR those bits with '1' so that we need
    //  to have counts > 16M before the picture starts to fade

    static void writeCountAtColumnRow(BufferedImage image, int rows, int c, int r, int val) {
      image.setRGB(c, rows - 1 - r, val ^ 0xFF_00_00_00);
    }

    static int getCountAtColumnRow(BufferedImage image, int rows, int c, int r) {
      return image.getRGB(c, rows - 1 - r) ^ 0xFF_00_00_00;
    }

  }

}
