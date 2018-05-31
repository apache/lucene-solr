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
package org.apache.solr.handler.component;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.spatial.prefix.HeatmapFacetCounter;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
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

/** A 2D spatial faceting summary of a rectangular region. Used by {@link org.apache.solr.handler.component.FacetComponent}
 * and {@link org.apache.solr.request.SimpleFacets}. */
public class SpatialHeatmapFacets {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  //underneath facet_counts we put this here:
  public static final String RESPONSE_KEY = "facet_heatmaps";

  public static final String FORMAT_PNG = "png";
  public static final String FORMAT_INTS2D = "ints2D";
  //note: if we change or add more formats, remember to update the javadoc on the format param
  //TODO for more format ideas, see formatCountsAndAddToNL

  public static final double DEFAULT_DIST_ERR_PCT = 0.15;

  /** Called by {@link org.apache.solr.request.SimpleFacets} to compute heatmap facets. */
  public static NamedList<Object> getHeatmapForField(String fieldKey, String fieldName, ResponseBuilder rb, SolrParams params, DocSet docSet) throws IOException {
    //get the strategy from the field type
    final SchemaField schemaField = rb.req.getSchema().getField(fieldName);
    final FieldType type = schemaField.getType();

    final PrefixTreeStrategy strategy;
    final DistanceUnits distanceUnits;
    // note: the two instanceof conditions is not ideal, versus one. If we start needing to add more then refactor.
    if ((type instanceof AbstractSpatialPrefixTreeFieldType)) {
      AbstractSpatialPrefixTreeFieldType rptType = (AbstractSpatialPrefixTreeFieldType) type;
      strategy = (PrefixTreeStrategy) rptType.getStrategy(fieldName);
      distanceUnits = rptType.getDistanceUnits();
    } else if (type instanceof RptWithGeometrySpatialField) {
      RptWithGeometrySpatialField rptSdvType  = (RptWithGeometrySpatialField) type;
      strategy = rptSdvType.getStrategy(fieldName).getIndexStrategy();
      distanceUnits = rptSdvType.getDistanceUnits();
    } else {
      //FYI we support the term query one too but few people use that one
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "heatmap field needs to be of type "
          + SpatialRecursivePrefixTreeFieldType.class + " or " + RptWithGeometrySpatialField.class);
    }

    final SpatialContext ctx = strategy.getSpatialContext();

    //get the bbox (query Rectangle)
    String geomStr = params.getFieldParam(fieldKey, FacetParams.FACET_HEATMAP_GEOM);
    final Shape boundsShape = geomStr == null ? ctx.getWorldBounds() : SpatialUtils.parseGeomSolrException(geomStr, ctx);

    //get the grid level (possibly indirectly via distErr or distErrPct)
    final int gridLevel;
    Integer gridLevelObj = params.getFieldInt(fieldKey, FacetParams.FACET_HEATMAP_LEVEL);
    final int maxGridLevel = strategy.getGrid().getMaxLevels();
    if (gridLevelObj != null) {
      gridLevel = gridLevelObj;
      if (gridLevel <= 0 || gridLevel > maxGridLevel) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            FacetParams.FACET_HEATMAP_LEVEL +" should be > 0 and <= " + maxGridLevel);
      }
    } else {
      //SpatialArgs has utility methods to resolve a 'distErr' from optionally set distErr & distErrPct. Arguably that
      // should be refactored to feel less weird than using it like this.
      SpatialArgs spatialArgs = new SpatialArgs(SpatialOperation.Intersects/*ignored*/,
          boundsShape == null ? ctx.getWorldBounds() : boundsShape);
      final Double distErrObj = params.getFieldDouble(fieldKey, FacetParams.FACET_HEATMAP_DIST_ERR);
      if (distErrObj != null) {
        // convert distErr units based on configured units
        spatialArgs.setDistErr(distErrObj * distanceUnits.multiplierFromThisUnitToDegrees());
      }
      spatialArgs.setDistErrPct(params.getFieldDouble(fieldKey, FacetParams.FACET_HEATMAP_DIST_ERR_PCT));
      double distErr = spatialArgs.resolveDistErr(ctx, DEFAULT_DIST_ERR_PCT);
      if (distErr <= 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            FacetParams.FACET_HEATMAP_DIST_ERR_PCT + " or " + FacetParams.FACET_HEATMAP_DIST_ERR
                + " should be > 0 or instead provide " + FacetParams.FACET_HEATMAP_LEVEL + "=" + maxGridLevel
                + " if you insist on maximum detail");
      }
      //The SPT (grid) can lookup a grid level satisfying an error distance constraint
      gridLevel = strategy.getGrid().getLevelForDistance(distErr);
    }

    //Compute!
    final HeatmapFacetCounter.Heatmap heatmap;
    try {
      heatmap = HeatmapFacetCounter.calcFacets(
          strategy,
          rb.req.getSearcher().getTopReaderContext(),
          getTopAcceptDocs(docSet, rb.req.getSearcher()), // turn DocSet into Bits
          boundsShape,
          gridLevel,
          params.getFieldInt(fieldKey, FacetParams.FACET_HEATMAP_MAX_CELLS, 100_000) // will throw if exceeded
      );
    } catch (IllegalArgumentException e) {//e.g. too many cells
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.toString(), e);
    }

    //Populate response
    NamedList<Object> result = new NamedList<>();
    result.add("gridLevel", gridLevel);
    result.add("columns", heatmap.columns);
    result.add("rows", heatmap.rows);
    result.add("minX", heatmap.region.getMinX());
    result.add("maxX", heatmap.region.getMaxX());
    result.add("minY", heatmap.region.getMinY());
    result.add("maxY", heatmap.region.getMaxY());

    boolean hasNonZero = false;
    for (int count : heatmap.counts) {
      if (count > 0) {
        hasNonZero = true;
        break;
      }
    }
    formatCountsAndAddToNL(fieldKey, rb, params, heatmap.columns, heatmap.rows, hasNonZero ? heatmap.counts : null, result);

    return result;
  }

  private static Bits getTopAcceptDocs(DocSet docSet, SolrIndexSearcher searcher) throws IOException {
    if (searcher.getLiveDocSet() == docSet) {
      return null; // means match everything (all live docs). This can speedup things a lot.
    } else if (docSet.size() == 0) {
      return new Bits.MatchNoBits(searcher.maxDoc()); // can speedup things a lot
    } else if (docSet instanceof BitDocSet) {
      return ((BitDocSet) docSet).getBits();
    } else {
      // TODO DocSetBase.calcBits ought to be at DocSet level?
      FixedBitSet bits = new FixedBitSet(searcher.maxDoc());
      for (DocIterator iter = docSet.iterator(); iter.hasNext();) {
        bits.set(iter.nextDoc());
      }
      return bits;
    }
  }

  private static void formatCountsAndAddToNL(String fieldKey, ResponseBuilder rb, SolrParams params,
                                             int columns, int rows, int[] counts, NamedList<Object> result) {
    final String format = params.getFieldParam(fieldKey, FacetParams.FACET_HEATMAP_FORMAT, FORMAT_INTS2D);
    final Object countsVal;
    switch (format) {
      case FORMAT_INTS2D: //A List of List of Integers. Good for small heatmaps and ease of consumption
        countsVal = counts != null ? asInts2D(columns, rows, counts) : null;
        break;
      case FORMAT_PNG: //A PNG graphic; compressed.  Good for large & dense heatmaps; hard to consume.
        countsVal = counts != null ? asPngBytes(columns, rows, counts, rb) : null;
        break;
      //TODO  case skipList: //A sequence of values; negative values are actually how many 0's to insert.
      //            Good for small or large but sparse heatmaps.
      //TODO    auto choose png or skipList; use skipList when < ~25% full or <= ~512 cells
      //  remember to augment error list below when we add more formats.
      default:
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "format should be " + FORMAT_INTS2D + " or " + FORMAT_PNG);
    }
    result.add("counts_" + format, countsVal);
  }

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

  //package access for tests
  static byte[] asPngBytes(final int columns, final int rows, final int[] counts, ResponseBuilder rb) {
    long startTimeNano = System.nanoTime();
      BufferedImage image = PngHelper.newImage(columns, rows);
      for (int c = 0; c < columns; c++) {
        for (int r = 0; r < rows; r++) {
          PngHelper.writeCountAtColumnRow(image, rows, c, r, counts[c * rows + r]);
        }
      }
      byte[] bytes = PngHelper.writeImage(image);
      long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNano);
      log.debug("heatmap nativeSize={} pngSize={} pngTime={}", (counts.length * 4), bytes.length, durationMs);
      if (rb != null && rb.isDebugTimings()) {
        rb.addDebug(durationMs, "timing", "heatmap png generation");
      }
      return bytes;
  }

  //
  // Distributed Support
  //

  /** Parses request to "HeatmapFacet" instances. */
  public static LinkedHashMap<String,HeatmapFacet> distribParse(SolrParams params, ResponseBuilder rb) {
    final LinkedHashMap<String, HeatmapFacet> heatmapFacets = new LinkedHashMap<>();
    final String[] heatmapFields = params.getParams(FacetParams.FACET_HEATMAP);
    if (heatmapFields != null) {
      for (String heatmapField : heatmapFields) {
        HeatmapFacet facet = new HeatmapFacet(rb, heatmapField);
        heatmapFacets.put(facet.getKey(), facet);
      }
    }
    return heatmapFacets;
  }

  /** Called by FacetComponent's impl of
   * {@link org.apache.solr.handler.component.SearchComponent#modifyRequest(ResponseBuilder, SearchComponent, ShardRequest)}. */
  public static void distribModifyRequest(ShardRequest sreq, LinkedHashMap<String, HeatmapFacet> heatmapFacets) {
    // Set the format to PNG because it's compressed and it's the only format we have code to read at the moment.
    // We re-write the facet.heatmap list with PNG format in local-params where it has highest precedence.

    //Remove existing heatmap field param vals; we will rewrite
    sreq.params.remove(FacetParams.FACET_HEATMAP);
    for (HeatmapFacet facet : heatmapFacets.values()) {
      //add heatmap field param
      ModifiableSolrParams newLocalParams = new ModifiableSolrParams();
      if (facet.localParams != null) {
        newLocalParams.add(facet.localParams);
      }
      // Set format to PNG; it's the only one we parse
      newLocalParams.set(FacetParams.FACET_HEATMAP_FORMAT, FORMAT_PNG);
      sreq.params.add(FacetParams.FACET_HEATMAP,
          newLocalParams.toLocalParamsString() + facet.facetOn);
    }
  }

  /** Called by FacetComponent.countFacets which is in turn called by FC's impl of
   * {@link org.apache.solr.handler.component.SearchComponent#handleResponses(ResponseBuilder, ShardRequest)}. */
  @SuppressWarnings("unchecked")
  public static void distribHandleResponse(LinkedHashMap<String, HeatmapFacet> heatmapFacets, NamedList srsp_facet_counts) {
    NamedList<NamedList<Object>> facet_heatmaps = (NamedList<NamedList<Object>>) srsp_facet_counts.get(RESPONSE_KEY);
    if (facet_heatmaps == null) {
      return;
    }
    // (should the caller handle the above logic?  Arguably yes.)
    for (Map.Entry<String, NamedList<Object>> entry : facet_heatmaps) {
      String fieldKey = entry.getKey();
      NamedList<Object> shardNamedList = entry.getValue();
      final HeatmapFacet facet = heatmapFacets.get(fieldKey);
      if (facet == null) {
        log.error("received heatmap for field/key {} that we weren't expecting", fieldKey);
        continue;
      }
      facet.counts = addPngToIntArray((byte[]) shardNamedList.remove("counts_" + FORMAT_PNG), facet.counts);
      if (facet.namedList == null) {
        // First shard
        facet.namedList = shardNamedList;
      } else {
        assert facet.namedList.equals(shardNamedList);
      }
    }
  }

  //package access for tests
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

  /** Called by FacetComponent's impl of
   * {@link org.apache.solr.handler.component.SearchComponent#finishStage(ResponseBuilder)}. */
  public static NamedList distribFinish(LinkedHashMap<String, HeatmapFacet> heatmapInfos, ResponseBuilder rb) {
    NamedList<NamedList<Object>> result = new SimpleOrderedMap<>();
    for (Map.Entry<String, HeatmapFacet> entry : heatmapInfos.entrySet()) {
      final HeatmapFacet facet = entry.getValue();
      final NamedList<Object> namedList = facet.namedList;
      if (namedList == null) {
        continue;//should never happen but play it safe
      }
      formatCountsAndAddToNL(entry.getKey(), rb, SolrParams.wrapDefaults(facet.localParams, rb.req.getParams()),
          (int) namedList.get("columns"), (int) namedList.get("rows"), facet.counts, namedList);
      result.add(entry.getKey(), namedList);
    }
    return result;
  }

  /** Goes in {@link org.apache.solr.handler.component.FacetComponent.FacetInfo#heatmapFacets}, created by
   * {@link #distribParse(org.apache.solr.common.params.SolrParams, ResponseBuilder)}. */
  public static class HeatmapFacet extends FacetComponent.FacetBase {
    //note: 'public' following-suit with FacetBase & existing subclasses... though should this really be?

    //Holds response NamedList for this field, with counts pulled out. Taken from 1st shard response.
    public NamedList<Object> namedList;
    //Like Heatmap.counts in Lucene spatial, although null if it would be all-0.
    public int[] counts;

    public HeatmapFacet(ResponseBuilder rb, String facetStr) {
      super(rb, FacetParams.FACET_HEATMAP, facetStr);
      //note: logic in super (FacetBase) is partially redundant with SimpleFacet.parseParams :-(
    }
  }

  //
  // PngHelper
  //

  //package access for tests
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
