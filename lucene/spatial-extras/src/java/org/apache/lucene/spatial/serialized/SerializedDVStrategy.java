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
package org.apache.lucene.spatial.serialized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.spatial.ShapeValues;
import org.apache.lucene.spatial.ShapeValuesSource;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.util.DistanceToShapeValueSource;
import org.apache.lucene.spatial.util.ShapeValuesPredicate;
import org.apache.lucene.util.BytesRef;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.io.BinaryCodec;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;


/**
 * A SpatialStrategy based on serializing a Shape stored into BinaryDocValues.
 * This is not at all fast; it's designed to be used in conjunction with another index based
 * SpatialStrategy that is approximated (like {@link org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy})
 * to add precision or eventually make more specific / advanced calculations on the per-document
 * geometry.
 * The serialization uses Spatial4j's {@link org.locationtech.spatial4j.io.BinaryCodec}.
 *
 * @lucene.experimental
 */
public class SerializedDVStrategy extends SpatialStrategy {

  /**
   * A cache heuristic for the buf size based on the last shape size.
   */
  //TODO do we make this non-volatile since it's merely a heuristic?
  private volatile int indexLastBufSize = 8 * 1024;//8KB default on first run

  /**
   * Constructs the spatial strategy with its mandatory arguments.
   */
  public SerializedDVStrategy(SpatialContext ctx, String fieldName) {
    super(ctx, fieldName);
  }

  @Override
  public Field[] createIndexableFields(Shape shape) {
    int bufSize = Math.max(128, (int) (this.indexLastBufSize * 1.5));//50% headroom over last
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(bufSize);
    final BytesRef bytesRef = new BytesRef();//receiver of byteStream's bytes
    try {
      ctx.getBinaryCodec().writeShape(new DataOutputStream(byteStream), shape);
      //this is a hack to avoid redundant byte array copying by byteStream.toByteArray()
      byteStream.writeTo(new FilterOutputStream(null/*not used*/) {
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          bytesRef.bytes = b;
          bytesRef.offset = off;
          bytesRef.length = len;
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.indexLastBufSize = bytesRef.length;//cache heuristic
    return new Field[]{new BinaryDocValuesField(getFieldName(), bytesRef)};
  }

  @Override
  public DoubleValuesSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    //TODO if makeShapeValueSource gets lifted to the top; this could become a generic impl.
    return new DistanceToShapeValueSource(makeShapeValueSource(), queryPoint, multiplier, ctx);
  }

  /**
   * Returns a Query that should be used in a random-access fashion.
   * Use in another manner will be SLOW.
   */
  @Override
  public Query makeQuery(SpatialArgs args) {
    ShapeValuesSource shapeValueSource = makeShapeValueSource();
    ShapeValuesPredicate predicateValueSource = new ShapeValuesPredicate(shapeValueSource, args.getOperation(), args.getShape());
    return new PredicateValueSourceQuery(predicateValueSource);
  }

  /**
   * Provides access to each shape per document
   */ //TODO raise to SpatialStrategy
  public ShapeValuesSource makeShapeValueSource() {
    return new ShapeDocValueSource(getFieldName(), ctx.getBinaryCodec());
  }

  /** Warning: don't iterate over the results of this query; it's designed for use in a random-access fashion
   * by {@link TwoPhaseIterator}.
   */
  static class PredicateValueSourceQuery extends Query {
    private final ShapeValuesPredicate predicateValueSource;

    public PredicateValueSourceQuery(ShapeValuesPredicate predicateValueSource) {
      this.predicateValueSource = predicateValueSource;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
          TwoPhaseIterator it = predicateValueSource.iterator(context, approximation);
          return new ConstantScoreScorer(this, score(), scoreMode, it);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return predicateValueSource.isCacheable(ctx);
        }

      };
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             predicateValueSource.equals(((PredicateValueSourceQuery) other).predicateValueSource);
    }

    @Override
    public int hashCode() {
      return classHash() + 31 * predicateValueSource.hashCode();
    }

    @Override
    public String toString(String field) {
      return "PredicateValueSourceQuery(" +
               predicateValueSource.toString() +
             ")";
    }
  }//PredicateValueSourceQuery

  /**
   * Implements a ShapeValueSource by deserializing a Shape from BinaryDocValues using BinaryCodec.
   * @see #makeShapeValueSource()
   */
  static class ShapeDocValueSource extends ShapeValuesSource {

    private final String fieldName;
    private final BinaryCodec binaryCodec;//spatial4j

    private ShapeDocValueSource(String fieldName, BinaryCodec binaryCodec) {
      this.fieldName = fieldName;
      this.binaryCodec = binaryCodec;
    }

    @Override
    public ShapeValues getValues(LeafReaderContext readerContext) throws IOException {
      final BinaryDocValues docValues = DocValues.getBinary(readerContext.reader(), fieldName);

      return new ShapeValues() {
        @Override
        public boolean advanceExact(int doc) throws IOException {
          return docValues.advanceExact(doc);
        }

        @Override
        public Shape value() throws IOException {
          BytesRef bytesRef = docValues.binaryValue();
          DataInputStream dataInput
              = new DataInputStream(new ByteArrayInputStream(bytesRef.bytes, bytesRef.offset, bytesRef.length));
          return binaryCodec.readShape(dataInput);
        }

      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, fieldName);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ShapeDocValueSource that = (ShapeDocValueSource) o;

      if (!fieldName.equals(that.fieldName)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = fieldName.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "shapeDocVal(" + fieldName + ")";
    }
  }//ShapeDocValueSource
}
