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

package org.apache.lucene.monitor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.search.Query;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;

/**
 * Serializes and deserializes MonitorQuery objects into byte streams
 *
 * Use this for persistent query indexes
 */
public interface MonitorQuerySerializer {

  /**
   * Builds a MonitorQuery from a byte representation
   */
  MonitorQuery deserialize(BytesRef binaryValue);

  /**
   * Converts a MonitorQuery into a byte representation
   */
  BytesRef serialize(MonitorQuery query);

  /**
   * Build a serializer from a query parser
   *
   * @param parser a parser to convert a String representation of a query into a lucene query object
   */
  static MonitorQuerySerializer fromParser(Function<String, Query> parser) {
    return new MonitorQuerySerializer() {
      @Override
      public MonitorQuery deserialize(BytesRef binaryValue) {
        ByteArrayInputStream is = new ByteArrayInputStream(binaryValue.bytes, binaryValue.offset, binaryValue.length);
        try (InputStreamDataInput data = new InputStreamDataInput(is)) {
          String id = data.readString();
          String query = data.readString();
          Map<String, String> metadata = new HashMap<>();
          for (int i = data.readInt(); i > 0; i--) {
            metadata.put(data.readString(), data.readString());
          }
          return new MonitorQuery(id, parser.apply(query), query, metadata);
        } catch (IOException e) {
          throw new RuntimeException(e);  // shouldn't happen, we're reading from a bytearray!
        }
      }

      @Override
      public BytesRef serialize(MonitorQuery query) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (OutputStreamDataOutput data = new OutputStreamDataOutput(os)) {
          data.writeString(query.getId());
          data.writeString(query.getQueryString());
          data.writeInt(query.getMetadata().size());
          for (Map.Entry<String, String> entry : query.getMetadata().entrySet()) {
            data.writeString(entry.getKey());
            data.writeString(entry.getValue());
          }
          return new BytesRef(os.toByteArray());
        }
        catch (IOException e) {
          throw new RuntimeException(e);  // All in memory, so no IOException should be thrown
        }
      }
    };
  }

}
