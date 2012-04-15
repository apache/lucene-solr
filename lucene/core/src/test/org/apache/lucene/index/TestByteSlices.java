package org.apache.lucene.index;

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RecyclingByteBlockAllocator;

public class TestByteSlices extends LuceneTestCase {

  public void testBasic() throws Throwable {
    ByteBlockPool pool = new ByteBlockPool(new RecyclingByteBlockAllocator(ByteBlockPool.BYTE_BLOCK_SIZE, Integer.MAX_VALUE));

    final int NUM_STREAM = atLeast(100);

    ByteSliceWriter writer = new ByteSliceWriter(pool);

    int[] starts = new int[NUM_STREAM];
    int[] uptos = new int[NUM_STREAM];
    int[] counters = new int[NUM_STREAM];

    ByteSliceReader reader = new ByteSliceReader();

    for(int ti=0;ti<100;ti++) {

      for(int stream=0;stream<NUM_STREAM;stream++) {
        starts[stream] = -1;
        counters[stream] = 0;
      }
      
      int num = atLeast(10000);
      for (int iter = 0; iter < num; iter++) {
        int stream = random().nextInt(NUM_STREAM);
        if (VERBOSE)
          System.out.println("write stream=" + stream);

        if (starts[stream] == -1) {
          final int spot = pool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
          starts[stream] = uptos[stream] = spot + pool.byteOffset;
          if (VERBOSE)
            System.out.println("  init to " + starts[stream]);
        }

        writer.init(uptos[stream]);
        int numValue = random().nextInt(20);
        for(int j=0;j<numValue;j++) {
          if (VERBOSE)
            System.out.println("    write " + (counters[stream]+j));
          // write some large (incl. negative) ints:
          writer.writeVInt(random().nextInt());
          writer.writeVInt(counters[stream]+j);
        }
        counters[stream] += numValue;
        uptos[stream] = writer.getAddress();
        if (VERBOSE)
          System.out.println("    addr now " + uptos[stream]);
      }
    
      for(int stream=0;stream<NUM_STREAM;stream++) {
        if (VERBOSE)
          System.out.println("  stream=" + stream + " count=" + counters[stream]);

        if (starts[stream] != -1 && starts[stream] != uptos[stream]) {
          reader.init(pool, starts[stream], uptos[stream]);
          for(int j=0;j<counters[stream];j++) {
            reader.readVInt();
            assertEquals(j, reader.readVInt()); 
          }
        }
      }

      pool.reset();
    }
  }
}
