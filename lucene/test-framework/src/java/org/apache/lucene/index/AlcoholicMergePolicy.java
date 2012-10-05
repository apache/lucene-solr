package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;

import org.apache.lucene.util._TestUtil;

/** 
 * <p>
 * Merge policy for testing, it is like an alcoholic.
 * It drinks (merges) at night, and randomly decides what to drink.
 * During the daytime it sleeps.
 * </p>
 * <p>
 * if tests pass with this, then they are likely to pass with any 
 * bizarro merge policy users might write.
 * </p>
 * <p>
 * It is a fine bottle of champagne (Ordered by Martijn). 
 * </p>
 */
public class AlcoholicMergePolicy extends LogMergePolicy {
  
  private final Random random;
  private final Calendar calendar;
  
  public AlcoholicMergePolicy(TimeZone tz, Random random) {
    this.calendar = new GregorianCalendar(tz, Locale.ROOT);
    calendar.setTimeInMillis(_TestUtil.nextLong(random, 0, Long.MAX_VALUE));
    this.random = random;
    maxMergeSize = _TestUtil.nextInt(random, 1024*1024, Integer.MAX_VALUE);
  }
  
  @Override
  //@BlackMagic(level=Voodoo);
  protected long size(SegmentInfoPerCommit info) throws IOException {
    int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
    if (hourOfDay < 6 || 
        hourOfDay > 20 || 
        // its 5 o'clock somewhere
        random.nextInt(23) == 5) {
      
      Drink[] values = Drink.values();
      // pick a random drink during the day
      return values[random.nextInt(values.length)].drunkFactor * info.sizeInBytes();
    }

    return info.sizeInBytes();
  }
  
  private static enum Drink {
    
    Beer(15), Wine(17), Champagne(21), WhiteRussian(22), SingleMalt(30);
    
    long drunkFactor;
    
    Drink(long drunkFactor) {
      this.drunkFactor = drunkFactor;
    }
  }
  
}
