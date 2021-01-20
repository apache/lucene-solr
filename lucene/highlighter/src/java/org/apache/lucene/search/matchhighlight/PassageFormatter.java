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
package org.apache.lucene.search.matchhighlight;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.RandomAccess;
import java.util.function.Function;

/**
 * Formats a collection of {@linkplain Passage passages} over a given string, cleaning up and
 * resolving restrictions concerning overlaps, allowed sub-ranges over the input string and length
 * restrictions.
 *
 * <p>Passages are demarcated with constructor-provided ellipsis and start/end marker sequences.
 */
public class PassageFormatter {
  private final String ellipsis;
  private final Function<OffsetRange, String> markerStart;
  private final Function<OffsetRange, String> markerEnd;

  private final ArrayList<OffsetRange> markerStack = new ArrayList<>();

  public PassageFormatter(String ellipsis, String markerStart, String markerEnd) {
    this(ellipsis, (m) -> markerStart, (m) -> markerEnd);
  }

  public PassageFormatter(
      String ellipsis,
      Function<OffsetRange, String> markerStart,
      Function<OffsetRange, String> markerEnd) {
    this.ellipsis = ellipsis;
    this.markerStart = markerStart;
    this.markerEnd = markerEnd;
  }

  public List<String> format(CharSequence value, List<Passage> passages, List<OffsetRange> ranges) {
    assert PassageSelector.sortedAndNonOverlapping(passages);
    assert PassageSelector.sortedAndNonOverlapping(ranges);
    assert withinRange(new OffsetRange(0, value.length()), passages);
    assert ranges instanceof RandomAccess;

    if (ranges.isEmpty()) {
      return Collections.emptyList();
    }

    ArrayList<String> result = new ArrayList<>();
    StringBuilder buf = new StringBuilder();

    int rangeIndex = 0;
    OffsetRange range = ranges.get(rangeIndex);
    passageFormatting:
    for (Passage passage : passages) {
      // Move to the range of the current passage.
      while (passage.from >= range.to) {
        if (++rangeIndex == ranges.size()) {
          break passageFormatting;
        }
        range = ranges.get(rangeIndex);
      }

      assert range.from <= passage.from && range.to >= passage.to : range + " ? " + passage;

      buf.setLength(0);
      if (range.from < passage.from) {
        buf.append(ellipsis);
      }
      format(buf, value, passage);
      if (range.to > passage.to) {
        buf.append(ellipsis);
      }
      result.add(buf.toString());
    }
    return result;
  }

  private boolean withinRange(OffsetRange limits, List<? extends OffsetRange> contained) {
    contained.forEach(
        r -> {
          if (r.from < limits.from || r.to > limits.to) {
            throw new AssertionError(
                String.format(
                    Locale.ROOT,
                    "Range outside of the permitted limit (limit = %s): %s",
                    limits,
                    r));
          }
        });
    return true;
  }

  public StringBuilder format(StringBuilder buf, CharSequence value, final Passage passage) {
    switch (passage.markers.size()) {
      case 0:
        // No markers, full passage appended.
        buf.append(value, passage.from, passage.to);
        break;

      case 1:
        // One marker, trivial and frequent case so it's handled separately.
        OffsetRange m = passage.markers.iterator().next();
        buf.append(value, passage.from, m.from);
        buf.append(markerStart.apply(m));
        buf.append(value, m.from, m.to);
        buf.append(markerEnd.apply(m));
        buf.append(value, m.to, passage.to);
        break;

      default:
        // Multiple markers, possibly overlapping or nested.
        markerStack.clear();
        multipleMarkers(value, passage, buf, markerStack);
        break;
    }

    return buf;
  }

  /** Handle multiple markers, possibly overlapping or nested. */
  private void multipleMarkers(
      CharSequence value, final Passage p, StringBuilder b, ArrayList<OffsetRange> markerStack) {
    int at = p.from;
    int max = p.to;
    SlicePoint[] slicePoints = slicePoints(p);
    for (SlicePoint slicePoint : slicePoints) {
      b.append(value, at, slicePoint.offset);
      OffsetRange currentMarker = slicePoint.marker;
      switch (slicePoint.type) {
        case START:
          markerStack.add(currentMarker);
          b.append(markerStart.apply(currentMarker));
          break;

        case END:
          int markerIndex = markerStack.lastIndexOf(currentMarker);
          for (int k = markerIndex; k < markerStack.size(); k++) {
            b.append(markerEnd.apply(markerStack.get(k)));
          }
          markerStack.remove(markerIndex);
          for (int k = markerIndex; k < markerStack.size(); k++) {
            b.append(markerStart.apply(markerStack.get(k)));
          }
          break;

        default:
          throw new RuntimeException();
      }

      at = slicePoint.offset;
    }

    if (at < max) {
      b.append(value, at, max);
    }
  }

  private static SlicePoint[] slicePoints(Passage p) {
    SlicePoint[] slicePoints = new SlicePoint[p.markers.size() * 2];
    int x = 0;
    for (OffsetRange m : p.markers) {
      slicePoints[x++] = new SlicePoint(SlicePoint.Type.START, m.from, m);
      slicePoints[x++] = new SlicePoint(SlicePoint.Type.END, m.to, m);
    }

    // Order slice points by their offset
    Comparator<SlicePoint> c =
        Comparator.<SlicePoint>comparingInt(pt -> pt.offset)
            .thenComparingInt(pt -> pt.type.ordering)
            .thenComparing(
                (a, b) -> {
                  if (a.type == SlicePoint.Type.START) {
                    // Longer start slice points come first.
                    return Integer.compare(b.marker.to, a.marker.to);
                  } else {
                    // Shorter end slice points come first.
                    return Integer.compare(b.marker.from, a.marker.from);
                  }
                });

    Arrays.sort(slicePoints, c);

    return slicePoints;
  }

  static class SlicePoint {
    enum Type {
      START(2),
      END(1);

      private final int ordering;

      Type(int ordering) {
        this.ordering = ordering;
      }
    }

    public final int offset;
    public final Type type;
    public final OffsetRange marker;

    public SlicePoint(Type t, int offset, OffsetRange m) {
      this.type = t;
      this.offset = offset;
      this.marker = m;
    }

    @Override
    public String toString() {
      return "(" + type + ", " + marker + ")";
    }
  }
}
