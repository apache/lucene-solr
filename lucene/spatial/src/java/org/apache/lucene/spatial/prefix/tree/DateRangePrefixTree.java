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
package org.apache.lucene.spatial.prefix.tree;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import com.spatial4j.core.shape.Shape;

/**
 * A PrefixTree for date ranges in which the levels of the tree occur at natural periods of time (e.g. years,
 * months, ...). You pass in {@link Calendar} objects with the desired fields set and the unspecified
 * fields unset, which conveys the precision.  The implementation makes some optimization assumptions about a
 * {@link java.util.GregorianCalendar}; others could probably be supported easily.
 * <p>
 * Warning: If you construct a Calendar and then get something from the object like a field (e.g. year) or
 * milliseconds, then every field is fully set by side-effect. So after setting the fields, pass it to this
 * API first.
 * @lucene.experimental
 */
public class DateRangePrefixTree extends NumberRangePrefixTree {

  /*
    WARNING  java.util.Calendar is tricky to work with:
    * If you "get" any field value, every field becomes "set". This can introduce a Heisenbug effect,
        when in a debugger in some cases. Fortunately, Calendar.toString() doesn't apply.
    * Beware Calendar underflow of the underlying long.  If you create a Calendar from LONG.MIN_VALUE, and clear
     a field, it will underflow and appear close to LONG.MAX_VALUE (BC to AD).

    There are no doubt other reasons but those two were hard fought lessons here.

    TODO Improvements:
    * Make max precision configurable (i.e. to SECOND).
    * Make min & max year span configurable. Use that to remove pointless top levels of the SPT.
        If year span is > 10k, then add 1k year level. If year span is > 10k of 1k levels, add 1M level.
    * NumberRangePrefixTree: override getTreeCellIterator for optimized case where the shape isn't a date span; use
      FilterCellIterator of the cell stack.

  */

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static Calendar CAL_TMP;//template
  static {
    CAL_TMP = Calendar.getInstance(UTC, Locale.ROOT);
    CAL_TMP.clear();
  }

  private static final Calendar MINCAL = (Calendar) CAL_TMP.clone();
  private static final Calendar MAXCAL = (Calendar) CAL_TMP.clone();
  static {
    MINCAL.setTimeInMillis(Long.MIN_VALUE);
    MAXCAL.setTimeInMillis(Long.MAX_VALUE);
  }
  //BC years are decreasing, remember.  Yet ActualMaximum is the numerically high value, ActualMinimum is 1.
  private static final int BC_FIRSTYEAR = MINCAL.getActualMaximum(Calendar.YEAR);
  private static final int BC_LASTYEAR = MINCAL.getActualMinimum(Calendar.YEAR);//1
  private static final int BC_YEARS = BC_FIRSTYEAR - BC_LASTYEAR + 1;
  private static final int AD_FIRSTYEAR = MAXCAL.getActualMinimum(Calendar.YEAR);//1
  private static final int AD_LASTYEAR = MAXCAL.getActualMaximum(Calendar.YEAR);
  private static final int AD_YEAR_BASE =  (((BC_YEARS-1) / 1000_000)+1) * 1000_000;
  static { assert BC_LASTYEAR == 1 && AD_FIRSTYEAR == 1; }

  //how many million years are there?
  private static final int NUM_MYEARS = (AD_YEAR_BASE + AD_LASTYEAR) / 1000_000;

  private static int calFieldLen(int field) {
    return CAL_TMP.getMaximum(field) - CAL_TMP.getMinimum(field) + 1;
  }

  private static final int[] FIELD_BY_LEVEL = {
      -1/*unused*/, -1, -1, Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH,
      Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND};
  private static final int yearLevel = 3;

  public static final DateRangePrefixTree INSTANCE = new DateRangePrefixTree();

  private final UnitNRShape minLV, maxLV;
  private final UnitNRShape gregorianChangeDateLV;

  protected DateRangePrefixTree() {
    super(new int[]{//sublevels by level
        NUM_MYEARS,
        1000,//1 thousand thousand-years in a million years
        1000,//1 thousand years in a thousand-year
        calFieldLen(Calendar.MONTH),
        calFieldLen(Calendar.DAY_OF_MONTH),
        calFieldLen(Calendar.HOUR_OF_DAY),
        calFieldLen(Calendar.MINUTE),
        calFieldLen(Calendar.SECOND),
        calFieldLen(Calendar.MILLISECOND),
    });
    maxLV = toShape((Calendar)MAXCAL.clone());
    minLV = toShape((Calendar)MINCAL.clone());
    if (MAXCAL instanceof GregorianCalendar) {
      //TODO this should be a configurable param by passing a Calendar serving as a template.
      GregorianCalendar gCal = (GregorianCalendar)MAXCAL;
      gregorianChangeDateLV = toUnitShape(gCal.getGregorianChange());
    } else {
      gregorianChangeDateLV = null;
    }
  }

  @Override
  public int getNumSubCells(UnitNRShape lv) {
    int cmp = comparePrefix(lv, maxLV);
    assert cmp <= 0;
    if (cmp == 0)//edge case (literally!)
      return maxLV.getValAtLevel(lv.getLevel()+1);

    // if using GregorianCalendar and we're after the "Gregorian change date" then we'll compute
    //  the sub-cells ourselves more efficiently without the need to construct a Calendar.
    cmp = gregorianChangeDateLV != null ? comparePrefix(lv, gregorianChangeDateLV) : -1;
    //TODO consider also doing fast-path if field is <= hours even if before greg change date
    if (cmp >= 0) {
      int result = fastSubCells(lv);
      assert result == slowSubCells(lv) : "fast/slow numSubCells inconsistency";
      return result;
    } else {
      return slowSubCells(lv);
    }
  }

  private int fastSubCells(UnitNRShape lv) {
    if (lv.getLevel() == yearLevel+1) {//month
      switch (lv.getValAtLevel(lv.getLevel())) {
        case Calendar.SEPTEMBER:
        case Calendar.APRIL:
        case Calendar.JUNE:
        case Calendar.NOVEMBER:
          return 30;
        case Calendar.FEBRUARY:
          //get the year (negative numbers for BC)
          int yearAdj = lv.getValAtLevel(1) * 1_000_000;
          yearAdj += lv.getValAtLevel(2) * 1000;
          yearAdj += lv.getValAtLevel(3);
          int year = yearAdj - AD_YEAR_BASE;
          if (year % 4 == 0 && !(year % 100 == 0 && year % 400 != 0) )//leap year
            return 29;
          else
            return 28;
        default:
          return 31;
      }
    } else {//typical:
      return super.getNumSubCells(lv);
    }
  }

  private int slowSubCells(UnitNRShape lv) {
    int field = FIELD_BY_LEVEL[lv.getLevel()+1];
    //short-circuit optimization (GregorianCalendar assumptions)
    if (field == -1 || field == Calendar.YEAR || field >= Calendar.HOUR_OF_DAY)//TODO make configurable
      return super.getNumSubCells(lv);
    Calendar cal = toCalendar(lv);//somewhat heavyweight op; ideally should be stored on UnitNRShape somehow
    return cal.getActualMaximum(field) - cal.getActualMinimum(field) + 1;
  }

  /** Calendar utility method:
   * Returns a new {@link Calendar} in UTC TimeZone, ROOT Locale, with all fields cleared. */
  public Calendar newCal() {
    return (Calendar) CAL_TMP.clone();
  }

  /** Calendar utility method:
   * Returns the spatial prefix tree level for the corresponding {@link java.util.Calendar} field, such as
   * {@link java.util.Calendar#YEAR}.  If there's no match, the next greatest level is returned as a negative value.
   */
  public int getTreeLevelForCalendarField(int calField) {
    for (int i = yearLevel; i < FIELD_BY_LEVEL.length; i++) {
      if (FIELD_BY_LEVEL[i] == calField) {
        return i;
      } else if (FIELD_BY_LEVEL[i] > calField) {
        return -1 * i;
      }
    }
    throw new IllegalArgumentException("Bad calendar field?: " + calField);
  }

  /** Calendar utility method:
   * Gets the Calendar field code of the last field that is set prior to an unset field. It only
   * examines fields relevant to the prefix tree. If no fields are set, it returns -1. */
  public int getCalPrecisionField(Calendar cal) {
    int lastField = -1;
    for (int level = yearLevel; level < FIELD_BY_LEVEL.length; level++) {
      int field = FIELD_BY_LEVEL[level];
      if (!cal.isSet(field))
        break;
      lastField = field;
    }
    return lastField;
  }

  /** Calendar utility method:
   * Calls {@link Calendar#clear(int)} for every field after {@code field}. Beware of Calendar underflow. */
  public void clearFieldsAfter(Calendar cal, int field) {
    if (field == -1) {
      cal.clear();
      return;
    }
    int assertEra = -1;
    assert (assertEra = (((Calendar)cal.clone()).get(Calendar.ERA))) >= 0;//a trick to only get this if assert enabled
    for (int f = field+1; f < Calendar.FIELD_COUNT; f++) {
      cal.clear(f);
    }
    assert ((Calendar)cal.clone()).get(Calendar.ERA) == assertEra : "Calendar underflow";
  }

  /** Converts {@code value} from a {@link Calendar} or {@link Date} to a {@link Shape}. Other arguments
   * result in a {@link java.lang.IllegalArgumentException}.
   */
  @Override
  public UnitNRShape toUnitShape(Object value) {
    if (value instanceof Calendar) {
      return toShape((Calendar) value);
    } else if (value instanceof Date) {
      Calendar cal = newCal();
      cal.setTime((Date)value);
      return toShape(cal);
    }
    throw new IllegalArgumentException("Expecting Calendar or Date but got: "+value.getClass());
  }

  /** Converts the Calendar into a Shape.
   * The isSet() state of the Calendar is re-instated when done. */
  public UnitNRShape toShape(Calendar cal) {
    // Convert a Calendar into a stack of cell numbers
    final int calPrecField = getCalPrecisionField(cal);//must call first; getters set all fields
    try {
      int[] valStack = new int[maxLevels];//starts at level 1, not 0
      int len = 0;
      if (calPrecField >= Calendar.YEAR) {//year or better precision
        int year = cal.get(Calendar.YEAR);
        int yearAdj = cal.get(Calendar.ERA) == 0 ? AD_YEAR_BASE - (year - 1) : AD_YEAR_BASE + year;

        valStack[len++] = yearAdj / 1000_000;
        yearAdj -= valStack[len-1] * 1000_000;
        valStack[len++] = yearAdj / 1000;
        yearAdj -= valStack[len-1] * 1000;
        valStack[len++] = yearAdj;
        for (int level = yearLevel+1; level < FIELD_BY_LEVEL.length; level++) {
          int field = FIELD_BY_LEVEL[level];
          if (field > calPrecField)
            break;
          valStack[len++] = cal.get(field) - cal.getActualMinimum(field);
        }
      }

      return toShape(valStack, len);
    } finally {
      clearFieldsAfter(cal, calPrecField);//restore precision state modified by get()
    }
  }

  /** Calls {@link #toCalendar(org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape)}. */
  @Override
  public Object toObject(UnitNRShape shape) {
    return toCalendar(shape);
  }

  /** Converts the {@link org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.UnitNRShape} shape to a
   * corresponding Calendar that is cleared below its level. */
  public Calendar toCalendar(UnitNRShape lv) {
    if (lv.getLevel() == 0)
      return newCal();
    if (comparePrefix(lv, minLV) <= 0) {//shouldn't typically happen; sometimes in a debugger
      return (Calendar) MINCAL.clone();//full precision; truncation would cause underflow
    }
    assert comparePrefix(lv, maxLV) <= 0;
    Calendar cal = newCal();

    int yearAdj = lv.getValAtLevel(1) * 1_000_000;
    if (lv.getLevel() > 1) {
      yearAdj += lv.getValAtLevel(2) * 1000;
      if (lv.getLevel() > 2) {
        yearAdj += lv.getValAtLevel(3);
      }
    }
    if (yearAdj > AD_YEAR_BASE) {
      cal.set(Calendar.ERA, 1);
      cal.set(Calendar.YEAR, yearAdj - AD_YEAR_BASE);//setting the year resets the era
    } else {
      cal.set(Calendar.ERA, 0);//we assert this "sticks" at the end
      cal.set(Calendar.YEAR, (AD_YEAR_BASE - yearAdj) + 1);
    }
    for (int level = yearLevel+1; level <= lv.getLevel(); level++) {
      int field = FIELD_BY_LEVEL[level];
      cal.set(field, lv.getValAtLevel(level) + cal.getActualMinimum(field));
    }
    assert yearAdj > AD_YEAR_BASE || ((Calendar)cal.clone()).get(Calendar.ERA) == 0 : "ERA / YEAR underflow";
    return cal;
  }

  @Override
  protected String toString(UnitNRShape lv) {
    return toString(toCalendar(lv));
  }

  /** Calendar utility method:
   * Formats the calendar to ISO-8601 format, to include proper BC handling (1BC is "0000", 2BC is "-0001", etc.);
   * and WITHOUT a trailing 'Z'.
   * A fully cleared calendar will yield the string "*".
   * The isSet() state of the Calendar is re-instated when done. */
   @SuppressWarnings("fallthrough")
  public String toString(Calendar cal) {
    final int calPrecField = getCalPrecisionField(cal);//must call first; getters set all fields
    if (calPrecField == -1)
      return "*";
    try {
      //TODO not fully optimized; but it's at least not used in 'search'.
      //TODO maybe borrow code from Solr DateUtil (put in Lucene util somewhere), and have it reference this back?
      String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS";
      int ptnLen = 0;
      switch (calPrecField) {//switch fall-through is deliberate
        case Calendar.MILLISECOND: ptnLen += 4;
        case Calendar.SECOND: ptnLen += 3;
        case Calendar.MINUTE: ptnLen += 3;
        case Calendar.HOUR_OF_DAY: ptnLen += 5;
        case Calendar.DAY_OF_MONTH: ptnLen += 3;
        case Calendar.MONTH: ptnLen += 3;
        case Calendar.YEAR: ptnLen += 4;
        break;
        default: throw new IllegalStateException(""+calPrecField);
      }
      pattern = pattern.substring(0, ptnLen);
      SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.ROOT);
      format.setTimeZone(cal.getTimeZone());
      if (cal.get(Calendar.ERA) == 0) {//BC
        //SDF doesn't do this properly according to ISO-8601
        // Example: 1BC == "0000" (actually 0 AD), 2BC == "-0001", 3BC == "-0002", ...
        final int yearOrig = cal.get(Calendar.YEAR);
        cal.set(Calendar.YEAR, yearOrig-1);
        String str;
        try {
          str = format.format(cal.getTime());
        } finally {
          //reset to what it was
          cal.set(Calendar.ERA, 0);//necessary!
          cal.set(Calendar.YEAR, yearOrig);
        }
        if (yearOrig > 1)
          return "-" + str;
        else
          return "0000" + str.substring(4);
      }
      return format.format(cal.getTime());
    } finally {
      clearFieldsAfter(cal, calPrecField);//restore precision state modified by get()
    }
  }

  @Override
  protected UnitNRShape parseUnitShape(String str) throws ParseException {
    return toShape(parseCalendar(str));
  }

  /** Calendar utility method:
   * The reverse of {@link #toString(java.util.Calendar)}. It will only set the fields found, leaving
   * the remainder in an un-set state. A leading '-' or '+' is optional (positive assumed), and a
   * trailing 'Z' is also optional.
   * @param str not null and not empty
   * @return not null
   */
  public Calendar parseCalendar(String str) throws ParseException {
    // example: +2014-10-23T21:22:33.159Z
    if (str == null || str.isEmpty())
      throw new IllegalArgumentException("str is null or blank");
    Calendar cal = newCal();
    if (str.equals("*"))
      return cal;
    int offset = 0;//a pointer
    try {
      //year & era:
      int lastOffset = str.charAt(str.length()-1) == 'Z' ? str.length() - 1 : str.length();
      int hyphenIdx = str.indexOf('-', 1);//look past possible leading hyphen
      if (hyphenIdx < 0)
        hyphenIdx = lastOffset;
      int year = Integer.parseInt(str.substring(offset, hyphenIdx));
      cal.set(Calendar.ERA, year <= 0 ? 0 : 1);
      cal.set(Calendar.YEAR, year <= 0 ? -1*year + 1 : year);
      offset = hyphenIdx + 1;
      if (lastOffset < offset)
        return cal;

      //NOTE: We aren't validating separator chars, and we unintentionally accept leading +/-.
      // The str.substring()'s hopefully get optimized to be stack-allocated.

      //month:
      cal.set(Calendar.MONTH, Integer.parseInt(str.substring(offset, offset+2)) - 1);//starts at 0
      offset += 3;
      if (lastOffset < offset)
        return cal;
      //day:
      cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(str.substring(offset, offset+2)));
      offset += 3;
      if (lastOffset < offset)
        return cal;
      //hour:
      cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(str.substring(offset, offset+2)));
      offset += 3;
      if (lastOffset < offset)
        return cal;
      //minute:
      cal.set(Calendar.MINUTE, Integer.parseInt(str.substring(offset, offset+2)));
      offset += 3;
      if (lastOffset < offset)
        return cal;
      //second:
      cal.set(Calendar.SECOND, Integer.parseInt(str.substring(offset, offset+2)));
      offset += 3;
      if (lastOffset < offset)
        return cal;
      //ms:
      cal.set(Calendar.MILLISECOND, Integer.parseInt(str.substring(offset, offset+3)));
      offset += 3;//last one, move to next char
      if (lastOffset == offset)
        return cal;
    } catch (Exception e) {
      ParseException pe = new ParseException("Improperly formatted date: "+str, offset);
      pe.initCause(e);
      throw pe;
    }
    throw new ParseException("Improperly formatted date: "+str, offset);
  }

}
