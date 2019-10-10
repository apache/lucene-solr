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

package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.RoutedAliasUpdateProcessor;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CreationType.ASYNC_PREEMPTIVE;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CreationType.NONE;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CreationType.SYNCHRONOUS;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CollectionAdminParams.ROUTER_PREFIX;
import static org.apache.solr.common.params.CommonParams.TZ;

/**
 * Holds configuration for a routed alias, and some common code and constants.
 *
 * @see CreateAliasCmd
 * @see MaintainRoutedAliasCmd
 * @see RoutedAliasUpdateProcessor
 */
public class TimeRoutedAlias extends RoutedAlias {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final RoutedAliasTypes TYPE = RoutedAliasTypes.TIME;

  // These two fields may be updated within the calling thread during processing but should
  // never be updated by any async creation thread.
  private List<Map.Entry<Instant, String>> parsedCollectionsDesc; // k=timestamp (start), v=collection.  Sorted descending
  private Aliases parsedCollectionsAliases; // a cached reference to the source of what we parse into parsedCollectionsDesc

  // These are parameter names to routed alias creation, AND are stored as metadata with the alias.
  @SuppressWarnings("WeakerAccess")
  public static final String ROUTER_START = ROUTER_PREFIX + "start";
  @SuppressWarnings("WeakerAccess")
  public static final String ROUTER_INTERVAL = ROUTER_PREFIX + "interval";
  @SuppressWarnings("WeakerAccess")
  public static final String ROUTER_MAX_FUTURE = ROUTER_PREFIX + "maxFutureMs";
  public static final String ROUTER_AUTO_DELETE_AGE = ROUTER_PREFIX + "autoDeleteAge";
  public static final String ROUTER_PREEMPTIVE_CREATE_MATH = ROUTER_PREFIX + "preemptiveCreateMath";
  // plus TZ and NAME

  /**
   * Parameters required for creating a routed alias
   */
  @SuppressWarnings("WeakerAccess")
  public static final Set<String> REQUIRED_ROUTER_PARAMS = Set.of(
      CommonParams.NAME, ROUTER_TYPE_NAME, ROUTER_FIELD, ROUTER_START, ROUTER_INTERVAL);

  /**
   * Optional parameters for creating a routed alias excluding parameters for collection creation.
   */
  //TODO lets find a way to remove this as it's harder to maintain than required list
  @SuppressWarnings("WeakerAccess")
  public static final Set<String> OPTIONAL_ROUTER_PARAMS = Set.of(
      ROUTER_MAX_FUTURE, ROUTER_AUTO_DELETE_AGE, ROUTER_PREEMPTIVE_CREATE_MATH, TZ); // kinda special


  // This format must be compatible with collection name limitations
  static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE).appendPattern("[_HH[_mm[_ss]]]") //brackets mean optional
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter(Locale.ROOT).withZone(ZoneOffset.UTC); // deliberate -- collection names disregard TZ

  //
  // Instance data and methods
  //

  private final String aliasName;
  private final Map<String, String> aliasMetadata;
  private final String routeField;
  private final String intervalMath; // ex: +1DAY
  private final long maxFutureMs;
  private final String preemptiveCreateMath;
  private final String autoDeleteAgeMath; // ex: /DAY-30DAYS  *optional*
  private final TimeZone timeZone;
  private String start;

  TimeRoutedAlias(String aliasName, Map<String, String> aliasMetadata) throws SolrException {
    // Validate we got everything we need
    if (!aliasMetadata.keySet().containsAll(TimeRoutedAlias.REQUIRED_ROUTER_PARAMS)) {
      throw new SolrException(BAD_REQUEST, "A time routed alias requires these params: " + TimeRoutedAlias.REQUIRED_ROUTER_PARAMS
          + " plus some create-collection prefixed ones.");
    }

    this.aliasMetadata = aliasMetadata;

    this.start = this.aliasMetadata.get(ROUTER_START);
    this.aliasName = aliasName;
    final MapSolrParams params = new MapSolrParams(this.aliasMetadata); // for convenience
    final RequiredSolrParams required = params.required();
    String type = required.get(ROUTER_TYPE_NAME).toLowerCase(Locale.ROOT);
    if (!"time".equals(type)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only 'time' routed aliases is supported by TimeRoutedAlias, found:" + type);
    }
    routeField = required.get(ROUTER_FIELD);
    intervalMath = required.get(ROUTER_INTERVAL);

    //optional:
    maxFutureMs = params.getLong(ROUTER_MAX_FUTURE, TimeUnit.MINUTES.toMillis(10));
    // the date math configured is an interval to be subtracted from the most recent collection's time stamp
    String pcmTmp = params.get(ROUTER_PREEMPTIVE_CREATE_MATH);
    preemptiveCreateMath = pcmTmp != null ? (pcmTmp.startsWith("-") ? pcmTmp : "-" + pcmTmp) : null;
    autoDeleteAgeMath = params.get(ROUTER_AUTO_DELETE_AGE); // no default
    timeZone = TimeZoneUtils.parseTimezone(this.aliasMetadata.get(CommonParams.TZ));

    // More validation:

    // check that the date math is valid
    final Date now = new Date();
    try {
      final Date after = new DateMathParser(now, timeZone).parseMath(getIntervalMath());
      if (!after.after(now)) {
        throw new SolrException(BAD_REQUEST, "duration must add to produce a time in the future");
      }
    } catch (Exception e) {
      throw new SolrException(BAD_REQUEST, "bad " + TimeRoutedAlias.ROUTER_INTERVAL + ", " + e, e);
    }

    if (autoDeleteAgeMath != null) {
      try {
        final Date before = new DateMathParser(now, timeZone).parseMath(autoDeleteAgeMath);
        if (now.before(before)) {
          throw new SolrException(BAD_REQUEST, "duration must round or subtract to produce a time in the past");
        }
      } catch (Exception e) {
        throw new SolrException(BAD_REQUEST, "bad " + TimeRoutedAlias.ROUTER_AUTO_DELETE_AGE + ", " + e, e);
      }
    }
    if (preemptiveCreateMath != null) {
      try {
        new DateMathParser().parseMath(preemptiveCreateMath);
      } catch (ParseException e) {
        throw new SolrException(BAD_REQUEST, "Invalid date math for preemptiveCreateMath:" + preemptiveCreateMath);
      }
    }

    if (maxFutureMs < 0) {
      throw new SolrException(BAD_REQUEST, ROUTER_MAX_FUTURE + " must be >= 0");
    }
  }

  @Override
  public String computeInitialCollectionName() {
    return formatCollectionNameFromInstant(aliasName, parseStringAsInstant(this.start, timeZone));
  }

  @Override
  String[] formattedRouteValues(SolrInputDocument doc) {
    String routeField = getRouteField();
    Date fieldValue = (Date) doc.getFieldValue(routeField);
    String dest = calcCandidateCollection(fieldValue.toInstant()).getDestinationCollection();
    int nonValuePrefix = getAliasName().length() + getRoutedAliasType().getSeparatorPrefix().length();
    return new String[]{dest.substring(nonValuePrefix)};
  }

  public static Instant parseInstantFromCollectionName(String aliasName, String collection) {
    String separatorPrefix = TYPE.getSeparatorPrefix();
    final String dateTimePart;
    if (collection.contains(separatorPrefix)) {
      dateTimePart = collection.substring(collection.lastIndexOf(separatorPrefix) + separatorPrefix.length());
    } else {
      dateTimePart = collection.substring(aliasName.length() + 1);
    }
    return DATE_TIME_FORMATTER.parse(dateTimePart, Instant::from);
  }

  public static String formatCollectionNameFromInstant(String aliasName, Instant timestamp) {
    String nextCollName = DATE_TIME_FORMATTER.format(timestamp);
    for (int i = 0; i < 3; i++) { // chop off seconds, minutes, hours
      if (nextCollName.endsWith("_00")) {
        nextCollName = nextCollName.substring(0, nextCollName.length() - 3);
      }
    }
    assert DATE_TIME_FORMATTER.parse(nextCollName, Instant::from).equals(timestamp);
    return aliasName + TYPE.getSeparatorPrefix() + nextCollName;
  }

  private Instant parseStringAsInstant(String str, TimeZone zone) {
    Instant start = DateMathParser.parseMath(new Date(), str, zone).toInstant();
    checkMillis(start);
    return start;
  }

  private void checkMillis(Instant date) {
    if (!date.truncatedTo(ChronoUnit.SECONDS).equals(date)) {
      throw new SolrException(BAD_REQUEST,
          "Date or date math for start time includes milliseconds, which is not supported. " +
              "(Hint: 'NOW' used without rounding always has this problem)");
    }
  }

  @Override
  public boolean updateParsedCollectionAliases(ZkStateReader zkStateReader, boolean contextualize) {
    final Aliases aliases = zkStateReader.getAliases();
    if (this.parsedCollectionsAliases != aliases) {
      if (this.parsedCollectionsAliases != null) {
        log.debug("Observing possibly updated alias: {}", getAliasName());
      }
      this.parsedCollectionsDesc = parseCollections(aliases);
      this.parsedCollectionsAliases = aliases;
      return true;
    }
    if (contextualize) {
      this.parsedCollectionsDesc = parseCollections(aliases);
    }
    return false;
  }

  @Override
  public String getAliasName() {
    return aliasName;
  }

  @Override
  public String getRouteField() {
    return routeField;
  }

  @Override
  public RoutedAliasTypes getRoutedAliasType() {
    return RoutedAliasTypes.TIME;
  }

  @SuppressWarnings("WeakerAccess")
  public String getIntervalMath() {
    return intervalMath;
  }

  @SuppressWarnings("WeakerAccess")
  public long getMaxFutureMs() {
    return maxFutureMs;
  }

  @SuppressWarnings("WeakerAccess")
  public String getPreemptiveCreateWindow() {
    return preemptiveCreateMath;
  }

  @SuppressWarnings("WeakerAccess")
  public String getAutoDeleteAgeMath() {
    return autoDeleteAgeMath;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("aliasName", aliasName)
        .add("routeField", routeField)
        .add("intervalMath", intervalMath)
        .add("maxFutureMs", maxFutureMs)
        .add("preemptiveCreateMath", preemptiveCreateMath)
        .add("autoDeleteAgeMath", autoDeleteAgeMath)
        .add("timeZone", timeZone)
        .toString();
  }

  /**
   * Parses the elements of the collection list. Result is returned them in sorted order (most recent 1st)
   */
  private List<Map.Entry<Instant, String>> parseCollections(Aliases aliases) {
    final List<String> collections = getCollectionList(aliases);
    if (collections == null) {
      throw RoutedAlias.newAliasMustExistException(getAliasName());
    }
    // note: I considered TreeMap but didn't like the log(N) just to grab the most recent when we use it later
    List<Map.Entry<Instant, String>> result = new ArrayList<>(collections.size());
    for (String collection : collections) {
      Instant colStartTime = parseInstantFromCollectionName(aliasName, collection);
      result.add(new AbstractMap.SimpleImmutableEntry<>(colStartTime, collection));
    }
    result.sort((e1, e2) -> e2.getKey().compareTo(e1.getKey())); // reverse sort by key
    return result;
  }


  /**
   * Computes the timestamp of the next collection given the timestamp of the one before.
   */
  private Instant computeNextCollTimestamp(Instant fromTimestamp) {
    final Instant nextCollTimestamp =
        DateMathParser.parseMath(Date.from(fromTimestamp), "NOW" + intervalMath, timeZone).toInstant();
    assert nextCollTimestamp.isAfter(fromTimestamp);
    return nextCollTimestamp;
  }

  @Override
  public void validateRouteValue(AddUpdateCommand cmd) throws SolrException {

    final Instant docTimestamp =
        parseRouteKey(cmd.getSolrInputDocument().getFieldValue(getRouteField()));

    // FUTURE: maybe in some cases the user would want to ignore/warn instead?
    if (docTimestamp.isAfter(Instant.now().plusMillis(getMaxFutureMs()))) {
      throw new SolrException(BAD_REQUEST,
          "The document's time routed key of " + docTimestamp + " is too far in the future given " +
              ROUTER_MAX_FUTURE + "=" + getMaxFutureMs());
    }

    // Although this is also checked later, we need to check it here too to handle the case in Dimensional Routed
    // aliases where one can legally have zero collections for a newly encountered category and thus the loop later
    // can't catch this.

    // SOLR-13760 - we need to fix the date math to a specific instant when the first document arrives.
    // If we don't do this DRA's with a time dimension have variable start times across the other dimensions
    // and logic gets much to complicated, and depends too much on queries to zookeeper. This keeps life simpler.
    // I have to admit I'm not terribly fond of the mutation during a validate method however.
    Instant startTime;
    try {
      startTime = Instant.parse(start);
    } catch (DateTimeParseException e) {
      startTime = DateMathParser.parseMath(new Date(), start).toInstant();
      SolrCore core = cmd.getReq().getCore();
      ZkStateReader zkStateReader = core.getCoreContainer().getZkController().zkStateReader;
      Aliases aliases = zkStateReader.getAliases();
      Map<String, String> props = new HashMap<>(aliases.getCollectionAliasProperties(aliasName));
      start = DateTimeFormatter.ISO_INSTANT.format(startTime);
      props.put(ROUTER_START, start);

      // This could race, but it only occurs when the alias is first used and the values produced
      // should all be identical and who wins won't matter (baring cases of Date Math involving seconds,
      // which is pretty far fetched). Putting this in a separate thread to ensure that any failed
      // races don't cause documents to get rejected.
      core.runAsync(() -> zkStateReader.aliasesManager.applyModificationAndExportToZk(
          (a) -> aliases.cloneWithCollectionAliasProperties(aliasName, props)));

    }
    if (docTimestamp.isBefore(startTime)) {
      throw new SolrException(BAD_REQUEST, "The document couldn't be routed because " + docTimestamp +
          " is before the start time for this alias " +start+")");
    }
  }


  @Override
  public Map<String, String> getAliasMetadata() {
    return aliasMetadata;
  }

  @Override
  public Set<String> getRequiredParams() {
    return REQUIRED_ROUTER_PARAMS;
  }

  @Override
  public Set<String> getOptionalParams() {
    return OPTIONAL_ROUTER_PARAMS;
  }


  @Override
  protected String getHeadCollectionIfOrdered(AddUpdateCommand cmd) {
    return parsedCollectionsDesc.get(0).getValue();
  }


  private Instant calcPreemptNextColCreateTime(String preemptiveCreateMath, Instant nextCollTimestamp) {
    DateMathParser dateMathParser = new DateMathParser();
    dateMathParser.setNow(Date.from(nextCollTimestamp));
    try {
      return dateMathParser.parseMath(preemptiveCreateMath).toInstant();
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Invalid Preemptive Create Window Math:'" + preemptiveCreateMath + '\'', e);
    }
  }

  private Instant parseRouteKey(Object routeKey) {
    final Instant docTimestamp;
    if (routeKey instanceof Instant) {
      docTimestamp = (Instant) routeKey;
    } else if (routeKey instanceof Date) {
      docTimestamp = ((Date) routeKey).toInstant();
    } else if (routeKey instanceof CharSequence) {
      docTimestamp = Instant.parse((CharSequence) routeKey);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unexpected type of routeKey: " + routeKey);
    }
    return docTimestamp;
  }

  /**
   * Given the route key, finds the correct collection and an indication of any collection that needs to be created.
   * Future docs will potentially cause creation of a collection that does not yet exist. This method presumes that the
   * doc time stamp has already been checked to not exceed maxFutureMs
   *
   * @throws SolrException if the doc is too old to be stored in the TRA
   */
  @Override
  public CandidateCollection findCandidateGivenValue(AddUpdateCommand cmd) {
    Object value = cmd.getSolrInputDocument().getFieldValue(getRouteField());
    ZkStateReader zkStateReader = cmd.getReq().getCore().getCoreContainer().getZkController().zkStateReader;
    String printableId = cmd.getPrintableId();
    updateParsedCollectionAliases(zkStateReader, true);

    final Instant docTimestamp = parseRouteKey(value);

    // reparse explicitly such that if we are a dimension in a DRA, the list gets culled by our context
    // This does not normally happen with the above updateParsedCollectionAliases, because at that point the aliases
    // should be up to date and updateParsedCollectionAliases will short circuit
    this.parsedCollectionsDesc = parseCollections(zkStateReader.getAliases());
    CandidateCollection next1 = calcCandidateCollection(docTimestamp);
    if (next1 != null) return next1;

    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Doc " + printableId + " couldn't be routed with " + getRouteField() + "=" + docTimestamp);
  }

  private CandidateCollection calcCandidateCollection(Instant docTimestamp) {
    // Lookup targetCollection given route key.  Iterates in reverse chronological order.
    //    We're O(N) here but N should be small, the loop is fast, and usually looking for 1st.
    Instant next = null;
    if (this.parsedCollectionsDesc.isEmpty()) {
      String firstCol = computeInitialCollectionName();
      return new CandidateCollection(SYNCHRONOUS, firstCol);
    } else {
      Instant mostRecentCol = parsedCollectionsDesc.get(0).getKey();

      // despite most logic hinging on the first element, we must loop so we can complain if the doc
      // is too old and there's no valid collection.
      for (int i = 0; i < parsedCollectionsDesc.size(); i++) {
        Map.Entry<Instant, String> entry = parsedCollectionsDesc.get(i);
        Instant colStartTime = entry.getKey();
        if (i == 0) {
          next = computeNextCollTimestamp(colStartTime);
        }
        if (!docTimestamp.isBefore(colStartTime)) {  // (inclusive lower bound)
          CandidateCollection candidate;
          if (i == 0) {
            if (docTimestamp.isBefore(next)) {       // (exclusive upper bound)
              candidate = new CandidateCollection(NONE, entry.getValue()); //found it
              // simply goes to head collection no action required
            } else {
              // Although we create collections one at a time, this calculation of the ultimate destination is
              // useful for contextualizing TRA's used as dimensions in DRA's
              String creationCol = calcNextCollection(colStartTime);
              Instant colDestTime = colStartTime;
              Instant possibleDestTime = colDestTime;
              while (!docTimestamp.isBefore(possibleDestTime) || docTimestamp.equals(possibleDestTime)) {
                colDestTime = possibleDestTime;
                possibleDestTime = computeNextCollTimestamp(colDestTime);
              }
              String destCol = TimeRoutedAlias.formatCollectionNameFromInstant(getAliasName(),colDestTime);
              candidate = new CandidateCollection(SYNCHRONOUS, destCol, creationCol); //found it
            }
          } else {
            // older document simply goes to existing collection, nothing created.
            candidate = new CandidateCollection(NONE, entry.getValue()); //found it
          }

          if (candidate.getCreationType() == NONE && isNotBlank(getPreemptiveCreateWindow()) && !this.preemptiveCreateOnceAlready) {
            // are we getting close enough to the (as yet uncreated) next collection to warrant preemptive creation?
            Instant time2Create = calcPreemptNextColCreateTime(getPreemptiveCreateWindow(), computeNextCollTimestamp(mostRecentCol));
            if (!docTimestamp.isBefore(time2Create)) {
              String destinationCollection = candidate.getDestinationCollection(); // dest doesn't change
              String creationCollection = calcNextCollection(mostRecentCol);
              return new CandidateCollection(ASYNC_PREEMPTIVE, // add next collection
                  destinationCollection,
                  creationCollection);
            }
          }
          return candidate;
        }
      }
    }
    return null;
  }

  /**
   * Deletes some of the oldest collection(s) based on {@link TimeRoutedAlias#getAutoDeleteAgeMath()}. If
   * getAutoDelteAgemath is not present then this method does nothing. Per documentation is relative to a
   * collection being created. Therefore if nothing is being created, nothing is deleted.
   * @param actions The previously calculated add action(s). This collection should not be modified within
   *                this method.
   */
  private List<Action> calcDeletes(List<Action> actions) {
    final String autoDeleteAgeMathStr = this.getAutoDeleteAgeMath();
    if (autoDeleteAgeMathStr == null || actions .size() == 0) {
      return Collections.emptyList();
    }
    if (actions.size() > 1) {
      throw new IllegalStateException("We are not supposed to be creating more than one collection at a time");
    }

    String deletionReferenceCollection = actions.get(0).targetCollection;
    Instant deletionReferenceInstant = parseInstantFromCollectionName(getAliasName(), deletionReferenceCollection);
    final Instant delBefore;
    try {
      delBefore = new DateMathParser(Date.from(computeNextCollTimestamp(deletionReferenceInstant)), this.getTimeZone()).parseMath(autoDeleteAgeMathStr).toInstant();
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e); // note: should not happen by this point
    }

    List<Action> collectionsToDelete = new ArrayList<>();

    //iterating from newest to oldest, find the first collection that has a time <= "before".  We keep this collection
    // (and all newer to left) but we delete older collections, which are the ones that follow.
    int numToKeep = 0;
    DateTimeFormatter dtf = null;
    if (log.isDebugEnabled()) {
      dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.n", Locale.ROOT);
      dtf = dtf.withZone(ZoneId.of("UTC"));
    }
    for (Map.Entry<Instant, String> parsedCollection : parsedCollectionsDesc) {
      numToKeep++;
      final Instant colInstant = parsedCollection.getKey();
      if (colInstant.isBefore(delBefore) || colInstant.equals(delBefore)) {
        if (log.isDebugEnabled()) { // don't perform formatting unless debugging
          assert dtf != null;
          log.debug("{} is equal to or before {} deletions may be required", dtf.format(colInstant), dtf.format(delBefore));
        }
        break;
      } else {
        if (log.isDebugEnabled()) { // don't perform formatting unless debugging
          assert dtf != null;
          log.debug("{} is not before {} and will be retained", dtf.format(colInstant), dtf.format(delBefore));
        }
      }
    }

    log.debug("Collections will be deleted... parsed collections={}", parsedCollectionsDesc);
    final List<String> targetList = parsedCollectionsDesc.stream().map(Map.Entry::getValue).collect(Collectors.toList());
    log.debug("Iterating backwards on collection list to find deletions: {}", targetList);
    for (int i = parsedCollectionsDesc.size() - 1; i >= numToKeep; i--) {
      String toDelete = targetList.get(i);
      log.debug("Adding to TRA delete list:{}", toDelete);

      collectionsToDelete.add(new Action(this, ActionType.ENSURE_REMOVED, toDelete));
    }
    return collectionsToDelete;
  }

  private List<Action> calcAdd(String targetCol) {
    List<String> collectionList = getCollectionList(parsedCollectionsAliases);
    if (!collectionList.contains(targetCol) && !collectionList.isEmpty()) {
      // Then we need to add the next one... (which may or may not be the same as our target
      String mostRecentCol = collectionList.get(0);
      String pfx = getRoutedAliasType().getSeparatorPrefix();
      int sepLen = mostRecentCol.contains(pfx) ? pfx.length() : 1; // __TRA__ or _
      String mostRecentTime = mostRecentCol.substring(getAliasName().length() + sepLen);
      Instant parsed = DATE_TIME_FORMATTER.parse(mostRecentTime, Instant::from);
      String nextCol = calcNextCollection(parsed);
      return Collections.singletonList(new Action(this, ActionType.ENSURE_EXISTS, nextCol));
    } else {
      return Collections.emptyList();
    }
  }

  private String calcNextCollection(Instant mostRecentCollTimestamp) {
    final Instant nextCollTimestamp = computeNextCollTimestamp(mostRecentCollTimestamp);
    return TimeRoutedAlias.formatCollectionNameFromInstant(aliasName, nextCollTimestamp);
  }

  @Override
  protected List<Action> calculateActions(String targetCol) {
    List<Action> actions = new ArrayList<>();
    actions.addAll(calcAdd(targetCol));
    actions.addAll(calcDeletes(actions));
    return actions;
  }


}
