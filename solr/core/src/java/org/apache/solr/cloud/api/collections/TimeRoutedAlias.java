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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.google.common.base.MoreObjects;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.RoutedAliasUpdateProcessor;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.CreationType.ASYNC_PREEMPTIVE;
import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.CreationType.NONE;
import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.CreationType.SYNCHRONOUS;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CollectionAdminParams.ROUTER_PREFIX;
import static org.apache.solr.common.params.CommonParams.TZ;

/**
 * Holds configuration for a routed alias, and some common code and constants.
 *
 * @see CreateAliasCmd
 * @see MaintainTimeRoutedAliasCmd
 * @see RoutedAliasUpdateProcessor
 */
public class TimeRoutedAlias implements RoutedAlias {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // This class is created once per request and the overseer methods prevent duplicate create requests
  // from creating extra copies. All we need to track here is that we don't spam preemptive creates to
  // the overseer multiple times from *this* request.
  private volatile boolean preemptiveCreateOnceAlready = false;

  // These two fields may be updated within the calling thread during processing but should
  // never be updated by any async creation thread.
  private List<Map.Entry<Instant, String>> parsedCollectionsDesc; // k=timestamp (start), v=collection.  Sorted descending
  private Aliases parsedCollectionsAliases; // a cached reference to the source of what we parse into parsedCollectionsDesc

  // These are parameter names to routed alias creation, AND are stored as metadata with the alias.
  public static final String ROUTER_START = ROUTER_PREFIX + "start";
  public static final String ROUTER_INTERVAL = ROUTER_PREFIX + "interval";
  public static final String ROUTER_MAX_FUTURE = ROUTER_PREFIX + "maxFutureMs";
  public static final String ROUTER_AUTO_DELETE_AGE = ROUTER_PREFIX + "autoDeleteAge";
  public static final String ROUTER_PREEMPTIVE_CREATE_MATH = ROUTER_PREFIX + "preemptiveCreateMath";
  // plus TZ and NAME

  /**
   * Parameters required for creating a routed alias
   */
  public static final Set<String> REQUIRED_ROUTER_PARAMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      CommonParams.NAME,
      ROUTER_TYPE_NAME,
      ROUTER_FIELD,
      ROUTER_START,
      ROUTER_INTERVAL)));

  /**
   * Optional parameters for creating a routed alias excluding parameters for collection creation.
   */
  //TODO lets find a way to remove this as it's harder to maintain than required list
  public static final Set<String> OPTIONAL_ROUTER_PARAMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      ROUTER_MAX_FUTURE,
      ROUTER_AUTO_DELETE_AGE,
      ROUTER_PREEMPTIVE_CREATE_MATH,
      TZ))); // kinda special


  // This format must be compatible with collection name limitations
  private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
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
    if (!"time".equals(required.get(ROUTER_TYPE_NAME))) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only 'time' routed aliases is supported right now.");
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
      final Date after = new DateMathParser(now, timeZone).parseMath(intervalMath);
      if (!after.after(now)) {
        throw new SolrException(BAD_REQUEST, "duration must add to produce a time in the future");
      }
    } catch (Exception e) {
      throw new SolrException(BAD_REQUEST, "bad " + TimeRoutedAlias.ROUTER_INTERVAL + ", " + e, e);
    }

    if (autoDeleteAgeMath != null) {
      try {
        final Date before =  new DateMathParser(now, timeZone).parseMath(autoDeleteAgeMath);
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

  public static Instant parseInstantFromCollectionName(String aliasName, String collection) {
    final String dateTimePart = collection.substring(aliasName.length() + 1);
    return DATE_TIME_FORMATTER.parse(dateTimePart, Instant::from);
  }

  public static String formatCollectionNameFromInstant(String aliasName, Instant timestamp) {
    String nextCollName = DATE_TIME_FORMATTER.format(timestamp);
    for (int i = 0; i < 3; i++) { // chop off seconds, minutes, hours
      if (nextCollName.endsWith("_00")) {
        nextCollName = nextCollName.substring(0, nextCollName.length()-3);
      }
    }
    assert DATE_TIME_FORMATTER.parse(nextCollName, Instant::from).equals(timestamp);
    return aliasName + "_" + nextCollName;
  }

  Instant parseStringAsInstant(String str, TimeZone zone) {
    Instant start = DateMathParser.parseMath(new Date(), str, zone).toInstant();
    checkMilis(start);
    return start;
  }

  private void checkMilis(Instant date) {
    if (!date.truncatedTo(ChronoUnit.SECONDS).equals(date)) {
      throw new SolrException(BAD_REQUEST,
          "Date or date math for start time includes milliseconds, which is not supported. " +
              "(Hint: 'NOW' used without rounding always has this problem)");
    }
  }

  @Override
  public boolean updateParsedCollectionAliases(ZkController zkController) {
    final Aliases aliases = zkController.getZkStateReader().getAliases(); // note: might be different from last request
    if (this.parsedCollectionsAliases != aliases) {
      if (this.parsedCollectionsAliases != null) {
        log.debug("Observing possibly updated alias: {}", getAliasName());
      }
      this.parsedCollectionsDesc = parseCollections(aliases );
      this.parsedCollectionsAliases = aliases;
      return true;
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

  public String getIntervalMath() {
    return intervalMath;
  }

  public long getMaxFutureMs() {
    return maxFutureMs;
  }

  public String getPreemptiveCreateWindow() {
    return preemptiveCreateMath;
  }

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
  List<Map.Entry<Instant,String>> parseCollections(Aliases aliases) {
    final List<String> collections = aliases.getCollectionAliasListMap().get(aliasName);
    if (collections == null) {
      throw RoutedAlias.newAliasMustExistException(getAliasName());
    }
    // note: I considered TreeMap but didn't like the log(N) just to grab the most recent when we use it later
    List<Map.Entry<Instant,String>> result = new ArrayList<>(collections.size());
    for (String collection : collections) {
      Instant colStartTime = parseInstantFromCollectionName(aliasName, collection);
      result.add(new AbstractMap.SimpleImmutableEntry<>(colStartTime, collection));
    }
    result.sort((e1, e2) -> e2.getKey().compareTo(e1.getKey())); // reverse sort by key
    return result;
  }

  /** Computes the timestamp of the next collection given the timestamp of the one before. */
  public Instant computeNextCollTimestamp(Instant fromTimestamp) {
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
  }

  @Override
  public String createCollectionsIfRequired(AddUpdateCommand cmd) {
    SolrQueryRequest req = cmd.getReq();
    SolrCore core = req.getCore();
    CoreContainer coreContainer = core.getCoreContainer();
    CollectionsHandler collectionsHandler = coreContainer.getCollectionsHandler();
    final Instant docTimestamp =
        parseRouteKey(cmd.getSolrInputDocument().getFieldValue(getRouteField()));

    // Even though it is possible that multiple requests hit this code in the 1-2 sec that
    // it takes to create a collection, it's an established anti-pattern to feed data with a very large number
    // of client connections. This in mind, we only guard against spamming the overseer within a batch of
    // updates. We are intentionally tolerating a low level of redundant requests in favor of simpler code. Most
    // super-sized installations with many update clients will likely be multi-tenant and multiple tenants
    // probably don't write to the same alias. As such, we have deferred any solution to the "many clients causing
    // collection creation simultaneously" problem until such time as someone actually has that problem in a
    // real world use case that isn't just an anti-pattern.
    Map.Entry<Instant, String> candidateCollectionDesc = findCandidateGivenTimestamp(docTimestamp, cmd.getPrintableId());
    String candidateCollectionName = candidateCollectionDesc.getValue();

    try {
      switch (typeOfCreationRequired(docTimestamp, candidateCollectionDesc.getKey())) {
        case SYNCHRONOUS:
          // This next line blocks until all collections required by the current document have been created
          return createAllRequiredCollections(docTimestamp, cmd, candidateCollectionDesc);
        case ASYNC_PREEMPTIVE:
          if (!preemptiveCreateOnceAlready) {
            log.debug("Executing preemptive creation for {}", getAliasName());
            // It's important not to add code between here and the prior call to findCandidateGivenTimestamp()
            // in processAdd() that invokes updateParsedCollectionAliases(). Doing so would update parsedCollectionsDesc
            // and create a race condition. We are relying on the fact that get(0) is returning the head of the parsed
            // collections that existed when candidateCollectionDesc was created. If this class updates it's notion of
            // parsedCollectionsDesc since candidateCollectionDesc was chosen, we could create collection n+2
            // instead of collection n+1.
            String mostRecentCollName = this.parsedCollectionsDesc.get(0).getValue();

            // This line does not block and the document can be added immediately
            preemptiveAsync(() -> createNextCollection(mostRecentCollName, collectionsHandler), core);
          }
          return candidateCollectionName;
        case NONE:
          return candidateCollectionName; // could use fall through, but fall through is fiddly for later editors.
        default:
          throw unknownCreateType();
      }
      // do nothing if creationType == NONE
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
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

  /**
   * Create as many collections as required. This method loops to allow for the possibility that the docTimestamp
   * requires more than one collection to be created. Since multiple threads may be invoking maintain on separate
   * requests to the same alias, we must pass in the name of the collection that this thread believes to be the most
   * recent collection. This assumption is checked when the command is executed in the overseer. When this method
   * finds that all collections required have been created it returns the (possibly new) most recent collection.
   * The return value is ignored by the calling code in the async preemptive case.
   *
   * @param docTimestamp the timestamp from the document that determines routing
   * @param cmd the update command being processed
   * @param targetCollectionDesc the descriptor for the presently selected collection which should also be
   *                             the most recent collection in all cases where this method is invoked.
   * @return The latest collection, including collections created during maintenance
   */
  private String createAllRequiredCollections( Instant docTimestamp, AddUpdateCommand cmd,
                                               Map.Entry<Instant, String> targetCollectionDesc) {
    SolrQueryRequest req = cmd.getReq();
    SolrCore core = req.getCore();
    CoreContainer coreContainer = core.getCoreContainer();
    CollectionsHandler collectionsHandler = coreContainer.getCollectionsHandler();
    do {
      switch(typeOfCreationRequired(docTimestamp, targetCollectionDesc.getKey())) {
        case NONE:
          return targetCollectionDesc.getValue(); // we don't need another collection
        case ASYNC_PREEMPTIVE:
          // can happen when preemptive interval is longer than one time slice
          String mostRecentCollName = this.parsedCollectionsDesc.get(0).getValue();
          preemptiveAsync(() -> createNextCollection(mostRecentCollName, collectionsHandler), core);
          return targetCollectionDesc.getValue();
        case SYNCHRONOUS:
          createNextCollection(targetCollectionDesc.getValue(), collectionsHandler); // *should* throw if fails for some reason but...
          ZkController zkController = coreContainer.getZkController();
          if (!updateParsedCollectionAliases(zkController)) { // thus we didn't make progress...
            // this is not expected, even in known failure cases, but we check just in case
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "We need to create a new time routed collection but for unknown reasons were unable to do so.");
          }
          // then retry the loop ... have to do find again in case other requests also added collections
          // that were made visible when we called updateParsedCollectionAliases()
          targetCollectionDesc = findCandidateGivenTimestamp(docTimestamp, cmd.getPrintableId());
          break;
        default:
          throw unknownCreateType();

      }
    } while (true);
  }

  private SolrException unknownCreateType() {
    return new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown creation type while adding " +
        "document to a Time Routed Alias! This is a bug caused when a creation type has been added but " +
        "not all code has been updated to handle it.");
  }

  private void createNextCollection(String mostRecentCollName, CollectionsHandler collHandler) {
    // Invoke ROUTEDALIAS_CREATECOLL (in the Overseer, locked by alias name).  It will create the collection
    //   and update the alias contingent on the most recent collection name being the same as
    //   what we think so here, otherwise it will return (without error).
    try {
      MaintainTimeRoutedAliasCmd.remoteInvoke(collHandler, getAliasName(), mostRecentCollName);
      // we don't care about the response.  It's possible no collection was created because
      //  of a race and that's okay... we'll ultimately retry any way.

      // Ensure our view of the aliases has updated. If we didn't do this, our zkStateReader might
      //  not yet know about the new alias (thus won't see the newly added collection to it), and we might think
      //  we failed.
      collHandler.getCoreContainer().getZkController().getZkStateReader().aliasesManager.update();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private void preemptiveAsync(Runnable r, SolrCore core) {
    preemptiveCreateOnceAlready = true;
    core.runAsync(r);
  }

  /**
   * Determine if the a new collection will be required based on the document timestamp. Passing null for
   * preemptiveCreateInterval tells you if the document is beyond all existing collections with a response of
   * {@link CreationType#NONE} or {@link CreationType#SYNCHRONOUS}, and passing a valid date math for
   * preemptiveCreateMath additionally distinguishes the case where the document is close enough to the end of
   * the TRA to trigger preemptive creation but not beyond all existing collections with a value of
   * {@link CreationType#ASYNC_PREEMPTIVE}.
   *
   * @param docTimeStamp The timestamp from the document
   * @param targetCollectionTimestamp The timestamp for the presently selected destination collection
   * @return a {@code CreationType} indicating if and how to create a collection
   */
  private CreationType typeOfCreationRequired(Instant docTimeStamp, Instant targetCollectionTimestamp) {
    final Instant nextCollTimestamp = computeNextCollTimestamp(targetCollectionTimestamp);

    if (!docTimeStamp.isBefore(nextCollTimestamp)) {
      // current document is destined for a collection that doesn't exist, must create the destination
      // to proceed with this add command
      return SYNCHRONOUS;
    }

    if (isNotBlank(getPreemptiveCreateWindow())) {
      Instant preemptNextColCreateTime =
          calcPreemptNextColCreateTime(getPreemptiveCreateWindow(), nextCollTimestamp);
      if (!docTimeStamp.isBefore(preemptNextColCreateTime)) {
        return ASYNC_PREEMPTIVE;
      }
    }

    return NONE;
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
      docTimestamp = ((Date)routeKey).toInstant();
    } else if (routeKey instanceof CharSequence) {
      docTimestamp = Instant.parse((CharSequence)routeKey);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unexpected type of routeKey: " + routeKey);
    }
    return docTimestamp;
  }
  /**
   * Given the route key, finds the correct collection or returns the most recent collection if the doc
   * is in the future. Future docs will potentially cause creation of a collection that does not yet exist
   * or an error if they exceed the maxFutureMs setting.
   *
   * @throws SolrException if the doc is too old to be stored in the TRA
   */
  private Map.Entry<Instant, String> findCandidateGivenTimestamp(Instant docTimestamp, String printableId) {
    // Lookup targetCollection given route key.  Iterates in reverse chronological order.
    //    We're O(N) here but N should be small, the loop is fast, and usually looking for 1st.
    for (Map.Entry<Instant, String> entry : parsedCollectionsDesc) {
      Instant colStartTime = entry.getKey();
      if (!docTimestamp.isBefore(colStartTime)) {  // i.e. docTimeStamp is >= the colStartTime
        return entry; //found it
      }
    }
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Doc " + printableId + " couldn't be routed with " + getRouteField() + "=" + docTimestamp);
  }

  enum CreationType {
    NONE,
    ASYNC_PREEMPTIVE,
    SYNCHRONOUS
  }

}
