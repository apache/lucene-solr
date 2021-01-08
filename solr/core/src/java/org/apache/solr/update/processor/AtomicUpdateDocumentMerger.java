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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocumentBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.NumericValueFieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;

/**
 * @lucene.experimental
 */
public class AtomicUpdateDocumentMerger {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  protected final IndexSchema schema;
  protected final SchemaField idField;
  
  public AtomicUpdateDocumentMerger(SolrQueryRequest queryReq) {
    schema = queryReq.getSchema();
    idField = schema.getUniqueKeyField();
  }
  
  /**
   * Utility method that examines the SolrInputDocument in an AddUpdateCommand
   * and returns true if the documents contains atomic update instructions.
   */
  public static boolean isAtomicUpdate(final AddUpdateCommand cmd) {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    for (SolrInputField sif : sdoc.values()) {
      Object val = sif.getValue();
      if (val instanceof Map && !(val instanceof SolrDocumentBase)) {
        return true;
      }
    }
    
    return false;
  }

  /**
   * Merges the fromDoc into the toDoc using the atomic update syntax.
   * This method will look for a nested document (possibly {@code toDoc} itself) with an
   * equal ID, and merge into that one.
   * @param sdoc the doc containing update instructions
   * @param toDoc the target doc (possibly nested) before the update (will be modified in-place)
   * @return toDoc with modifications; never null
   */
  public SolrInputDocument merge(SolrInputDocument sdoc, SolrInputDocument toDoc) {
    if (mergeChildDocRecursive(sdoc, getRequiredId(sdoc), toDoc)) {
      return toDoc;
    }
    throw new IllegalStateException("Did not find child ID " + getRequiredId(sdoc) +
        " in parent ID " + getRequiredId(toDoc));
  }

  private boolean mergeChildDocRecursive(SolrInputDocument sdoc, Object sdocId, SolrInputDocument docWithChildren) {
    if (sdocId.equals(getRequiredId(docWithChildren))) {
      mergeDocHavingSameId(sdoc, docWithChildren);
      return true;
    }
    for (SolrInputField inputField : docWithChildren) {
      final Collection<Object> values = inputField.getValues();
      if (values == null) {
        continue;
      }
      for (Object value : values) {
        if (isChildDoc(value)) {
          if (mergeChildDocRecursive(sdoc, sdocId, (SolrInputDocument) value)) {
            return true;
          } // else continue the search
        }
      }
    }
    return false;
  }

  private String getRequiredId(SolrInputDocument sdoc) {
    String id = schema.printableUniqueKey(sdoc);
    if (id == null) {
      throw new IllegalStateException("partial updates require that docs have an ID");
    }
    return id;
  }

  /**
   * Merges the fromDoc into the toDoc using the atomic update syntax.
   * 
   * @param fromDoc SolrInputDocument which will merged into the toDoc
   * @param toDoc the final SolrInputDocument that will be mutated with the values from the fromDoc atomic commands
   * @return toDoc with mutated values
   */
  @SuppressWarnings({"unchecked"})
  private SolrInputDocument mergeDocHavingSameId(final SolrInputDocument fromDoc, SolrInputDocument toDoc) {
    for (SolrInputField sif : fromDoc.values()) {
     Object val = sif.getValue();
      if (val instanceof Map) {
        for (Entry<String,Object> entry : ((Map<String,Object>) val).entrySet()) {
          String key = entry.getKey();
          Object fieldVal = entry.getValue();
          switch (key) {
            case "add":
              doAdd(toDoc, sif, fieldVal);
              break;
            case "set":
              doSet(toDoc, sif, fieldVal);
              break;
            case "remove":
              doRemove(toDoc, sif, fieldVal);
              break;
            case "removeregex":
              doRemoveRegex(toDoc, sif, fieldVal);
              break;
            case "inc":
              doInc(toDoc, sif, fieldVal);
              break;
            case "add-distinct":
              doAddDistinct(toDoc, sif, fieldVal);
              break;
            default:
              throw new SolrException(ErrorCode.BAD_REQUEST,
                  "Error:" + getID(toDoc, schema) + " Unknown operation for the an atomic update: " + key);
          }
          // validate that the field being modified is not the id field.
          if (idField.getName().equals(sif.getName())) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid update of id field: " + sif);
          }

        }
      } else {
        // normal fields are treated as a "set"
        toDoc.put(sif.getName(), sif);
      }
    }
    
    return toDoc;
  }

  private static String getID(SolrInputDocument doc, IndexSchema schema) {
    String id = "";
    SchemaField sf = schema.getUniqueKeyField();
    if (sf != null) {
      id = "[doc="+doc.getFieldValue( sf.getName() )+"] ";
    }
    return id;
  }

  /**
   * Given a schema field, return whether or not such a field is supported for an in-place update.
   * Note: If an update command has updates to only supported fields (and _version_ is also supported),
   * only then is such an update command executed as an in-place update.
   */
  public static boolean isSupportedFieldForInPlaceUpdate(SchemaField schemaField) {
    return !(schemaField.indexed() || schemaField.stored() || !schemaField.hasDocValues() || 
        schemaField.multiValued() || !(schemaField.getType() instanceof NumericValueFieldType));
  }
  
  /**
   * Given an add update command, compute a list of fields that can be updated in-place. If there is even a single
   * field in the update that cannot be updated in-place, the entire update cannot be executed in-place (and empty set
   * will be returned in that case).
   * 
   * @return Return a set of fields that can be in-place updated.
   */
  @SuppressWarnings({"unchecked"})
  public static Set<String> computeInPlaceUpdatableFields(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    IndexSchema schema = cmd.getReq().getSchema();
    
    final SchemaField uniqueKeyField = schema.getUniqueKeyField();
    final String uniqueKeyFieldName = null == uniqueKeyField ? null : uniqueKeyField.getName();

    final Set<String> candidateFields = new HashSet<>();

    // if _version_ field is not supported for in-place update, bail out early
    SchemaField versionField = schema.getFieldOrNull(CommonParams.VERSION_FIELD);
    if (versionField == null || !isSupportedFieldForInPlaceUpdate(versionField)) {
      return Collections.emptySet();
    }
    
    String routeFieldOrNull = getRouteField(cmd);
    // first pass, check the things that are virtually free,
    // and bail out early if anything is obviously not a valid in-place update
    for (String fieldName : sdoc.getFieldNames()) {
      Object fieldValue = sdoc.getField(fieldName).getValue();
      if (fieldName.equals(uniqueKeyFieldName)
          || fieldName.equals(IndexSchema.ROOT_FIELD_NAME)
          || fieldName.equals(CommonParams.VERSION_FIELD)
          || fieldName.equals(routeFieldOrNull)) {
        if (fieldValue instanceof Map) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Updating unique key, version or route field is not allowed: " + sdoc.getField(fieldName));
        } else {
          continue;
        }
      }
      if (! (fieldValue instanceof Map) ) {
        // not an in-place update if there are fields that are not maps
        return Collections.emptySet();
      }
      // else it's a atomic update map...
      Map<String, Object> fieldValueMap = (Map<String, Object>)fieldValue;
      for (Entry<String, Object> entry : fieldValueMap.entrySet()) {
        String op = entry.getKey();
        Object obj = entry.getValue();
        if (!op.equals("set") && !op.equals("inc")) {
          // not a supported in-place update op
          return Collections.emptySet();
        } else if (op.equals("set") && (obj == null || (obj instanceof Collection && ((Collection) obj).isEmpty()))) {
          // when operation is set and value is either null or empty list
          // treat the update as atomic instead of inplace
          return Collections.emptySet();
        }
        // fail fast if child doc
        if(isChildDoc(((Map<String, Object>) fieldValue).get(op))) {
          return Collections.emptySet();
        }
      }
      candidateFields.add(fieldName);
    }

    if (candidateFields.isEmpty()) {
      return Collections.emptySet();
    }

    // second pass over the candidates for in-place updates
    // this time more expensive checks involving schema/config settings
    for (String fieldName: candidateFields) {
      SchemaField schemaField = schema.getField(fieldName);

      if (!isSupportedFieldForInPlaceUpdate(schemaField)) {
        return Collections.emptySet();
      } 

      // if this field has copy target which is not supported for in place, then empty
      for (CopyField copyField: schema.getCopyFieldsList(fieldName)) {
        if (!isSupportedFieldForInPlaceUpdate(copyField.getDestination()))
          return Collections.emptySet();
      }
    }
    
    // third pass: requiring checks against the actual IndexWriter due to internal DV update limitations
    SolrCore core = cmd.getReq().getCore();
    RefCounted<IndexWriter> holder = core.getSolrCoreState().getIndexWriter(core);
    Set<String> segmentSortingFields = null;
    try {
      IndexWriter iw = holder.get();
      segmentSortingFields = iw.getConfig().getIndexSortFields();
    } finally {
      holder.decref();
    }
    for (String fieldName: candidateFields) {
      if (segmentSortingFields.contains(fieldName) ) {
        return Collections.emptySet(); // if this is used for segment sorting, DV updates can't work
      }
    }
    
    return candidateFields;
  }

  private static String getRouteField(AddUpdateCommand cmd) {
    String result = null;
    SolrCore core = cmd.getReq().getCore();
    CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    if (cloudDescriptor != null) {
      String collectionName = cloudDescriptor.getCollectionName();
      ZkController zkController = core.getCoreContainer().getZkController();
      DocCollection collection = zkController.getClusterState().getCollection(collectionName);
      result = collection.getRouter().getRouteField(collection);
    }
    return result;
  }
  
  /**
   *
   * @param fullDoc the full doc to  be compared against
   * @param partialDoc the sub document to be tested
   * @return whether partialDoc is derived from fullDoc
   */
  public static boolean isDerivedFromDoc(SolrInputDocument fullDoc, SolrInputDocument partialDoc) {
    for(SolrInputField subSif: partialDoc) {
      Collection<Object> fieldValues = fullDoc.getFieldValues(subSif.getName());
      if (fieldValues == null) return false;
      if (fieldValues.size() < subSif.getValueCount()) return false;
      Collection<Object> partialFieldValues = subSif.getValues();
      // filter all derived child docs from partial field values since they fail List#containsAll check (uses SolrInputDocument#equals which fails).
      // If a child doc exists in partialDoc but not in full doc, it will not be filtered, and therefore List#containsAll will return false
      Stream<Object> nonChildDocElements = partialFieldValues.stream().filter(x -> !(isChildDoc(x) &&
          (fieldValues.stream().anyMatch(y ->
              (isChildDoc(x) &&
                  isDerivedFromDoc((SolrInputDocument) y, (SolrInputDocument) x)
              )
          )
          )));
      if (!nonChildDocElements.allMatch(fieldValues::contains)) return false;
    }
    return true;
  }

  /**
   *
   * @param completeHierarchy SolrInputDocument that represents the nested document hierarchy from its root
   * @param fieldPath the path to fetch, separated by a '/' e.g. /children/grandChildren
   * @return the SolrInputField of fieldPath
   */
  public static SolrInputField getFieldFromHierarchy(SolrInputDocument completeHierarchy, String fieldPath) {
    // substr to remove first '/'
    final List<String> docPaths = StrUtils.splitSmart(fieldPath.substring(1), '/');
    Pair<String, Integer> subPath;
    SolrInputField sifToReplace = null;
    SolrInputDocument currDoc = completeHierarchy;
    for (String subPathString: docPaths) {
      subPath = getPathAndIndexFromNestPath(subPathString);
      sifToReplace = currDoc.getField(subPath.getLeft());
      currDoc = (SolrInputDocument) ((List)sifToReplace.getValues()).get(subPath.getRight());
    }
    return sifToReplace;
  }
  
  /**
   * Given an AddUpdateCommand containing update operations (e.g. set, inc), merge and resolve the operations into
   * a partial document that can be used for indexing the in-place updates. The AddUpdateCommand is modified to contain
   * the partial document (instead of the original document which contained the update operations) and also
   * the prevVersion that this in-place update depends on.
   * Note: updatedFields passed into the method can be changed, i.e. the version field can be added to the set.
   * @return If in-place update cannot succeed, e.g. if the old document is deleted recently, then false is returned. A false
   *        return indicates that this update can be re-tried as a full atomic update. Returns true if the in-place update
   *        succeeds.
   */
  public boolean doInPlaceUpdateMerge(AddUpdateCommand cmd, Set<String> updatedFields) throws IOException {
    SolrInputDocument inputDoc = cmd.getSolrInputDocument();
    BytesRef rootIdBytes = cmd.getIndexedId();
    BytesRef idBytes = schema.indexableUniqueKey(cmd.getChildDocIdStr());

    updatedFields.add(CommonParams.VERSION_FIELD); // add the version field so that it is fetched too
    SolrInputDocument oldDocument = RealTimeGetComponent.getInputDocument
      (cmd.getReq().getCore(), idBytes, rootIdBytes,
          null, // don't want the version to be returned
          updatedFields, RealTimeGetComponent.Resolution.DOC);

    if (oldDocument == RealTimeGetComponent.DELETED || oldDocument == null) {
      // This doc was deleted recently. In-place update cannot work, hence a full atomic update should be tried.
      return false;
    }

    if (oldDocument.containsKey(CommonParams.VERSION_FIELD) == false) {
      throw new SolrException (ErrorCode.INVALID_STATE, "There is no _version_ in previous document. id=" + 
          cmd.getPrintableId());
    }
    Long oldVersion = (Long) oldDocument.remove(CommonParams.VERSION_FIELD).getValue();

    // If the oldDocument contains any other field apart from updatedFields (or id/version field), then remove them.
    // This can happen, despite requesting for these fields in the call to RTGC.getInputDocument, if the document was
    // fetched from the tlog and had all these fields (possibly because it was a full document ADD operation).
    if (updatedFields != null) {
      Collection<String> names = new HashSet<>(oldDocument.getFieldNames());
      for (String fieldName: names) {
        if (fieldName.equals(CommonParams.VERSION_FIELD)==false && fieldName.equals(ID)==false && updatedFields.contains(fieldName)==false) {
          oldDocument.remove(fieldName);
        }
      }
    }
    // Copy over all supported DVs from oldDocument to partialDoc
    //
    // Assuming multiple updates to the same doc: field 'dv1' in one update, then field 'dv2' in a second
    // update, and then again 'dv1' in a third update (without commits in between), the last update would
    // fetch from the tlog the partial doc for the 2nd (dv2) update. If that doc doesn't copy over the
    // previous updates to dv1 as well, then a full resolution (by following previous pointers) would
    // need to be done to calculate the dv1 value -- so instead copy all the potentially affected DV fields.
    SolrInputDocument partialDoc = new SolrInputDocument();
    String uniqueKeyField = schema.getUniqueKeyField().getName();
    for (String fieldName : oldDocument.getFieldNames()) {
      SchemaField schemaField = schema.getField(fieldName);
      if (fieldName.equals(uniqueKeyField) || isSupportedFieldForInPlaceUpdate(schemaField)) {
        partialDoc.addField(fieldName, oldDocument.getFieldValue(fieldName));
      }
    }

    mergeDocHavingSameId(inputDoc, partialDoc);

    // Populate the id field if not already populated (this can happen since stored fields were avoided during fetch from RTGC)
    if (!partialDoc.containsKey(schema.getUniqueKeyField().getName())) {
      partialDoc.addField(idField.getName(), 
          inputDoc.getField(schema.getUniqueKeyField().getName()).getFirstValue());
    }

    cmd.prevVersion = oldVersion;
    cmd.solrDoc = partialDoc;
    return true;
  }

  protected void doSet(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    String name = sif.getName();
    toDoc.setField(name, getNativeFieldValue(name, fieldVal));
  }

  protected void doAdd(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    String name = sif.getName();
    toDoc.addField(name, getNativeFieldValue(name, fieldVal));
  }

  protected void doAddDistinct(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    final String name = sif.getName();
    SolrInputField existingField = toDoc.get(name);

    Collection<Object> original = existingField != null ?
        existingField.getValues() :
        new ArrayList<>();

    int initialSize = original.size();
    if (fieldVal instanceof Collection) {
      for (Object object : (Collection) fieldVal) {
        addValueIfDistinct(name, original, object);
      }
    } else {
      addValueIfDistinct(name, original, fieldVal);
    }

    if (original.size() > initialSize) { // update only if more are added
      if (original.size() == 1) { // if single value, pass the value instead of List
        doAdd(toDoc, sif, original.toArray()[0]);
      } else {
        toDoc.setField(name, original);
      }
    }
  }

  protected void doInc(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    SolrInputField numericField = toDoc.get(sif.getName());
    SchemaField sf = schema.getField(sif.getName());

    if (sf.getType().getNumberType() == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "'inc' is not supported on non-numeric field " + sf.getName());
    }

    if (numericField != null || sf.getDefaultValue() != null) {
      // TODO: fieldtype needs externalToObject?
      String oldValS = (numericField != null) ?
          numericField.getFirstValue().toString(): sf.getDefaultValue().toString();
      BytesRefBuilder term = new BytesRefBuilder();
      sf.getType().readableToIndexed(oldValS, term);
      Object oldVal = sf.getType().toObject(sf, term.get());

      // behavior similar to doAdd/doSet
      Object resObj = getNativeFieldValue(sf.getName(), fieldVal);
      if (!(resObj instanceof Number)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid input '" + resObj + "' for field " + sf.getName());
      }
      Number result = (Number)resObj;
      if (oldVal instanceof Long) {
        result = ((Long) oldVal).longValue() + result.longValue();
      } else if (oldVal instanceof Float) {
        result = ((Float) oldVal).floatValue() + result.floatValue();
      } else if (oldVal instanceof Double) {
        result = ((Double) oldVal).doubleValue() + result.doubleValue();
      } else {
        // int, short, byte
        result = ((Integer) oldVal).intValue() + result.intValue();
      }
      toDoc.setField(sif.getName(), result);
    } else {
      toDoc.setField(sif.getName(), fieldVal);
    }
  }

  protected void doRemove(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    final String name = sif.getName();
    SolrInputField existingField = toDoc.get(name);
    if (existingField == null) return;
    @SuppressWarnings({"rawtypes"})
    final Collection<Object> original = existingField.getValues();
    if (fieldVal instanceof Collection) {
      for (Object object : (Collection) fieldVal) {
        removeObj(original, object, name);
      }
    } else {
      removeObj(original, fieldVal, name);
    }

    toDoc.setField(name, original);
  }

  protected void doRemoveRegex(SolrInputDocument toDoc, SolrInputField sif, Object valuePatterns) {
    final String name = sif.getName();
    final SolrInputField existingField = toDoc.get(name);
    if (existingField != null) {
      final Collection<Object> valueToRemove = new HashSet<>();
      final Collection<Object> original = existingField.getValues();
      final Collection<Pattern> patterns = preparePatterns(valuePatterns);
      for (Object value : original) {
        for(Pattern pattern : patterns) {
          final Matcher m = pattern.matcher(value.toString());
          if (m.matches()) {
            valueToRemove.add(value);
          }
        }
      }
      original.removeAll(valueToRemove);
      toDoc.setField(name, original);
    }
  }

  private Collection<Pattern> preparePatterns(Object fieldVal) {
    final Collection<Pattern> patterns = new LinkedHashSet<>(1);
    if (fieldVal instanceof Collection) {
      @SuppressWarnings({"unchecked"})
      Collection<Object> patternVals = (Collection<Object>) fieldVal;
      for (Object patternVal : patternVals) {
        patterns.add(Pattern.compile(patternVal.toString()));
      }
    } else {
      patterns.add(Pattern.compile(fieldVal.toString()));
    }
    return patterns;
  }

  private Object getNativeFieldValue(String fieldName, Object val) {
    if (isChildDoc(val) || val == null || (val instanceof Collection && ((Collection) val).isEmpty())) {
      return val;
    }
    SchemaField sf = schema.getField(fieldName);
    try {
      return sf.getType().toNativeType(val);
    } catch (SolrException ex) {
      throw new SolrException(SolrException.ErrorCode.getErrorCode(ex.code()),
          "Error converting field '" + sf.getName() + "'='" +val+"' to native type, msg=" + ex.getMessage(), ex);
    } catch (Exception ex) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Error converting field '" + sf.getName() + "'='" +val+"' to native type, msg=" + ex.getMessage(), ex);
    }
  }

  private static boolean isChildDoc(Object obj) {
    if(!(obj instanceof Collection)) {
      return obj instanceof SolrDocumentBase;
    }
    @SuppressWarnings({"rawtypes"})
    Collection objValues = (Collection) obj;
    if(objValues.size() == 0) {
      return false;
    }
    return objValues.iterator().next() instanceof SolrDocumentBase;
  }

  private void removeObj(Collection<Object> original, Object toRemove, String fieldName) {
    if(isChildDoc(toRemove)) {
      removeChildDoc(original, (SolrInputDocument) toRemove);
    } else {
      removeFieldValueWithNumericFudging(fieldName, original, toRemove);
    }
  }

  @SuppressWarnings({"unchecked"})
  private static void removeChildDoc(@SuppressWarnings({"rawtypes"})Collection original, SolrInputDocument docToRemove) {
    for(SolrInputDocument doc: (Collection<SolrInputDocument>) original) {
      if(isDerivedFromDoc(doc, docToRemove)) {
        original.remove(doc);
        return;
      }
    }
  }

  private void removeFieldValueWithNumericFudging(String fieldName, @SuppressWarnings({"rawtypes"}) Collection<Object> original, Object toRemove) {
    if (original.size() == 0) {
      return;
    }

    final BiConsumer<Collection<Object>, Object> removePredicate = (coll, existingElement) -> coll.remove(existingElement);
    modifyCollectionBasedOnFuzzyPresence(fieldName, original, toRemove, removePredicate, null);
  }

  private void addValueIfDistinct(String fieldName, Collection<Object> original, Object toAdd) {
    final BiConsumer<Collection<Object>, Object> addPredicate = (coll, newElement) -> coll.add(newElement);
    modifyCollectionBasedOnFuzzyPresence(fieldName, original, toAdd, null, addPredicate);
  }

  /**
   * Modifies a collection based on the (loosely-judged) presence or absence of a specific value
   *
   * Several classes of atomic update (notably 'remove' and 'add-distinct') rely on being able to identify whether an
   * item is already present in a given list of values.  Unfortunately the 'item' being checked for may be of different
   * types based on the format of the user request and on where the existing document was pulled from (tlog vs index).
   * As a result atomic updates needs a "fuzzy" way of checking presence and equality that is more flexible than
   * traditional equality checks allow.  This method does light type-checking to catch some of these more common cases
   * (Long compared against Integers, String compared against Date, etc.), and calls the provided lambda to modify the
   * field values as necessary.
   *
   * @param fieldName the field name involved in this atomic update operation
   * @param original the list of values currently present in the existing document
   * @param rawValue a value to be checked for in 'original'
   * @param ifPresent a function to execute if rawValue was found in 'original'
   * @param ifAbsent a function to execute if rawValue was not found in 'original'
   */
  private void modifyCollectionBasedOnFuzzyPresence(String fieldName, Collection<Object> original, Object rawValue,
                                                    BiConsumer<Collection<Object>, Object> ifPresent,
                                                    BiConsumer<Collection<Object>, Object> ifAbsent) {
    Object nativeValue = getNativeFieldValue(fieldName, rawValue);
    Optional<Object> matchingValue = findObjectWithTypeFuzziness(original, rawValue, nativeValue);
    if (matchingValue.isPresent() && ifPresent != null) {
      ifPresent.accept(original, matchingValue.get());
    } else if( (!matchingValue.isPresent()) && ifAbsent != null) {
      ifAbsent.accept(original, rawValue);
    }
  }

  private Optional<Object> findObjectWithTypeFuzziness(Collection<Object> original, Object rawValue, Object nativeValue) {
    if (nativeValue instanceof Double || nativeValue instanceof Float) {
      final Number nativeAsNumber = (Number) nativeValue;
      return original.stream().filter(val ->
              val.equals(rawValue) ||
                      val.equals(nativeValue) ||
                      (val instanceof Number && ((Number) val).doubleValue() == nativeAsNumber.doubleValue()) ||
                      (val instanceof String && val.equals(nativeAsNumber.toString())))
              .findFirst();
    } else if (nativeValue instanceof Long || nativeValue instanceof Integer) {
      final Number nativeAsNumber = (Number) nativeValue;
      return original.stream().filter(val ->
              val.equals(rawValue) ||
                      val.equals(nativeValue) ||
                      (val instanceof Number && ((Number) val).longValue() == nativeAsNumber.longValue()) ||
                      (val instanceof String && val.equals(nativeAsNumber.toString())))
              .findFirst();
    } else if (nativeValue instanceof Date) {
      return original.stream().filter(val ->
              val.equals(rawValue) ||
                      val.equals(nativeValue) ||
                      (val instanceof String && DateMathParser.parseMath(null, (String)val).equals(nativeValue)))
              .findFirst();
    } else if (original.contains(nativeValue)) {
      return Optional.of(nativeValue);
    } else if (original.contains(rawValue)) {
      return Optional.of(rawValue);
    } else {
      return Optional.empty();
    }
  }

  private static Pair<String, Integer> getPathAndIndexFromNestPath(String nestPath) {
    List<String> splitPath = StrUtils.splitSmart(nestPath, '#');
    if(splitPath.size() == 1) {
      return Pair.of(splitPath.get(0), 0);
    }
    return Pair.of(splitPath.get(0), Integer.parseInt(splitPath.get(1)));
  }
  
}
