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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.solr.core.SolrCore;
import org.apache.solr.common.SolrException;
import static org.apache.solr.common.SolrException.ErrorCode.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.util.plugin.SolrCoreAware;


/**
 * Base class for implementing Factories for FieldMutatingUpdateProcessors and 
 * FieldValueMutatingUpdateProcessors.
 *
 * <p>
 * This class provides all of the plumbing for configuring the 
 * FieldNameSelector using the following init params to specify selection 
 * criteria...
 * </p>
 * <ul>
 *   <li><code>fieldName</code> - selecting specific fields by field name lookup</li>
 *   <li><code>fieldRegex</code> - selecting specific fields by field name regex match (regexes are checked in the order specified)</li>
 *   <li><code>typeName</code> - selecting specific fields by fieldType name lookup</li>
 *   <li><code>typeClass</code> - selecting specific fields by fieldType class lookup, including inheritence and interfaces</li>
 * </ul>
 *
 * <p>
 * Each criteria can specified as either an &lt;arr&gt; of &lt;str&gt;, or 
 * multiple &lt;str&gt; with the same name.  When multiple criteria of a 
 * single type exist, fields must match <b>at least one</b> to be selected.  
 * If more then one type of criteria exist, fields must match 
 * <b>at least one of each</b> to be selected.
 * </p>
 * <p>
 * One or more <code>excludes</code> &lt;lst&gt; params may also be specified, 
 * containing any of the above criteria, identifying fields to be excluded 
 * from seelction even if they match the selection criteria.  As with the main 
 * selection critiera a field must match all of criteria in a single exclusion 
 * in order to be excluded, but multiple exclusions may be specified to get an
 * <code>OR</code> behavior
 * </p>
 *
 * <p>
 * In the ExampleFieldMutatingUpdateProcessorFactory configured below, 
 * fields will be mutated if the name starts with "foo" <i>or</i> "bar"; 
 * <b>unless</b> the field name contains the substring "SKIP" <i>or</i> 
 * the fieldType is (or subclasses) DateField.  Meaning a field named 
 * "foo_SKIP" is gaurunteed not to be selected, but a field named "bar_smith" 
 * that uses StrField will be selected.
 * </p>
 * <pre class="prettyprint">
 * &lt;processor class="solr.ExampleFieldMutatingUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldRegex"&gt;foo.*&lt;/str&gt;
 *   &lt;str name="fieldRegex"&gt;bar.*&lt;/str&gt;
 *   &lt;!-- each set of exclusions is checked independently --&gt;
 *   &lt;lst name="exclude"&gt;
 *     &lt;str name="fieldRegex"&gt;.*SKIP.*&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="exclude"&gt;
 *     &lt;str name="typeClass"&gt;solr.DateField&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/processor&gt;
 * </pre>
 * 
 * <p>
 * Subclasses define the default selection behavior to be applied if no 
 * criteria is configured by the user.  User configured "exclude" criteria 
 * will be applied to the subclass defined default selector.
 * </p>
 * 
 * @see FieldMutatingUpdateProcessor
 * @see FieldValueMutatingUpdateProcessor
 * @see FieldMutatingUpdateProcessor.FieldNameSelector
 */
public abstract class FieldMutatingUpdateProcessorFactory
  extends UpdateRequestProcessorFactory 
  implements SolrCoreAware {
  
  public static final class SelectorParams {
    public Set<String> fieldName = Collections.emptySet();
    public Set<String> typeName = Collections.emptySet();
    public Collection<String> typeClass = Collections.emptyList();
    public Collection<Pattern> fieldRegex = Collections.emptyList();
  }

  private SelectorParams inclusions = new SelectorParams();
  private Collection<SelectorParams> exclusions 
    = new ArrayList<SelectorParams>();

  private FieldMutatingUpdateProcessor.FieldNameSelector selector = null;
  
  protected final FieldMutatingUpdateProcessor.FieldNameSelector getSelector() {
    if (null != selector) return selector;

    throw new SolrException(SERVER_ERROR, "selector was never initialized, "+
                            " inform(SolrCore) never called???");
  }

  @SuppressWarnings("unchecked")
  public static SelectorParams parseSelectorParams(NamedList args) {
    SelectorParams params = new SelectorParams();
    
    params.fieldName = new HashSet<String>(oneOrMany(args, "fieldName"));
    params.typeName = new HashSet<String>(oneOrMany(args, "typeName"));

    // we can compile the patterns now
    Collection<String> patterns = oneOrMany(args, "fieldRegex");
    if (! patterns.isEmpty()) {
      params.fieldRegex = new ArrayList<Pattern>(patterns.size());
      for (String s : patterns) {
        try {
          params.fieldRegex.add(Pattern.compile(s));
        } catch (PatternSyntaxException e) {
          throw new SolrException
            (SERVER_ERROR, "Invalid 'fieldRegex' pattern: " + s, e);
        }
      }
    }
    
    // resolve this into actual Class objects later
    params.typeClass = oneOrMany(args, "typeClass");

    return params;
  }
                                                            

  /**
   * Handles common initialization related to source fields for 
   * constructoring the FieldNameSelector to be used.
   *
   * Will error if any unexpected init args are found, so subclasses should
   * remove any subclass-specific init args before calling this method.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void init(NamedList args) {

    inclusions = parseSelectorParams(args);

    List<Object> excList = args.getAll("exclude");
    for (Object excObj : excList) {
      if (null == excObj) {
        throw new SolrException
          (SERVER_ERROR, "'exclude' init param can not be null"); 
      }
      if (! (excObj instanceof NamedList) ) {
        throw new SolrException
          (SERVER_ERROR, "'exclude' init param must be <lst/>"); 
      }
      NamedList exc = (NamedList) excObj;
      exclusions.add(parseSelectorParams(exc));
      if (0 < exc.size()) {
        throw new SolrException(SERVER_ERROR, 
                                "Unexpected 'exclude' init sub-param(s): '" + 
                                args.getName(0) + "'");
      }
      // call once per instance
      args.remove("exclude");
    }
    if (0 < args.size()) {
      throw new SolrException(SERVER_ERROR, 
                              "Unexpected init param(s): '" + 
                              args.getName(0) + "'");
    }

  }

  public void inform(final SolrCore core) {
    
    final IndexSchema schema = core.getSchema();

    selector = 
      FieldMutatingUpdateProcessor.createFieldNameSelector
      (core.getResourceLoader(),
       core.getSchema(),
       inclusions.fieldName,
       inclusions.typeName,
       inclusions.typeClass,
       inclusions.fieldRegex,
       getDefaultSelector(core));

    for (SelectorParams exc : exclusions) {
      selector = FieldMutatingUpdateProcessor.wrap
        (selector,
         FieldMutatingUpdateProcessor.createFieldNameSelector
         (core.getResourceLoader(),
          core.getSchema(),
          exc.fieldName,
          exc.typeName,
          exc.typeClass,
          exc.fieldRegex,
          FieldMutatingUpdateProcessor.SELECT_NO_FIELDS));
    }
  }
  
  /**
   * Defines the default selection behavior when the user has not 
   * configured any specific criteria for selecting fields. The Default 
   * implementation matches all fields, and should be overridden by subclasses 
   * as needed.
   * 
   * @see FieldMutatingUpdateProcessor#SELECT_ALL_FIELDS
   */
  protected FieldMutatingUpdateProcessor.FieldNameSelector 
    getDefaultSelector(final SolrCore core) {

    return FieldMutatingUpdateProcessor.SELECT_ALL_FIELDS;

  }

  /**
   * Removes all instance of the key from NamedList, returning the Set of 
   * Strings that key refered to.  Throws an error if the key didn't refer 
   * to one or more strings (or arrays of strings)
   * @exception SolrException invalid arr/str structure.
   */
  public static Collection<String> oneOrMany(final NamedList args, final String key) {
    List<String> result = new ArrayList<String>(args.size() / 2);
    final String err = "init arg '" + key + "' must be a string "
      + "(ie: 'str'), or an array (ie: 'arr') containing strings; found: ";
    
    for (Object o = args.remove(key); null != o; o = args.remove(key)) {
      if (o instanceof String) {
        result.add((String)o);
        continue;
      }
      
      if (o instanceof Object[]) {
        o = Arrays.asList((Object[]) o);
      }
      
      if (o instanceof Collection) {
        for (Object item : (Collection)o) {
          if (! (item instanceof String)) {
            throw new SolrException(SERVER_ERROR, err + item.getClass());
          }
          result.add((String)item);
        }
        continue;
      }
      
      // who knows what the hell we have
      throw new SolrException(SERVER_ERROR, err + o.getClass());
    }
    
    return result;
  }

}



