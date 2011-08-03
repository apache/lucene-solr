package org.apache.lucene.queryParser.core.config;

import org.apache.lucene.util.Attribute;

/**
 * This class should be used by every class that extends {@link Attribute} to
 * configure a {@link QueryConfigHandler}. It will be removed soon, it is only
 * used during the transition from old configuration API to new configuration
 * API.
 * 
 * @deprecated
 */
@Deprecated
public interface ConfigAttribute {

  void setQueryConfigHandler(AbstractQueryConfig config);
  
}
