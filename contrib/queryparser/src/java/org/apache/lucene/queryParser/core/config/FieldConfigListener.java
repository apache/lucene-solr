package org.apache.lucene.queryParser.core.config;

/**
 * This interface should be implemented by classes that wants to listen for
 * field configuration requests. The implementation receives a
 * {@link FieldConfig} object and may add/change its attributes.
 * 
 * @see FieldConfig
 * @see QueryConfigHandler
 */
public interface FieldConfigListener {

  /**
   * This method is called ever time a field configuration is requested.
   * 
   * @param fieldConfig
   *          the field configuration requested, should never be null
   */
  void buildFieldConfig(FieldConfig fieldConfig);

}
