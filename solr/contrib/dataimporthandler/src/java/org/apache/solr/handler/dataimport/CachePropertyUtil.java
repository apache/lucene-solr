package org.apache.solr.handler.dataimport;

public class CachePropertyUtil {
  public static String getAttributeValueAsString(Context context, String attr) {
    Object o = context.getSessionAttribute(attr, Context.SCOPE_ENTITY);
    if (o == null) {
      o = context.getResolvedEntityAttribute(attr);
    }
    if (o == null && context.getRequestParameters() != null) {
      o = context.getRequestParameters().get(attr);
    }
    if (o == null) {
      return null;
    }
    return o.toString();
  }
  
  public static Object getAttributeValue(Context context, String attr) {
    Object o = context.getSessionAttribute(attr, Context.SCOPE_ENTITY);
    if (o == null) {
      o = context.getResolvedEntityAttribute(attr);
    }
    if (o == null && context.getRequestParameters() != null) {
      o = context.getRequestParameters().get(attr);
    }
    if (o == null) {
      return null;
    }
    return o;
  }
  
}
