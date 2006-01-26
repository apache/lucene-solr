<%@ page import=""%>
<%@ page contentType="text/plain;charset=UTF-8" language="java" %>
<%
  java.util.Enumeration e = System.getProperties().propertyNames();
  while(e.hasMoreElements()) {
    String prop = (String)e.nextElement();
    out.println(prop + " = " + System.getProperty(prop));
  }
%>