<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<%@ taglib uri="/WEB-INF/lucene-taglib.tld" prefix="JSP"%>
<%@ include file="header.jsp"%>
<%@ page import="java.util.*"%>

<% 
	String startRow = "0";
	String maxRows = "10";
	String query = request.getParameter("query");
	try{
		startRow = request.getParameter("startRow");
		maxRows = request.getParameter("maxRows");
	}
	catch(Exception e){
	}
%>

<table border=3>

	<JSP:Search throwOnException="false" id="rs" collection="E:/opt/lucene/index" criteria="<%= query %>" startRow="<%= startRow %>" maxRows="<%= maxRows %>">
	<%
		
		Set allFields = rs.getFields();
		int fieldSize = allFields.size();
		Iterator fieldIter = allFields.iterator();
		
		while(fieldIter.hasNext()){
			String nextField = (String) fieldIter.next();
			if(!nextField.equalsIgnoreCase("summary")){
	%>
				<tr><td><b><%= nextField %></b></td><td><%= rs.getField(nextField) %></td></tr>
			<%
			}else{
			%>
				<tr><td colspan="2"><b><%= rs.hitCount %>|<%= nextField %></b></td></tr>
				<tr><td colspan="2"><%= rs.getField(nextField) %></td></tr>
			<%
			}
		}
		
	%>
	</JSP:Search>
<%
	int count = 0;
	try{
		count = new Integer(rs.hitCount).intValue();
	}catch(Exception e){
		out.print(e);
	}
	if(count <= 0){
%>
	<tr>
		<td colspan=2>No results have been found</td>
	</tr>
<%
	}
%>
	<tr>
		<td colspan=2><%= rs.hitCount %></td>
	</tr>
	</table>
	

<%@include file="footer.jsp"%>
