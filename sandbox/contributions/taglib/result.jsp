<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<%@ taglib uri="/WEB-INF/lucene-taglib.tld" prefix="LUCENE"%>
<%@ page import="java.util.*"%>
<html>
<head>
	<title>Using Lucene Taglib</title>
	<style>
	BODY{background-color:#F5F5F5}
	</style>
</head>
<body>
<% 
	String startRow = "";
	String maxRows = "";
	String criteria = "";
	int colCount = 0;
	String analyzerType = "STANDARD_ANALYZER";
	String search = "contents";
	String stopWords="";
	String fieldList="";
	String flagList="";
	String collection = "E:/search/lucene/index,E:/opt/lucene/index";
	String throwOnException = "false";
	String fieldthrowOnException = "false";
	String columnthrowOnException = "false";
	String runOnce = "false";
	String query = "";
	
	try{
		query = request.getQueryString();
		criteria = request.getParameter("criteria");
		startRow = request.getParameter("startRow");
		maxRows = request.getParameter("maxRows");
		analyzerType = request.getParameter("analyzerType");
		search = request.getParameter("search");
		stopWords = request.getParameter("stopWords");
		fieldList = request.getParameter("fieldList");
		flagList = request.getParameter("flagList");
		throwOnException = request.getParameter("throwOnException");
		fieldthrowOnException = request.getParameter("fieldthrowOnException");
		columnthrowOnException = request.getParameter("columnthrowOnException");
		runOnce = request.getParameter("runOnce");
	}
	catch(Exception e){
	}
%>
<h3> Search results for "<%= criteria %>"</h3>
<table border=3>

	<LUCENE:Search id="rs" 
				throwOnException="<%= throwOnException %>" 
				collection="<%= collection %>" 
				criteria="<%= criteria %>" 
				startRow="<%= startRow %>" 
				maxRows="<%= maxRows %>"
				analyzerType="<%= analyzerType %>"
				search="<%= search %>"
				stopWords="<%= stopWords %>"
				fieldList="<%= fieldList %>"
				flagList="<%= flagList %>">
		<tr>
		<LUCENE:Column id="header" runOnce="true" throwOnException="false">
			<% colCount = header.columnCount; %>
			
			<th><b><%= header.columnName %></b></th>
		</LUCENE:Column>
		</tr>
		<LUCENE:Column id="col" throwOnException="<%= columnthrowOnException %>" runOnce="<%= runOnce %>">
		<tr>
			<td colspan="<%= col.columnCount %>"><b>[<%= rs.loopCount %>][<%= rs.rowCount %>]</b>&nbsp;
				<LUCENE:Field id="fld" name="<%= col.columnName %>" throwOnException="<%= fieldthrowOnException %>">
				
					<% if(col.columnName.equalsIgnoreCase("url")){ %>
						<a href="<%= fld.value %>">
					<% } %>
					
					<%= fld.value %>
					
					<% if(col.columnName.equalsIgnoreCase("url")){ %>
						</a>
					<% } %>
					
				</LUCENE:Field>
			</td>
		</tr>
		</LUCENE:Column>
	</LUCENE:Search>
<%
	if(rs.hitCount <= 0){
%>
	<tr>
		<td colspan=2>No results have been found</td>
	</tr>
<%
	}
%>
	<tr>
		<td><b>hitCount</b></td>
		<td colspan="<%= colCount-1 %>"><%= rs.hitCount %></td>
	</tr>
	<tr>
		<td><b>pageCount</b></td>
		<td colspan="<%= colCount-1 %>"><%= rs.pageCount %></td>
	</tr>
	<tr>
		<td><b>firstPage</b></td>
		<td colspan="<%= colCount-1 %>"><a href="<%= rs.firstPage %><%= query %>"><%= rs.firstPage %></a></td>
	</tr>
	<tr>
		<td><b>nextPage</b></td>
		<td colspan="<%= colCount-1 %>"><a href="<%= rs.nextPage %><%= query %>"><%= rs.nextPage %></a></td>
	</tr>
	<tr>
		<td><b>previousPage</b></td>
		<td colspan="<%= colCount-1 %>"><a href="<%= rs.previousPage %><%= query %>"><%= rs.previousPage %></a></td>
	</tr>
	<tr>
		<td><b>lastPage</b></td>
		<td colspan="<%= colCount-1 %>"><a href="<%= rs.lastPage %><%= query %>"><%= rs.lastPage %></a></td>
	</tr>
	<tr>
		<td colspan="<%= colCount %>">
		<select name="pagelist" onchange="location.href=this.value">
		<%
		Iterator pages = rs.pageList.iterator();
		while(pages.hasNext()){
			String listNext = (String) pages.next();
			%>
			<option value="<%= listNext %><%= query %>"><%= listNext %></option>
			<%
		}
		%>
		</select>
		</td>
	</tr>
	
</table>
</body>
</html>