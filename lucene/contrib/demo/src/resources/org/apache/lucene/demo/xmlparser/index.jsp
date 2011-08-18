<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
 
        http://www.apache.org/licenses/LICENSE-2.0
 
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
import="org.apache.lucene.search.*,org.apache.lucene.document.*"
pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
 	<link rel="stylesheet" type="text/css" href="stylesheet.css">
	<title>XML Query Parser demo</title>
</head>
<body>
<h1>Job Search</h1>
<%
			// Load form variables
			String description=request.getParameter("description");
			String type=request.getParameter("type");
			String salaryRange=request.getParameter("salaryRange"); 
%>
<form method="POST" action="FormBasedXmlQueryDemo">
<table >
	<tr>
		<th class="formHeader">Description</th>
		<td>
			<input name="description" value="<%=description==null?"":description%>"/>
		</td>
	</tr>  
	<tr>
		<th class="formHeader">Type</th> 
		<td> 
 			<select name="type">
 				<option value="" <%=type==null?"selected":""%>>Any</option>
 				<option value="Contract" <%="Contract".equals(type)?"selected":""%>>Contract</option>
				<option value="Permanent" <%="Permanent".equals(type)?"selected":""%>>Permanent</option>
			</select> 
		</td> 
	</tr>
	<tr>       
		<th class="formHeader">Salary</th> 
		<td>
 			<select name="salaryRange">
 				<option value="" <%=salaryRange==null?"selected":""%>>Any</option>
<%
				String ranges[]={"20","30","40", "50","60","70","80","90","100","110","120","150","200"};
				for(int i=1;i<ranges.length;i++)
				{
					String rangeText=ranges[i-1]+"-"+ranges[i];
%>				
 					<option value="<%=rangeText%>" <%=rangeText.equals(salaryRange)?"selected":""%>><%=ranges[i-1]%> to <%=ranges[i]%>k</option>
<%
				}
%>
			</select> 
		</td> 
	</tr>	
		
	<tr>
		<th class="formHeader">Locations</th>  
		<td>   
<%
		String locs[]={"South","North","East","West"};  
		boolean allLocsBlank=true;
		for(int i=0;i<locs.length;i++)
		{			
			if(request.getParameter(locs[i])!=null)
			{
				allLocsBlank=false;
			}
		}
		for(int i=0;i<locs.length;i++)
		{			
%>		
			<input id='cb<%=locs[i]%>'  
				name="<%=locs[i]%>" 
<%
				if((allLocsBlank)||("on".equals(request.getParameter(locs[i])))) 	{
%>
					checked="checked" 			
<%				}	
%>								
				type="checkbox"/>
			<label for="cb<%=locs[i]%>"><%=locs[i]%></label>
<%
		}
%>		
		</td>		
	</tr>
	
	<tr>
		<th></th>
		<td>
			<input type="submit" value="search"/>
		</td>		
	</tr>
</table>
</form>	 
<%
		Document[] results=(Document[])request.getAttribute("results");
		if(results!=null)
		{
%>
			<table width="600">
				<tr>
					<th class="resultsHeader">Type</th>
					<th class="resultsHeader">Location</th>
					<th class="resultsHeader">Salary</th>
					<th class="resultsHeader">Description</th>
				</tr>
			<%
						for (int i = 0; i < results.length; i++)
						{
							Document doc = results[i];
			%>
				<tr class="resultsRow">  
					<td><%=doc.get("type")%></td>
					<td><%=doc.get("location")%></td>
					<td class="resultNum"><%=doc.get("salary")%>,000</td>
					<td><%=doc.get("description")%></td>
				</tr>

			<%			
						}
			%>	
			</table>
			
<% 					
		}//end if has results
%>	
</body>
</html>
