// Lucene Search Query Constructor
// Author:  Kelvin Tan  (kelvin@relevanz.com)
// Date:    14/02/2002
// Version: 1.1

// Change this according to what you use to name the field modifiers in your form.
// e.g. with the field "name", the modifier will be called "nameModifier"
var modifierSuffix = 'Modifier';

// Do you wish the query to be displayed as an alert box?
var debug = true;

// Do you wish the function to submit the form upon query construction?
var submitOnConstruction = true;

// Constructs the query
// @param query Form field to represent the constructed query to be submitted
function doMakeQuery( query )
{
  var frm = query.form;
  var formElements = frm.elements;
  query.value = '';
  for(var i=0; i<formElements.length; i++)
  {
    var element = formElements[i];
    var elementName = element.name;
    var elementValue = element.value;
    if(elementValue.length > 0)
    {
      for(var j=0; j<formElements.length; j++)
      {
        var subElement = formElements[j];
        if(subElement.name == (elementName + modifierSuffix))
        {
          var subElementValue = subElement.options[subElement.selectedIndex].value;
          if(subElementValue == 'And')
          {
            addAnd(query, elementName, elementValue);
          }     
          else if(subElementValue == 'Not')
          {
            addNot(query, elementName, elementValue);
          }
          else if(subElementValue == 'Or')
          {
            addOr(query, elementName, elementValue);
          }
        }
      }
    }
  }
  
  if(debug)
  {
    alert('Query:' + query.value);
  }
  
  if(submitOnConstruction)
  {
    frm.submit();
  }
}

function addOr(query, field, fieldValue)
{
  if(query.value.length == 0)
  {
    query.value = '(' + field + ':(' + fieldValue + '))';
  }
  else
  {
    query.value = query.value + ' (' + field + ':(' + fieldValue + '))';
  }  
}

function addAnd(query, field, fieldValue)
{
  if(query.value.length == 0)
  {
    query.value = '+(' + field + ':(' + fieldValue + '))';
  }
  else
  {
    query.value = query.value + ' +(' + field + ':(' + fieldValue + '))';
  }  
}

function addNot(query, field, fieldValue)
{
  if(query.value.length == 0)
  {
    query.value = '-(' + field + ':(' + fieldValue + '))';
  }
  else
  {
    query.value = query.value + ' -(' + field + ':(' + fieldValue + '))';
  }  
}