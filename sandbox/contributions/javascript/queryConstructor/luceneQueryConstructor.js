// Lucene Search Query Constructor
// Author:  Kelvin Tan  (kelvin at relevanz.com)

// Change this according to what you use to name the field modifiers in your form.
// e.g. with the field "name", the modifier will be called "nameModifier"
var modifierSuffix = 'Modifier';

// Do you wish the query to be displayed as an alert box?
var debug = false;

// Do you wish the function to submit the form upon query construction?
var submitOnConstruction = false;

// prefix modifier for boolean AND queries
var AND_MODIFIER = '+';

// prefix modifier for boolean NOT queries
var NOT_MODIFIER = '-';

// prefix modifier for boolean OR queries
var OR_MODIFIER  = '';

// default prefix modifier for boolean queries
var DEFAULT_MODIFIER = OR_MODIFIER;

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
          var subElementValue;
          
          // support drop-down select lists, radio buttons and text fields
          if(subElement.type == "select")
          {
            subElementValue = subElement.options[subElement.selectedIndex].value;
          }
          else if(subElement.type == "radio")
          {
            // radio button elements often have the same element name, 
            // so ensure we have the right one
            if(subElement.checked)
            {
              subElementValue = subElement.value;              
            }
            else
            {
              continue;
            }
          }
          else
          {
            subElementValue = subElement.value;
          }
          
          if(subElementValue == 'And')
          {
            addFieldWithModifier(query, AND_MODIFIER, elementName, elementValue);
          }     
          else if(subElementValue == 'Not')
          {
            addFieldWithModifier(query, NOT_MODIFIER, elementName, elementValue);
          }
          else if(subElementValue == 'Or')
          {
            addFieldWithModifier(query, OR_MODIFIER, elementName, elementValue);
          }
          else
          {
            addFieldWithModifier(query, DEFAULT_MODIFIER, elementName, elementValue);
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

function addFieldWithModifier(query, modifier, field, fieldValue)
{
  if(query.value.length == 0)
  {
    query.value = modifier + '(' + field + ':(' + fieldValue + '))';
  }
  else
  {
    query.value = query.value + ' ' + modifier + '(' + field + ':(' + fieldValue + '))';
  }  
}