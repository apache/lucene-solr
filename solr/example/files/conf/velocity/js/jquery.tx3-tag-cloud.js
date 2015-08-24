/*
 * ----------------------------------------------------------------------------
 * "THE BEER-WARE LICENSE" (Revision 42):
 * Tuxes3 wrote this file. As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return Tuxes3
 * ----------------------------------------------------------------------------
 */
(function($)
{
  var settings;
    $.fn.tx3TagCloud = function(options)
    {

      //
      // DEFAULT SETTINGS
      //
      settings = $.extend({
        multiplier    : 1
      }, options);
      main(this);

    }

    function main(element)
    {
      // adding style attr
      element.addClass("tx3-tag-cloud");
      addListElementFontSize(element);
    }

    /**
     * calculates the font size on each li element 
     * according to their data-weight attribut
     */
    function addListElementFontSize(element)
    {
      var hDataWeight = -9007199254740992;
      var lDataWeight = 9007199254740992;
      $.each(element.find("li"), function(){
        cDataWeight = getDataWeight(this);
        if (cDataWeight == undefined)
        {
          logWarning("No \"data-weight\" attribut defined on <li> element");
        }
        else
        {
          hDataWeight = cDataWeight > hDataWeight ? cDataWeight : hDataWeight;
          lDataWeight = cDataWeight < lDataWeight ? cDataWeight : lDataWeight;
        }
      });
      $.each(element.find("li"), function(){
        var dataWeight = getDataWeight(this);
        var percent = Math.abs((dataWeight - lDataWeight)/(lDataWeight - hDataWeight));
        $(this).css('font-size', (1 + (percent * settings['multiplier'])) + "em");
      });

    }

    function getDataWeight(element)
    {
      return parseInt($(element).attr("data-weight"));
    }

    function logWarning(message)
    {
      console.log("[WARNING] " + Date.now() + " : " + message);
    }

}(jQuery));