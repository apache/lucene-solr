# Copyright:: Copyright (c) 2007 Apache Software Foundation
# License::   Apache Version 2.0 (see http://www.apache.org/licenses/)

# Methods added to this helper will be available to all templates in the application.
module ApplicationHelper
  def facet_label(field)
     field.match(/(.*)_.*/)[1].humanize.downcase
  end
end
