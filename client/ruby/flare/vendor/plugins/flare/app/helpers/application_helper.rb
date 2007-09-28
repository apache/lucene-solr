module ApplicationHelper
  def facet_label(field)
     field.match(/(.*)_.*/)[1].humanize.downcase
  end
end
