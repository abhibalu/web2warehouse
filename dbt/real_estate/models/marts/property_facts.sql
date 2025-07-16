select
  p.id as property_id,
  p.price,
  p.bedroom,
  p.bathroom,
  ri.room_name,
  ri.dimensions
from {{ ref('property_details') }} p
left join {{ ref('property_details_room_items') }} ri
  on p.id = ri.id
