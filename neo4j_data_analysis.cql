match (n:restaurants) with count(n.cuisine) as total match (n:restaurants) return (n.cuisine),count (n.cuisine),((100.0*((count(n.cuisine))))/total) as percent order by count(n.cuisine)  DESC Limit 1

match (n:restaurants) with count(n.cuisine) as total match (n:restaurants) where (n.cuisine) = "American " return (n.borough),count (n.cuisine), ((100.0*((count(n.cuisine))))/total) as percent order by n.borough DESC Limit 1

match (n:restaurants) with count(n.cuisine) as total match (n:restaurants)-[:restaurants_address]->(a:address) where n.cuisine = "American " and n.borough = "Staten Island" return a.zipcode,Count(n.cuisine),((100.0*((count(n.cuisine))))/total) as percent order by a.zipcode LIMIT 1
