with tt as (select permit_class, permit_status, year(issued_date) as year from tree_permits)
pivot tt on (permit_status) using count() group by permit_class, year order by all nulls last
