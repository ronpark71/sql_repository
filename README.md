Here is an inventory description of my sample SQL library:

<b>heap_events_create_consolidated_events_view.sql</b> - Redshift SQL script that creates a view for every Heap event table, and then creates a consolidated view on top of all the event views, so that users can see the representation of all events in one place with consolidated column names.  Query is not Heap data specific, and can be used in any situation where you want to automatically merge tables into a single view.
