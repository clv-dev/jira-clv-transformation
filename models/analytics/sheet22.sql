	SELECT
		"Key"
		, sprint 
		, status 
		, summary 
		, to_timestamp(updated, 'MM/DD/YYYY HH24:MI:SS') AS updated 
		, assignee 
		, to_date("End Date", 'MM/DD/YYYY') AS end_date
		, "Issue Type" 
		, to_date("Start date", 'MM/DD/YYYY') AS start_date
		, "Est. Story Points" 
	FROM postgres.intermediate.sheet21