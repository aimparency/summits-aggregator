CREATE TABLE nodes (
	id UUID PRIMARY KEY, 
	title VARCHAR NOT NULL, 
	notes VARCHAR NOT NULL
);

CREATE TABLE flows (
	from_id UUID,
	into_id UUID,
	notes VARCHAR NOT NULL, 
	share REAL NOT NULL, 
	PRIMARY KEY (from_id, into_id) 
);

