CREATE TABLE nodes (
	id UUID PRIMARY KEY, 
	title VARCHAR NOT NULL, 
	notes VARCHAR NOT NULL
);

CREATE TABLE connections (
	from_node UUID,
	to_node	UUID,
	notes VARCHAR NOT NULL, 
	flow REAL NOT NULL, 
	PRIMARY KEY (from_node, to_node) 
);

