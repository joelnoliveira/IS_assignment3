CREATE TABLE route_supplier (
	id	 BIGSERIAL,
	name VARCHAR(512) NOT NULL,
	PRIMARY KEY(id)
);

CREATE TABLE route_capacity (
	id		 BIGSERIAL,
	capacity	 INTEGER NOT NULL,
	transport_type VARCHAR(512) NOT NULL,
	PRIMARY KEY(id)
);

ALTER TABLE route_supplier ADD UNIQUE (name);

