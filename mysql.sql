create table signals
(
	timestamp timestamp default '1970-01-01 02:00:00' not null,
	rssi int not null,
	stations_id int not null,
	distributed_tags_id int not null
)
;

create index signals_stations_id_fk
	on signals (stations_id)
;

create index signals_tags_id_fk
	on signals (distributed_tags_id)
;

create index signals_timestamp_index
	on signals (timestamp)
;

create index signals_rssi_index
	on signals (rssi)
;

create table stations
(
	id int not null
		primary key,
	zones_id int not null
)
;

create index stations_zones_id_fk
	on stations (zones_id)
;

alter table signals
	add constraint signals_stations_stations_id_fk
		foreign key (stations_id) references waytation.stations (id)
;

create table tags
(
	id int not null
		primary key,
	active_from timestamp default '1970-01-01 02:00:00' not null,
	active_to timestamp default '1970-01-01 02:00:00' not null
)
;

alter table signals
	add constraint signals_tags_id_fk
		foreign key (distributed_tags_id) references waytation.tags (id)
;

create table zones
(
	id int not null
		primary key,
	name varchar(20) not null
)
;

alter table stations
	add constraint stations_zones_id_fk
		foreign key (zones_id) references waytation.zones (id)
;


