/* 
    script for creating property tables for 
    curated property hotel, room, services, 
    amneties, facilities, and so on
    - first the DATABASE propert and 
        SCHEMA curated must be created
    - ensure user rezaware has priviledges to drop and
        create tables, views, and so on

    Contributors:
        nuwan.waidyanatha@rezgateway.com
        ushan.jayasooriya@colombo.rezgateway.com

*/
DROP TABLE IF EXISTS curated.prop_detail;
CREATE TABLE curated.prop_detail
(
  prop_pk serial primary key,
  prop_alt_uuid character varying (100),
  prop_name character varying (200),
  prop_grp_fk integer,
  cate_uref_fk integer,
  addr_line_1 character varying (200),
  addr_line_2 character varying (200),
  city_district character varying (200),
  city character varying (200),
  subdiv_type character varying (100),
  subdiv_3661_code character varying (3),
  country_3661_alpha3 character varying (3),
  post_code  character varying (100),
/*  gis_fk character varying (100), */
  description character varying (200),
  star_rating integer,
  rating_method character varying (100),
  review_score double precision,
  source_uuid character varying (200),
  data_source character varying(100),
  data_owner  character varying(100),
  created_dt timestamp without time zone,
  created_by character varying(100),
  created_proc character varying(100),
  modified_dt timestamp without time zone,
  modified_by character varying(100),
  modified_proc character varying(100),
  deactivate_dt timestamp without time zone
)
WITH (
  OIDS=FALSE
);
comment on column curated.prop_detail.prop_pk is 'system generated integer upon insert';
comment on column curated.prop_detail.prop_alt_uuid is 'alternated id defined by a previous process';
comment on column curated.prop_detail.prop_name is 'public name as displayed on hotel name board';
comment on column curated.prop_detail.prop_grp_fk is 'pk from prop_groups table';
comment on column curated.prop_detail.cate_uref_fk is 'name type categorical value from util_ref';
comment on column curated.prop_detail.addr_line_1 is 'physical address line 1 e.g. street name';
comment on column curated.prop_detail.addr_line_2 is 'physical address line 1 e.g. area or block';
comment on column curated.prop_detail.city_district is 'sub district in city name e.g. downtown or CBD';
comment on column curated.prop_detail.city is 'located city iso 3661 3 letter code';
comment on column curated.prop_detail.subdiv_type is 'admin subdivision type state province or county';
comment on column curated.prop_detail.subdiv_3661_code is 'iso code of the state province or country';
comment on column curated.prop_detail.country_3661_alpha3 is 'country iso 3661 3 letter code';
comment on column curated.prop_detail.post_code is 'area postal code';
/*comment on column curated.prop_detail.gis_fk is 'uuid to the GIS DB point or shape coordinate';*/
comment on column curated.prop_detail.description is 'either quailifier description about the hotel';
comment on column curated.prop_detail.star_rating is 'acredited entity assigned rating';
comment on column curated.prop_detail.rating_method is 'entity rated self or commercially rated';
comment on column curated.prop_detail.review_score is 'accredited entity assessed review score';
comment on column curated.prop_detail.source_uuid is 'uuid or pk of the record in the data source';
comment on column curated.prop_detail.data_source is 'storage data was taken e.g. S3 folder';
comment on column curated.prop_detail.data_owner is 'reference to the origin of the data';


DROP TABLE IF EXISTS curated.prop_alt_name;
CREATE TABLE curated.prop_alt_name
(
  alt_names_pk serial primary key,
  prop_detail_fk character varying (100),
  alt_name character varying (200),
  type_uref_fk integer,
  description character varying (200),
  source_uuid character varying (100),
  data_source character varying(200),
  data_owner  character varying(200),
  created_dt timestamp without time zone,
  created_by character varying(100),
  created_proc character varying(100),
  modified_dt timestamp without time zone,
  modified_by character varying(100),
  modified_proc character varying(100),
  deactivate_dt timestamp without time zone
)
WITH (
  OIDS=FALSE
);
comment on column curated.prop_alt_name.alt_names_pk is 'system generated integer upon insert';
comment on column curated.prop_alt_name.prop_detail_fk is 'alternated id defined by a previous process';
comment on column curated.prop_alt_name.alt_name is 'public name as displayed on hotel name board';
comment on column curated.prop_alt_name.type_uref_fk is 'name type categorical value from util_ref';
comment on column curated.prop_alt_name.description is 'description about the alternate name';
comment on column curated.prop_alt_name.source_uuid is 'uuid or pk of the record from the data source';
comment on column curated.prop_alt_name.data_source is 'storage data was taken e.g. S3 folder';
comment on column curated.prop_alt_name.data_owner is 'reference to the origin of the data';


DROP TABLE IF EXISTS curated.prop_grp;
CREATE TABLE curated.prop_grp
(
  prop_grp_pk serial primary key,
  grp_alt_uuid character varying (100),
  grp_name character varying (200),
  type_uref_fk character varying (200),
  addr_line_1 character varying (200),
  addr_line_2 character varying (200),
  city_district character varying (200),
  city character varying (200),
  subdiv_type character varying (100),
  subdiv_3661_code character varying (3),
  country_3661_alpha3 character varying (3),
  post_code character varying (100),
  grp_url character varying (200),
/*  gis_fk character varying (100),*/
  description character varying (200),
  star_rating integer,
  rating_method character varying (100),
  review_score double precision,
  source_uuid character varying (200),
  data_source character varying(100),
  data_owner  character varying(100),
  created_dt timestamp without time zone,
  created_by character varying(100),
  created_proc character varying(100),
  modified_dt timestamp without time zone,
  modified_by character varying(100),
  modified_proc character varying(100),
  deactivate_dt timestamp without time zone
)
WITH (
  OIDS=FALSE
);
comment on column curated.prop_grp.prop_grp_pk is 'system generated integer upon insert';
comment on column curated.prop_grp.grp_alt_uuid is 'alternated id defined by a previous process';
comment on column curated.prop_grp.grp_name is 'hotel branding name or common name';
comment on column curated.prop_grp.type_uref_fk is 'hotel group type e.g. resort hotel inn';
comment on column curated.prop_grp.addr_line_1 is 'physical address line 1 e.g. street name';
comment on column curated.prop_grp.addr_line_2 is 'physical address line 1 e.g. area or block';
comment on column curated.prop_grp.city is 'city name of Grp HQ';
comment on column curated.prop_grp.city_district is 'sub district in city name e.g. downtown or CBD';
comment on column curated.prop_grp.subdiv_type is 'admin subdivision type state province or county';
comment on column curated.prop_grp.subdiv_3661_code is 'iso code state province country of Grp HQ';
comment on column curated.prop_grp.country_3661_alpha3 is 'country iso code of Grp HQ';
comment on column curated.prop_grp.post_code is 'area postal code';
comment on column curated.prop_grp.grp_url is 'property group www URL';
/* comment on column curated.prop_grp.gis_fk is 'uuid to the GIS DB point or shape of Grp HQ'; */
comment on column curated.prop_grp.description is 'either quailifier description about the group';
comment on column curated.prop_grp.star_rating is 'acredited entity assigned rating';
comment on column curated.prop_grp.rating_method is 'entity rated self or commercially rated';
comment on column curated.prop_grp.review_score is 'accredited entity assessed group review score';
comment on column curated.prop_grp.source_uuid is 'uuid or pk of the record in the data source';
comment on column curated.prop_grp.data_source is 'storage data was taken e.g. S3 folder';
comment on column curated.prop_grp.data_owner is 'reference to the origin of the data';


DROP TABLE IF EXISTS curated.prop_room;
CREATE TABLE curated.prop_room
(
  room_pk serial primary key,
  room_uuid character varying (100),
  prop_fk integer,
  style_uref_fk character varying (200),
  style_count integer,
  description character varying (200),
  review_score double precision,
  source_uuid character varying (200),
  data_source character varying(100),
  data_owner  character varying(100),
  created_dt timestamp without time zone,
  created_by character varying(100),
  created_proc character varying(100),
  modified_dt timestamp without time zone,
  modified_by character varying(100),
  modified_proc character varying(100),
  deactivate_dt timestamp without time zone
)
WITH (
  OIDS=FALSE
);
comment on column curated.prop_room.room_pk is 'system generated integer upon insert';
comment on column curated.prop_room.room_uuid is 'alternated id defined by a previous process';
comment on column curated.prop_room.prop_fk is 'property primary key';
comment on column curated.prop_room.style_uref_fk is 'room style lookup value primary key';
comment on column curated.prop_room.style_count is 'acredited entity assigned rating';
comment on column curated.prop_room.description is 'a quailifier description about the group';
comment on column curated.prop_room.review_score is 'accredited entity assessed group review score';
comment on column curated.prop_room.source_uuid is 'uuid or pk of the record in the data source';
comment on column curated.prop_room.data_source is 'storage data was taken e.g. S3 folder';
comment on column curated.prop_room.data_owner is 'reference to the origin of the data';


DROP TABLE IF EXISTS curated.prop_facility;
CREATE TABLE curated.prop_facility
(
  facil_pk serial primary key,
  facil_uuid character varying (100),
  prop_fk integer,
  type_uref_fk character varying (200),
  facil_name character varying (200),
  capacity integer,
  description character varying (200),
  review_score double precision,
  source_uuid character varying (200),
  data_source character varying(100),
  data_owner  character varying(100),
  created_dt timestamp without time zone,
  created_by character varying(100),
  created_proc character varying(100),
  modified_dt timestamp without time zone,
  modified_by character varying(100),
  modified_proc character varying(100),
  deactivate_dt timestamp without time zone
)
WITH (
  OIDS=FALSE
);
comment on column curated.prop_facility.facil_pk is 'system generated integer upon insert';
comment on column curated.prop_facility.facil_uuid is 'alternated id defined by a previous process';
comment on column curated.prop_facility.prop_fk is 'property primary key';
comment on column curated.prop_facility.type_uref_fk is 'room style lookup value primary key';
comment on column curated.prop_facility.facil_name is 'unique name given to the facility e.g cheers pub';
comment on column curated.prop_facility.capacity is 'maximum number of guests carrying capacity';
comment on column curated.prop_facility.description is 'a quailifier description about the group';
comment on column curated.prop_facility.review_score is 'assessed facility review score';
comment on column curated.prop_facility.source_uuid is 'uuid or pk of the record in the data source';
comment on column curated.prop_facility.data_source is 'storage data was taken e.g. S3 folder';
comment on column curated.prop_facility.data_owner is 'reference to the origin of the data';

ALTER TABLE curated.prop_detail
  OWNER TO rezaware;
ALTER TABLE curated.prop_alt_name
  OWNER TO rezaware;
ALTER TABLE curated.prop_grp
  OWNER TO rezaware;
ALTER TABLE curated.prop_room
  OWNER TO rezaware;
ALTER TABLE curated.prop_facility
  OWNER TO rezaware;