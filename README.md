# ScyBase

The goal of this project is to implement a (very) rudimentary database engine that is loosely based on a
hybrid between MySQL and SQLite. It operates entirely from the command line (no GUI).

Note: Conjunctions and disjunctions are not supported. The supported conditions are =, !=, >, >=, <, <=, like, is null, is not null.

Test queries:
---------------------------------------------------------------------
create database industry;
use industry;
create table parts (description TEXT, availability INT NOT NULL);
select * from parts;
insert into parts(availability) values(0);
insert into parts(availability) values(1);
insert into parts(availability) values(0);
insert into parts(availability) values(0);
insert into parts(availability) values(2);
insert into parts(availability) values(0);
insert into parts(availability) values(0);
insert into parts(availability) values(4);
insert into parts(availability) values(0);
insert into parts(availability) values(6);
insert into parts(availability) values(0);
insert into parts(availability) values(0);
insert into parts(availability) values(7);
insert into parts(availability) values(0);
insert into parts(availability) values(0);
insert into parts(availability) values(3);
insert into parts(availability) values(0);
insert into parts(availability) values(0);
insert into parts(availability) values(0);
insert into parts(availability) values(0);
insert into parts(availability) values(6);
insert into parts(availability) values(0);
insert into parts(availability) values(4);
insert into parts(availability) values(0);
select * from parts;
update parts set description = "king of the hill" where availability = 0;
update parts set description = "king of the valley" where availability = 6;
update parts set description = "king of the north" where availability = 4;
update parts set description = "queen" where availability = 2;
update parts set row_id = 2 where row_id = 1;
update parts set description = "prince" where row_id > 0;
update parts set description = "princess" where availability > 0;
update parts set description = "minister" where row_id < 14;
update parts set description = "cook" where availability < 2;
delete from parts where availability = 0;
select * from parts;
delete from parts;
drop table parts;
drop database industry;
