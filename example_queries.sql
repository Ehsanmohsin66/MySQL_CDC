show databases;
show variables like '%binlog%';

create database test_finalto;
use test_finalto;
create table customer
(id int primary key auto_increment,
first_name varchar(20),
last_name varchar(20),
address varchar(20),
total_purchase decimal(12,2),
number_orders int8
);
Insert into test_finalto.customer (first_name, last_name, address, total_purchase, number_orders)
values
("Rob","Key","39 Mayfair London",123.67,1);


