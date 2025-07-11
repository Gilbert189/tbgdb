-- Schema for a blank database. This doesn't purge the database when queried.

create table if not exists Users (
	uid integer primary key,
	name text,
	avatar text,
	user_group text,
	posts text,
	signature text,
	email text,
	blurb text,
	location text,
	real_name text,
	social json,
	website text,
	gender text,
	first_scraped datetime,
	last_scraped datetime
);

create table if not exists Boards (
	bid integer primary key,
	board_name text
);

create table if not exists Topics (
	tid integer primary key,
	topic_name text,
	bid integer references Boards(tid)
);

create table if not exists Messages (
	mid integer primary key,
	subject text,
	date datetime,
	edited datetime,
	content text,
	user integer references Users(uid),
	icon text,
	tid integer references Topics(tid),
	first_scraped datetime default (datetime()),
	last_scraped datetime,
	deleted boolean default false
);

create table if not exists Statistics (
	key text unique,
	value
);