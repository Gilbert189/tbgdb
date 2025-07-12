-- Schema for a blank database. This doesn't purge the database when queried.

pragma journal_mode=WAL;

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
	bid integer primary key not null,
	board_name text
) without rowid;

create table if not exists Topics (
	tid integer primary key not null,
	topic_name text,
	bid integer references Boards(bid)
) without rowid;

create table if not exists Messages (
	mid integer primary key not null,
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
) without rowid;

create table if not exists Statistics (
	key text unique,
	value
);