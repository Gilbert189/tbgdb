-- Schema for a blank database. This doesn't purge the database when queried.

pragma journal_mode=WAL;

create table if not exists Users (
	uid integer primary key not null,
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
	first_scraped datetime default (datetime()),
	last_scraped datetime,
	url text as (concat('https://tbgforums.com/forums/index.php?action=profile;u=', uid))
) without rowid;

create table if not exists Boards (
	bid integer primary key not null,
	board_name text,
	url text as (concat('https://tbgforums.com/forums/index.php?board=', bid))
) without rowid;

create table if not exists Topics (
	tid integer primary key not null,
	topic_name text,
	bid integer references Boards(bid),
	url text as (concat('https://tbgforums.com/forums/index.php?topic=', tid))
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
	deleted boolean default false,
	url text as (concat('https://tbgforums.com/forums/index.php?msg=', mid))
) without rowid;

create table if not exists Statistics (
	key text unique,
	value
);