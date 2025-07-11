# TBGDB
(or Post Thief "Mostpan" Mat if that's your jam.)

> ## HEADS UP!
> This scraper is only meant to be used by a single person (myself). It's published here just to 
> make inheritance easier, in the case that my server got inactive/compromised/corrupted and
> someone else would like to pass the TBGDB torch.
> 
> If you want to run your own instance of TBGDB, please do so under everyone's consent. (Or at
> least the majority.)

TBGDB is a screen-scraper suite that scrapes and presents the scraped data from the 
[Text Based Games Forums](https://tbgforums.com/forums/index.php).

## But why?!
Simply, I want a better way to search posts in the TBGs.

The SMF's search feature is absolutely terrible. For some reason (probably performance, though 
reality would suggest otherwise) it doesn't search the entire website, which is very absurd.
It's not uncommon for me to search the FluxBB archives just to find a post that has a keyword I
want to search, hoping that the post is old enough. Of course, this is very inconvenient.

That and I want to do some data analysis as always. =)

## Scripts
There are three scripts in this suite:

- `scraper.py` is self-explanatory. It creates a `tbgs.db` file which stores the data it scrapes.
- `server.py` is a [Flask](https://flask.palletsprojects.com/en/stable/) app that presents the
  scraped data into an HTTP API via WSGI.
- `web.py` is another Flask app, but serves a website alongside the API. 

## Installation
One thing to note about these scripts is that they don't offer any form of CLI. You have to change
the scripts in order to configure them. The configurations are all documented

### Prerequisites for the scraper
Before starting the TBGDB scraper, you should make sure you have:
- a TBG account to retrieve user data, along with their credentials;
- set the account to use UTC time to make time calculations more accurate; and
- sufficient storage on your machine (a full scrape takes about 1 GB of storage);

### Prerequisites for the API/website
Before deploying the TBGDB website, you should make sure you have:
- done all the prerequisites of the scraper;
- a WSGI server of your choice and the knowledge to configure them; and
- sufficient storage on your machine for the database backups, if you enabled them;
