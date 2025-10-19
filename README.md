# TBGDB
(or Post Thief "Mostpan" Mat if that's your jam.)

> ## HEADS UP!
> This scraper is only meant to be used by a single person (myself). It's published here just to 
> make documentation and inheritance easier, in the case that my server got inactive/compromised/
> corrupted and someone else would like to pass the TBGDB torch.
> 
> Screen-scrapers are powerful tools. With great power there must also come great responsibility.
> The TBGs is merely a single, feeble server that can't handle being overloaded with requests.
> While there are many screen scrapes running around the world that is more potent than TBGDB, I
> personally don't want every TBGer to run an instance of TBGDB for their own purposes. 
> **One scraper is enough for everyone: no more, no less.**

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
- `web.py` should be another Flask app that also serves a website alongside the API. 
  But the website is not currently implemented, so all it does now is serving the API at `/api/*`.

## Installation
One thing to note about these scripts is that they don't implement any form of CLI. You have to
change the scripts's code in order to configure them. The configurations are all documented there.
You may consider this not optimal, but you have to keep in mind that TBGDB is not for everyone to 
run (see the [admonition](#heads-up) above).

### Starting the scraper
Before starting the TBGDB scraper, you should make sure you have:
- Python (around 3.12) and some knowledge of it;
- a TBG account to retrieve user data, along with their credentials;
- set the account to use UTC time to make time calculations more accurate; 
- sufficient storage on your machine (a full scrape takes about 1 GB of storage); and
- configured the scraper properly.

> [!IMPORTANT]
> By default, the scraper imports `my_secrets` to retrieve the password of Clicky (my personal bot). 
> When you see something like this:
> ```
> Traceback (most recent call last):
>   File "scraper.py", line ..., in <module>
>     from my_secrets.tbgs import clicky  # change this to something else
> ModuleNotFoundError: No module named 'my_secrets'
> ```
> you should delete that line and replace the value of `PASSWORD` to your bot's password.

After you've done all that, you're ready to start the scraper. Enter this on your terminal:
```
$ python scraper.py
```
The scraper should make a `tbgs.db` file (or as you defined it in the `DB_FILE` variable). This 
[SQLite](https://www.sqlite.org/) database contains messages, topics, and boards scraped by TBGDB.

### Prerequisites for the API/website
Before deploying the TBGDB server, you should make sure you have:
- done all the prerequisites of the scraper;
- [Flask](https://flask.palletsprojects.com/en/stable/) (around 3.0.3) and some knowledge of it;
- a WSGI server of your choice and the knowledge to configure them; which implies
- knowledge on how to set up a web server;
- sufficient storage on your machine for the database backups, if you enabled them; and
- configured the server properly.

After you've done all that, you're ready to deploy the server. 

> [!IMPORTANT]
> To make the explanations easier, these examples uses Flask's own 
> [development server](https://flask.palletsprojects.com/en/stable/server/). As stated in the docs
> (and the printout on the terminal), this server isn't meant for production (hosting for everyone
> to use). Refer to your WSGI server's documentation to figure out how to start TBGDB's
> API/website with it.

To start the API server, enter this on your terminal:
```
$ flask --app server run
```
The terminal should print an address where the app is served.

> [!NOTE]
> The API will create some more tables, views, and indicies on the main database to implement
> the FTS (full text search) functionality and make queries faster (especially for 
> `/counts/<criteria>`). This will make the scraper work slower, as more data is processed and 
> stored into the database. Because of this, the database would become slightly larger as well.
> Just keep that in mind.

To start the web server, enter this on your terminal:
```
$ flask --app web run
```
The web server should import the API server automatically, so there's no need to run both
seperately.
