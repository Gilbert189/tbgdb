import logging
from datetime import datetime, timedelta
import sqlite3
import json
from pprint import pprint  # noqa
from collections import defaultdict
from itertools import chain
from time import sleep

from tbgclient import api, Session, Page, Message
from tbgclient.exceptions import RequestError as TBGRequestError
from tbgclient.parsers import forum as parser
from requests.exceptions import RequestException
from my_secrets.tbgs import clicky  # change this to something else

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


# Here is the configuration for the scraper.
USERNAME = "Clicky"
"Username of the account used by the scraper."
PASSWORD = clicky
"Password of the account used by the scraper."
RECENT_POWER = 5
"Multiplier for the most recent posts. Should be no smaller than 1."
HALF_TIME = timedelta(hours=3)
"Decay time of the recent post multiplier."
DB_FILE = "tbgs.db"
"Database to store the scraped data."
GREEDY_SCRAPE = False
"""When scraping topic pages, setting this to True would scrape the BBC of all
the messages on the page. Otherwise, it only scrapes the BBC of the message
the scraper happens to scrape."""
REVIEW_SIZE = 1000
"""How many posts to scrape in the review phase."""


logger.info(f"Logging in as {USERNAME}")
session = Session()
session.login(USERNAME, PASSWORD)
session.make_default()
# The lxml scraper is much more robust than html.parser.
parser.parser_config.update(features="lxml")

logger.info(f"Connecting to {DB_FILE}")
db = sqlite3.connect(DB_FILE)
with open("schema.sql", "r") as f:
    cursor = db.executescript(f.read())
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat(timespec='seconds'))
sqlite3.register_converter("datetime", lambda dt: datetime.fromisoformat(dt))
sqlite3.register_adapter(dict, lambda obj: json.dumps(obj))
sqlite3.register_converter("json", lambda obj: json.loads(obj))


def retry_on_error(func):  # noqa
    """A decorator that recalls a function when a connection error occured."""
    def wrapper(*args, **kwargs):  # noqa
        while True:
            try:
                return func(*args, **kwargs)
            except RequestException as e:
                logger.error(e)
                sleep(1)
    return wrapper


def update_stats(key, value, cursor=None):  # noqa
    """Updates a value in the Statistics table."""
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "insert or replace into Statistics (key, value) values (?, ?)",
        (key, value),
    )
    db.commit()


def update_user(user_dict, cursor=None):  # noqa
    """Inserts or updates the given user into the database."""
    if cursor is None:
        cursor = db.cursor()
    maybe_user_dict = defaultdict(lambda: None)
    maybe_user_dict.update(user_dict)
    return cursor.execute(
        "insert into Users ("
        "   uid, name, avatar, user_group, posts, signature, email, blurb,"
        "   location, real_name, social, website, gender, last_scraped"
        ")"
        " values ("
        "   :uid, :name, :avatar, :group, :posts, :signature, :email, :blurb,"
        "   :location, :real_name, :social, :website, :gender, :last_scraped"
        ")"
        "on conflict(uid) do update"
        " set name=ifnull(excluded.name, name),"
        "     avatar=ifnull(excluded.avatar, avatar),"
        "     user_group=ifnull(excluded.user_group, user_group),"
        "     posts=ifnull(excluded.posts, posts),"
        "     signature=ifnull(excluded.signature, signature),"
        "     email=ifnull(excluded.email, email),"
        "     blurb=ifnull(excluded.blurb, blurb),"
        "     location=ifnull(excluded.location, location),"
        "     real_name=ifnull(excluded.real_name, real_name),"
        "     social=ifnull(excluded.social, social),"
        "     website=ifnull(excluded.website, website),"
        "     gender=ifnull(excluded.gender, gender),"
        "     last_scraped=ifnull(excluded.last_scraped, last_scraped)"
        " where uid=:uid",
        maybe_user_dict
    )


def update_msg(msg_dict, cursor=None):  # noqa
    """Inserts or updates the given message into the database."""
    if cursor is None:
        cursor = db.cursor()
    maybe_msg_dict = defaultdict(lambda: None)
    maybe_msg_dict.update(msg_dict)
    # This order shouldn't result in a foreign key constraint error.
    if maybe_msg_dict["bid"] is not None:
        # On the scan phase, surrounding posts wouldn't have ["bid"] set,
        # (unless GREEDY_SCRAPE is set to True) and since we added
        # NOT NULL constraint to the table, we should add this row only if
        # that key is set.
        cursor.execute(
            "insert or replace into Boards (bid, board_name)"
            " values (:bid, :board_name)",
            maybe_msg_dict
        )
    cursor.execute(
        "insert or replace into Topics (tid, topic_name, bid)"
        " values (:tid, :topic_name, :bid)",
        maybe_msg_dict
    )
    update_user(maybe_msg_dict["user"], cursor=cursor)
    cursor.execute(
        "insert into Messages ("
        "   mid, subject, date, edited, content, user, icon, tid,"
        "   last_scraped"
        ")"
        " values ("
        "   :mid, :subject, :date, :edited, :content, :uid, :icon, :tid,"
        "   :now"
        ")"
        # Some messages passed here may not have some keys set.
        # This UPSERT statement should prevent them being set with NULL.
        "on conflict(mid) do update"
        " set subject=ifnull(excluded.subject, subject),"
        "     date=ifnull(excluded.date, date),"
        "     edited=ifnull(excluded.edited, edited),"
        "     content=ifnull(excluded.content, content),"
        "     user=ifnull(excluded.user, user),"
        "     icon=ifnull(excluded.icon, icon),"
        "     tid=ifnull(excluded.tid, tid),"
        "     last_scraped=ifnull(excluded.last_scraped, last_scraped)"
        " where mid=:mid",
        {
            # Default values...
            "edited": None,
            "icon": None,
            # ...that would be overwritten by maybe_msg_dict.
            **maybe_msg_dict,
            "uid": maybe_msg_dict.get("user", {}).get("uid", None),
            "now": datetime.now()
        }
    )
    db.commit()


def get_bbc(msg_dict):  # noqa
    """Uses the quotefast action to get the raw BBC of a post.

    It also determines if the post is deleted or not."""
    try:
        msg = Message(**msg_dict)
        msg = retry_on_error(msg.update_quotefast)()
        msg_dict["content"] = msg.content
    except TBGRequestError:
        logger.info(f"Cannot scrape mID {msg.mid}, assume deleted")
        msg_dict["deleted"] = True
        # Since it's deleted, it would be better to blank the contents.
        msg_dict["content"] = None


# This scraper works in four phases:
# - Discovery phase: use the most recent posts page to discover new posts
# - Scan phase: scrape the posts potentially missed by the last phase
# - Review phase: pick some random posts to review
# - User phase: review all the users stored by the scraper
logger.info("Entering main loop.")
try:
    while True:
        last_mid = (
            cursor.execute("select ifnull(max(mid), 1) from Messages")
            .fetchone()[0]
        )

        logger.info("Entering discovery phase")
        update_stats("phases.discovery", datetime.now())

        # The recent posts page has 10 pages
        for i in range(10):
            res = retry_on_error(api.do_action)(
                session,
                "recent",
                params={"start": str(i * 10)},
                no_percents=True
            )
            recent = Page(
                **parser.parse_page(res.text, parser.parse_search_content),
                content_type=dict
            )
            for msg in recent.contents:
                get_bbc(msg)
                update_msg(msg, cursor=cursor)
                if msg["mid"] <= last_mid:
                    break
            else:  # No breaks
                continue
            break  # passing the previous for's breaks

        logger.info("Entering scan phase")
        update_stats("phases.scan", datetime.now())

        first_mid = (
            cursor.execute("select ifnull(min(mid), 1) from Messages")
            .fetchone()[0]
        )

        # Better scrape the latest posts first.
        for mid in chain(
            reversed(range(last_mid, msg["mid"])),
            # In the case that the scraper is stopped on the first incomplete
            # scrape, this iterator would scrape the rest of the forum.
            reversed(range(3, first_mid)),
            # mID 3 is the very first post that is publicly accessible in the
            # TBGs.
        ):
            # We're only concerned about messages not found by the discover
            # phase.
            result = cursor.execute(
                "select content from Messages where mid=?",
                (mid,),
            ).fetchone()
            if result is not None:
                continue

            try:
                res = retry_on_error(api.get_message_page)(session, mid)
            except TBGRequestError:
                logger.info(f"Cannot scrape mID {mid}, assume deleted")
                continue
            topic_page = Page(
                **parser.parse_page(res.text, parser.parse_topic_content),
                content_type=dict
            )

            for msg in topic_page.contents:
                if GREEDY_SCRAPE or msg["mid"] == mid:
                    get_bbc(msg)
                else:
                    # Since we skipped get_bbc from laziness,
                    # this is still in HTML, so better blank it.
                    # We will retrieve it later on the review phase.
                    msg["content"] = None
                update_msg(msg)

        logger.info("Entering review phase")
        update_stats("phases.review", datetime.now())
        # Pick some random message IDs, weighted by the time posted.
        # Recent posts has a higher chance of being picked.
        # The query is based from this SO post:
        # https://stackoverflow.com/a/56006340
        # which in turn is based from an algorithm from Efraimidis et al:
        # https://utopia.duth.gr/~pefraimi/research/data/2007EncOfAlg.pdf
        query = cursor.execute(
            # SQLite (at least by itself) doesn't have generate_series like
            # in PostgreSQL, so this is a viable alternative
            """
            with recursive series(x) as (
                select 3
                    union all
                select x+1 from series
                where x < (select mid from Messages order by mid desc limit 1)
            )
            """
            # divide random() by 2 to prevent integer overflow
            """
            select x, 62 - log2(abs(random() / 2)) as priority from series as a
            left join (
                select
                    mid,
                    62 - log2(abs(random() / 2))
                    / (pow(2, -(unixepoch() - unixepoch(date)) / :half_time)
                       * :multiplier + 1) as priority
                from Messages
            ) as b
            on b.mid = a.x
            order by priority
            limit :limit;
            """,
            {
                "half_time": HALF_TIME.total_seconds(),
                "multiplier": RECENT_POWER - 1,
                "limit": REVIEW_SIZE,
            }
        )
        for mid, _ in query:
            try:
                res = retry_on_error(api.get_message_page)(session, mid)
            except TBGRequestError:
                logger.info(f"Cannot scrape mID {mid}, assume deleted")
                continue
            topic_page = Page(
                **parser.parse_page(res.text, parser.parse_topic_content),
                content_type=dict
            )

            for msg in topic_page.contents:
                if GREEDY_SCRAPE or msg["mid"] == mid:
                    get_bbc(msg)
                else:
                    # Since we skipped get_bbc from laziness,
                    # this is still in HTML, so better blank it.
                    # We will retrieve it later on the review phase.
                    msg["content"] = None
                update_msg(msg)

        logger.info("Entering user phase")
        update_stats("phases.user", datetime.now())

        # For some reason using the cursor alone doesn't iterate through all
        # the rows, so I need to use fetchall()
        for (uid,) in cursor.execute("select uid from Users").fetchall():
            res = retry_on_error(api.do_action)(
                session, "profile",
                params={"u": str(uid)},
                no_percents=True
            )
            parser.check_errors(res.text, res)
            parsed = parser.parse_profile(res.text)

            update_user(parsed, cursor=cursor)
            db.commit()

        db.commit()  # CAUTION: keep this at the end of the loop!
except Exception:
    logger.critical("Error caught on main loop!")
    raise
finally:
    db.commit()
