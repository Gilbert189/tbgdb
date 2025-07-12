import logging
from datetime import datetime, timedelta
import sqlite3
import json
from pprint import pprint  # noqa
from collections import defaultdict
from itertools import chain

from tbgclient import api, Session, Page, Message
from tbgclient.exceptions import RequestError as TBGRequestError
from tbgclient.parsers import forum as parser
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
HALF_TIME = timedelta(hours=12)
"Decay time of the recent post multiplier."
DB_FILE = "tbgs.db"
"Database to store the scraped data."
GREEDY_SCRAPE = False
"""When scraping topic pages, setting this to True would scrape the BBC of all
the pages. """


logger.info(f"Logging in as {USERNAME}")
session = Session()
session.login(USERNAME, PASSWORD)
session.make_default()

logger.info(f"Connecting to {DB_FILE}")
db = sqlite3.connect(DB_FILE)
with open("schema.sql", "r") as f:
    cursor = db.executescript(f.read())
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat(timespec='seconds'))
sqlite3.register_converter("datetime", lambda dt: datetime.fromisoformat(dt))
sqlite3.register_adapter(dict, lambda obj: json.dumps(obj))
sqlite3.register_converter("json", lambda obj: json.loads(obj))


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
        "insert or replace into Users ("
        "   uid, name, avatar, user_group, posts, signature, email, blurb,"
        "   location, real_name, social, website, gender, last_scraped"
        ")"
        " values ("
        "   :uid, :name, :avatar, :group, :posts, :signature, :email, :blurb,"
        "   :location, :real_name, :social, :website, :gender, :last_scraped"
        ")",
        maybe_user_dict
    )


def update_msg(msg_dict, cursor=None):  # noqa
    """Inserts or updates the given message into the database."""
    if cursor is None:
        cursor = db.cursor()
    maybe_msg_dict = defaultdict(lambda: None)
    maybe_msg_dict.update(msg_dict)
    # This order shouldn't result in a foreign key constraint error.
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
        "insert or replace into Messages ("
        "   mid, subject, date, edited, content, user, icon, tid,"
        "   last_scraped"
        ")"
        " values ("
        "   :mid, :subject, :date, :edited, :content, :user, :icon, :tid,"
        "   :now"
        ")",
        {
            # Default values...
            "edited": None,
            "icon": None,
            # ...that would be overwritten by maybe_msg_dict.
            **maybe_msg_dict,
            "user": maybe_msg_dict["user"]["uid"],
            "now": datetime.now()
        }
    )


def get_bbc(msg_dict):  # noqa
    """Uses the quotefast action to get the raw BBC of a post.

    It also determines if the post is deleted or not."""
    try:
        msg = Message(**msg_dict)
        msg = msg.update_quotefast()
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
            res = api.do_action(
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
                res = api.get_message_page(session, mid)
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

        db.commit()  # CAUTION: keep this at the end of the loop!
        break
except Exception:
    logger.critical("Error caught on main loop!")
    db.commit()
    raise
