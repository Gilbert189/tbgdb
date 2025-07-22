import sqlite3
from datetime import datetime
import json
import re

from flask import Blueprint, request, url_for, make_response, current_app

api = Blueprint("api", __name__)


# Here is the configuration for the API server.
DB_FILE = "tbgs.db"
"Database to store the scraped data."


db = sqlite3.connect(DB_FILE, check_same_thread=False)
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat(timespec='seconds'))
sqlite3.register_converter("datetime", lambda dt: datetime.fromisoformat(dt))
sqlite3.register_adapter(dict, lambda obj: json.dumps(obj))
sqlite3.register_converter("json", lambda obj: json.loads(obj))

def dict_factory(cursor, row):  # noqa
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}
db.row_factory = dict_factory # noqa


def build_fts():  # noqa
    current_app.logger.info("Building FTS tables")
    db.executescript("""
-- HACK: need to use a view since we're storing the FTS table temporarily
-- and using the tables direcly doesn't work
create view if not exists temp.MessageView as
    select mid, subject, content, user from Messages;
create virtual table temp.MessageFTS using fts5(
    mid, subject, content, user,
    content=MessageView,
    content_rowid=mid
);
create temporary trigger MessageFTS_insert after insert on Messages begin
    insert into MessageFTS (rowid, mid, subject, content, user)
        values (new.mid, new.mid, new.subject, new.content, new.user);
end;
create temporary trigger MessageFTS_delete after delete on Messages begin
    insert into MessageFTS (MessageFTS, rowid, mid, subject, content, user)
        values ('delete', old.mid, old.mid, old.subject, old.content, old.user);
end;
create temporary trigger MessageFTS_update after update on Messages begin
    insert into MessageFTS (MessageFTS, rowid, mid, subject, content, user)
        values ('delete', old.mid, old.mid, old.subject, old.content, old.user);
    insert into MessageFTS (rowid, mid, subject, content, user)
        values (new.mid, new.mid, new.subject, new.content, new.user);
end;
insert into MessageFTS (MessageFTS) values ('rebuild');

create view if not exists temp.TopicView as
    select tid, topic_name, bid from Topics;
create virtual table temp.TopicFTS using fts5(
    tid, topic_name, bid,
    content=TopicView,
    content_rowid=tid
);
create temporary trigger TopicFTS_insert after insert on Topics begin
    insert into TopicFTS (rowid, tid, topic_name, bid)
        values (new.tid, new.tid, new.topic_name, new.bid);
end;
create temporary trigger TopicFTS_delete after delete on Topics begin
    insert into TopicFTS (TopicFTS, rowid, tid, topic_name, bid)
        values ('delete', old.tid, old.tid, old.topic_name, old.bid);
end;
create temporary trigger TopicFTS_update after update on Topics begin
    insert into TopicFTS (TopicFTS, rowid, tid, topic_name, bid)
        values ('delete', old.tid, old.tid, old.topic_name, old.bid);
    insert into TopicFTS (rowid, tid, topic_name, bid)
        values (new.tid, new.tid, new.topic_name, new.bid);
end;
insert into TopicFTS (TopicFTS) values ('rebuild');
    """)  # noqa


uptime = datetime.now()


@api.route("/post/<mid>")
@api.route("/message/<mid>")
def get_message(mid):  # noqa
    cur = db.cursor()
    query = cur.execute("select * from Messages where mid=?", (mid,))
    query = query.fetchone()

    if query is None:
        return query, 404
    return query


@api.route("/user/<uid>")
def get_user(uid):  # noqa
    cur = db.cursor()
    query = cur.execute("select * from Users where uid=?", (uid,))
    query = query.fetchone()

    if query is None:
        return query, 404
    return query


@api.route("/topic/<tid>")
def get_topic(tid):  # noqa
    cur = db.cursor()
    query = cur.execute("select * from Topics where tid=?", (tid,))
    query = query.fetchone()

    if query is None:
        return query, 404
    return query


@api.route("/search/messages")
def search_messages():  # noqa
    def sanitize(x):  # noqa
        return re.sub(r"\W", "_", x)

    args = request.args.to_dict()
    if len(args) == 0:
        return {"ValueError": "at least a query is required"}

    cur = db.cursor()
    status_code = 200
    try:
        query = cur.execute(
            "select * from MessageFTS where "
            # very risky...
            + " and ".join(
                f"{x} match :{x}"
                for x in map(sanitize, args.keys())
            ),
            args
        )
        query = query.fetchall()
    except sqlite3.Error as e:
        return {type(e).__name__: str(e)}, 400

    if query == []:
        status_code = 404
    return query, status_code


@api.route("/search/topics")
def search_topics():  # noqa
    def sanitize(x):  # noqa
        return re.sub(r"\W", "_", x)

    args = request.args.to_dict()
    if len(args) == 0:
        return {"ValueError": "at least a query string is required"}

    cur = db.cursor()
    status_code = 200
    try:
        query = cur.execute(
            "select * from TopicFTS where "
            # very risky...
            + " and ".join(
                f"{x} match :{x}"
                for x in map(sanitize, args.keys())
            ),
            args
        )
        query = query.fetchall()
    except sqlite3.Error as e:
        return {type(e).__name__: str(e)}, 400

    if query == []:
        status_code = 404
    return query, status_code


@api.route("/stats")
def statistics():  # noqa
    def sanitize(x):  # noqa
        return re.sub(r"\W", "_", x)

    cur = db.cursor()
    query = cur.execute("select key, value from Statistics")
    query = {pair["key"]: pair["value"] for pair in query.fetchall()}

    return query


@api.route("/about")
def about():  # noqa
    about_text = f"""
This is an API for TBGDB, a screen-scraper suite for the Text Based Games \
Forums (hereafter called "TBGs").

Here you can access messages, topics, and users that TBGDB has scraped. \
See the "urls" key on {url_for('.hello')} for examples.

Data scraped by TBGDB are property of their respective authors. Please \
respect their rights.
    """.strip()
    # You may modify this about text.
    res = make_response(about_text)
    res.content_type = "text/plain"
    return res


@api.route("/")
def hello():  # noqa
    return {
        "hello": "world",
        "uptime": uptime.isoformat(),
        "urls": {
            "about": url_for(".about"),
            "get_message": url_for(".get_message", mid="$mid"),
            "get_topic": url_for(".get_topic", tid="$tid"),
            "search_messages": url_for(".search_messages",
                                       contents="...",
                                       subject="..."),
            "search_topics": url_for(".search_topics",
                                     topic_name="..."),
        },
    }


def create_app():  # noqa
    from flask import Flask
    app = Flask(__name__)

    app.register_blueprint(api)

    with app.app_context():
        build_fts()

    return app
