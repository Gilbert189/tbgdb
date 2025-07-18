import sqlite3
from datetime import datetime
import json
import logging
import re

from flask import Blueprint, jsonify, request

api = Blueprint("api", __name__)
logger = logging.getLogger(__name__)


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

logger.info("Building FTS tables")
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


@api.route("/post/<mid>")
@api.route("/message/<mid>")
def get_message(mid):  # noqa
    cur = db.cursor()
    query = cur.execute("select * from Messages where mid=?", (mid,))
    query = query.fetchone()

    res = jsonify(query)
    if query is None:
        res.status_code = 404
    return res


@api.route("/user/<uid>")
def get_user(uid):  # noqa
    cur = db.cursor()
    query = cur.execute("select * from Users where uid=?", (uid,))
    query = query.fetchone()

    res = jsonify(query)
    if query is None:
        res.status_code = 404
    return res


@api.route("/topic/<tid>")
def get_topic(tid):  # noqa
    cur = db.cursor()
    query = cur.execute("select * from Topics where tid=?", (tid,))
    query = query.fetchone()

    res = jsonify(query)
    if query is None:
        res.status_code = 404
    return res


@api.route("/search/messages")
def search_messages():  # noqa
    def sanitize(x):  # noqa
        return re.sub(r"\W", "_", x)

    args = request.args.to_dict()
    cur = db.cursor()
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
        res = jsonify({type(e).__name__: str(e)})
        res.status_code = 400
        return res

    res = jsonify(query)
    if query == []:
        res.status_code = 404
    return res


@api.route("/search/topics")
def search_topic():  # noqa
    def sanitize(x):  # noqa
        return re.sub(r"\W", "_", x)

    args = request.args.to_dict()
    cur = db.cursor()
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
        print(query)
    except sqlite3.Error as e:
        res = jsonify({type(e).__name__: str(e)})
        res.status_code = 400
        return res

    res = jsonify(query)
    if query == []:
        res.status_code = 404
    return res


@api.route("/stats")
def statistics():  # noqa
    def sanitize(x):  # noqa
        return re.sub(r"\W", "_", x)

    cur = db.cursor()
    query = cur.execute("select key, value from Statistics")
    query = {pair["key"]: pair["value"] for pair in query.fetchall()}

    res = jsonify(query)
    return res


def create_app():  # noqa
    from flask import Flask
    app = Flask(__name__)

    app.register_blueprint(api)

    return app
