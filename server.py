import sqlite3
from datetime import datetime
import json
import re

from flask import Blueprint, request, url_for, make_response, current_app, g

api = Blueprint("api", __name__)


# Here is the configuration for the API server.
DB_FILE = "tbgs.db"
"Database to store the scraped data."


db = sqlite3.connect(
    DB_FILE,
    check_same_thread=False,
    detect_types=sqlite3.PARSE_DECLTYPES
)
sqlite3.register_adapter(datetime,
                         lambda dt: dt.isoformat(timespec='seconds'))
# sqlite3.register_converter("datetime",
#                            lambda dt: datetime.fromisoformat(dt.decode()))
sqlite3.register_adapter(dict,
                         lambda obj: json.dumps(obj))
sqlite3.register_converter("json",
                           lambda obj: json.loads(obj))


def dict_factory(cursor, row):  # noqa
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}
db.row_factory = dict_factory # noqa


def build_fts(force_rebuild=False):  # noqa
    if not force_rebuild:
        cur = db.cursor()
        tables = {
            row["name"]
            for row in cur.execute("select name from sqlite_master").fetchall()
        }
        required_tables = (
            {"MessageView", "TopicView"}
            | {
                table + trigger
                for table in ["MessageFTS", "TopicFTS"]
                for trigger in ["", "_insert", "_update", "_delete"]
            }
        )
        if tables > required_tables:
            current_app.logger.info("FTS tables exists, not building again.")
            return

    current_app.logger.info("Building FTS tables")
    db.executescript("""
create view if not exists MessageView as
    select mid, subject, content, name as username, topic_name, board_name
    from Messages
        join Topics using (tid)
        join Boards using (bid)
        join Users on Messages.user=Users.uid;
create virtual table if not exists MessageFTS using fts5(
    subject, content, username, topic_name, board_name,
    content=MessageView,
    content_rowid=mid
);
create trigger if not exists MessageFTS_insert after insert on Messages begin
    insert into MessageFTS
        (rowid, subject, content, username, topic_name, board_name)
    values (
        new.mid, new.subject, new.content,
        (select name from Users where uid=new.user),
        (select topic_name from Topics where tid=new.tid),
        (select board_name
            from Boards
                join Topics using (bid)
            where tid=new.tid)
    );
end;
create trigger if not exists MessageFTS_delete after delete on Messages begin
    insert into MessageFTS
        (MessageFTS, rowid, subject, content, username, topic_name, board_name)
    values (
        'delete', old.mid, old.subject, old.content,
        (select name from Users where uid=old.user),
        (select topic_name from Topics where tid=old.tid),
        (select board_name
            from Boards
                join Topics using (bid)
            where tid=old.tid)
    );
end;
create trigger if not exists MessageFTS_update after update on Messages begin
    insert into MessageFTS
        (MessageFTS, rowid, subject, content, username, topic_name, board_name)
        values (
            'delete', old.mid, old.subject, old.content,
            (select name from Users where uid=old.user),
            (select topic_name from Topics where tid=old.tid),
            (select board_name
                from Boards
                    join Topics using (bid)
                where tid=old.tid)
        );
    insert into MessageFTS
        (rowid, subject, content, username, topic_name, board_name)
        values (
            new.mid, new.subject, new.content,
            (select name from Users where uid=new.user),
            (select topic_name from Topics where tid=new.tid),
            (select board_name
                from Boards
                    join Topics using (bid)
                where tid=new.tid)
        );
end;
insert into MessageFTS (MessageFTS) values ('rebuild');

create view if not exists TopicView as
    select tid, topic_name, board_name
    from Topics
        full join Boards using (bid);
create virtual table if not exists TopicFTS using fts5(
    topic_name, board_name,
    content=TopicView,
    content_rowid=tid
);
create trigger if not exists TopicFTS_insert after insert on Topics begin
    insert into TopicFTS (rowid, topic_name, board_name)
        values (new.tid, new.topic_name, new.board_name);
end;
create trigger if not exists TopicFTS_delete after delete on Topics begin
    insert into TopicFTS (TopicFTS, rowid, topic_name, board_name)
        values ('delete', old.tid, old.topic_name, old.board_name);
end;
create trigger if not exists TopicFTS_update after update on Topics begin
    insert into TopicFTS (TopicFTS, rowid, topic_name, board_name)
        values ('delete', old.tid, old.topic_name, old.board_name);
    insert into TopicFTS (rowid, topic_name, board_name)
        values (new.tid, new.topic_name, new.board_name);
end;
insert into TopicFTS (TopicFTS) values ('rebuild');
    """)


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


@api.route("/forum/<bid>")
@api.route("/board/<bid>")
def get_board(bid):  # noqa
    cur = db.cursor()
    query = cur.execute("select * from Boards where bid=?", (bid,))
    query = query.fetchone()

    if query is None:
        return query, 404
    return query


@api.route("/topic/<tid>/posts")
@api.route("/topic/<tid>/messages")
def get_topic_messages(tid):  # noqa
    cur = db.cursor()
    query = cur.execute(
        "select * from Messages where tid=? order by mid asc",
        (tid,)
    )
    query = query.fetchall()

    if query == []:
        return query, 404
    return query


@api.route("/forum/<bid>/topics")
@api.route("/board/<bid>/topics")
def get_board_topics(bid):  # noqa
    cur = db.cursor()
    query = cur.execute(
        """
        select tid, topic_name, max(mid) as latest_post
        from Topics
            join Messages using (tid)
        where bid=?
        group by tid
        order by latest_post desc""",
        (bid,)
    )
    query = query.fetchall()

    if query == []:
        return query, 404
    return query


@api.route("/search/messages")
def search_messages():  # noqa
    def sanitize(x):  # noqa
        return re.sub(r"\W", "_", x)

    args = request.args.to_dict().get("q", "")
    if len(args) == 0:
        return {"ValueError": "at least a query is required"}, 400

    cur = db.cursor()
    status_code = 200
    try:
        query = cur.execute(
            "select rowid as mid, * from MessageFTS(?)",
            (args,)
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

    args = request.args.to_dict().get("q", "")
    if len(args) == 0:
        return {"ValueError": "at least a query is required"}, 400

    cur = db.cursor()
    status_code = 200
    try:
        query = cur.execute(
            "select rowid as tid, * from TopicFTS(?)",
            (args,)
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
Forums.

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
            "get_board": url_for(".get_board", bid="$bid"),
            "search_messages": url_for(".search_messages",
                                       q="content:..."),
            "search_topics": url_for(".search_topics",
                                     q="content:..."),
            "get_messages_on_topic":
                url_for(".get_topic_messages", tid="$tid"),
            "get_topics_on_board":
                url_for(".get_board_topics", bid="$bid"),
            **{
                k: v()
                for k, v in current_app.config.other_api_examples.items()
            }
        },
    }


def create_app():  # noqa
    from flask import Flask
    app = Flask(__name__)

    with app.app_context():
        app.config.db = db  # please don't scream at me
        g.blueprints = {
            "api": api
        }
        app.config.other_api_examples = {}
        build_fts()
        import mostpan_ext  # noqa

    app.register_blueprint(api)

    return app
