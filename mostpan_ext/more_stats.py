"""
Serves more statistics of the TBGDB's data, mostly in chart form.

Matplotlib is required to create the charts.
"""

from flask import current_app, Blueprint, g, request, url_for
from io import BytesIO
from datetime import datetime, timedelta
import sqlite3
import re
from functools import partial


logger = current_app.logger.getChild("more_stats")


db = current_app.config.db
api = g.blueprints.get("api", None)
if api is not None:
    stats_api = Blueprint('stats', __name__, url_prefix="/stats")

    DATE_FORMATS = {
        "hourly": "%Y-%m-%dT%H",
        "daily": "%Y-%m-%d",
        "weekly": "%Y-W%W",
        "monthly": "%Y-%m",
    }
    RANGE_LIMIT = {
        "hourly": timedelta(weeks=1),
        "daily": timedelta(weeks=24),
        "weekly": timedelta(weeks=120),
        "monthly": timedelta(weeks=600),
    }
    DEFAULT_RANGE = {
        "hourly": timedelta(days=1),
        "daily": timedelta(weeks=4),
        "weekly": timedelta(weeks=24),
        "monthly": timedelta(weeks=52),
    }

    def to_bool(x):  # noqa
        try:
            return bool(int(x))
        except Exception:
            return x in (
                "true", "True", "TRUE", "T", "t", "yes", "y", "YES",
            )

    @stats_api.route("/counts/<sample>")
    def message_count(sample):  # noqa
        try:
            if sample not in DATE_FORMATS:
                raise ValueError(
                    f"allowed sample ranges are {list(DATE_FORMATS)}"
                )

            args = request.args

            # Assemble the user conditions
            user_conditions = [
                f"user={int(uid)}"
                for uid in args.getlist("user")
            ]
            if user_conditions == []:
                user_conditions = ["1"]
            if args.get("combine_users", default=True, type=to_bool):
                user_conditions = [" or ".join(user_conditions)]
            # Assemble the topic conditions
            topic_conditions = [
                f"tid={int(tid)}"
                for tid in args.getlist("topic")
            ]
            if topic_conditions == []:
                topic_conditions = ["1"]
            if args.get("combine_topics", default=True, type=to_bool):
                topic_conditions = [" or ".join(topic_conditions)]
            # Assemble the board conditions
            board_conditions = [
                f"bid={int(bid)}"
                for bid in args.getlist("board")
            ]
            if board_conditions == []:
                board_conditions = ["1"]
            if args.get("combine_boards", default=True, type=to_bool):
                board_conditions = [" or ".join(board_conditions)]
            # Assemble the time conditions
            # args.get uses the default value on error (bad)
            # so need to use a more convoluted method that does throw an error
            start_range = args.get("start")
            if start_range is None:
                start_range = datetime.now() - DEFAULT_RANGE[sample]
            else:
                start_range = datetime.fromisoformat(start_range)
            end_range = args.get("end")
            if end_range is None:
                end_range = datetime.now()
            else:
                end_range = datetime.fromisoformat(end_range)
            if end_range - start_range > RANGE_LIMIT[sample]:
                raise ValueError("range exceeds limit")
            time_conditions = (
                f"unixepoch(date) > {start_range.timestamp()}"
                f" and unixepoch(date) < {end_range.timestamp()}"
            )

            cumulative = args.get("cumulative", default=False, type=to_bool)
            count_criteria = (
                "sum(count(*)) over (order by strftime(:datefmt, date))"
                if cumulative
                else "count(*)"
            )

            result = {}
            cur = db.cursor()
            combinations = [
                re.sub(
                    r"(?<!\d)1 and | and 1",
                    "",
                    f"{user} and {topic} and {board}"
                )
                for user in user_conditions
                for topic in topic_conditions
                for board in board_conditions
            ]
            for message_conditions in combinations:
                query = cur.execute(
                    f"""
                    select
                        strftime(:datefmt, date) as time,
                        {count_criteria} as count
                    from Messages
                        join Topics using (tid)
                        join Boards using (bid)
                    where ({message_conditions}) and ({time_conditions})
                    group by time
                    """,
                    {"datefmt": DATE_FORMATS[sample]}
                ).fetchall()
                for item in query:
                    (result
                     .setdefault(item["time"], {})
                     .update({message_conditions: item["count"]})
                     )
            # Fill missing values with either:
            # - zero if we're not taking a running sum, or
            # - the last value if we're taking one
            last_value = {}
            for key_, point in sorted(result.items()):
                last_value.update(point)
                for message_conditions in combinations:
                    point.setdefault(
                        message_conditions,
                        int(last_value[message_conditions])
                        if cumulative
                        else 0
                    )

            return {
                "conditions": {
                    "user": user_conditions,
                    "topic": topic_conditions,
                    "board": board_conditions,
                },
                "start": start_range.isoformat(timespec="seconds"),
                "end": end_range.isoformat(timespec="seconds"),
                "counts": result,
            }
        except ValueError as e:
            return {type(e).__name__: str(e)}, 400
        except sqlite3.Error as e:
            return {type(e).__name__: str(e)}, 422

    @stats_api.route("/complete")
    def completeness():  # noqa
        cur = db.cursor()
        return {
            "message": cur.execute(
                """
                select
                    (1.0 * count(content) / count(*)) as filled_content,
                    (1.0 * count(*) / max(mid)) as existing_posts
                from Messages
                """
            ).fetchone(),
            "topic": cur.execute(
                """
                select
                    (1.0 * count(topic_name) / count(*)) as filled_names,
                    (1.0 * count(*) / max(tid)) as existing_topics
                from Topics
                """
            ).fetchone(),
        }

    current_app.config.other_api_examples.update({
        "message_counts_over_time":
        partial(url_for,
                "api.stats.message_count",
                sample="hourly,daily,weekly,monthly",
                user="...",
                topic="...",
                board="...",
                start="ISOdate...",
                end="ISOdate...",
                cumulative="true,false")
    })

    api.register_blueprint(stats_api, path="/")
