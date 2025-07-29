"""
Serves more statistics of the TBGDB's data, mostly in chart form.

Matplotlib is required to create the charts.
"""

from flask import current_app, Blueprint, g, request, url_for
from io import BytesIO
from datetime import datetime, timedelta
import sqlite3
from functools import partial


logger = current_app.logger.getChild("more_stats")


db = current_app.config.db
api = g.blueprints.get("api", None)
if api is not None:
    stats_api = Blueprint('stats', __name__, url_prefix="/stats")

    @stats_api.route("/counts/<sample>")
    def message_count(sample):  # noqa
        date_formats = {
            "hourly": "%Y-%m-%dT%H",
            "daily": "%Y-%m-%d",
            "weekly": "%Y-W%W",
            "monthly": "%Y-%m",
        }
        range_limit = {
            "hourly": timedelta(weeks=1),
            "daily": timedelta(weeks=24),
            "weekly": timedelta(weeks=120),
            "monthly": timedelta(weeks=600),
        }
        default_range = {
            "hourly": timedelta(days=1),
            "daily": timedelta(weeks=4),
            "weekly": timedelta(weeks=24),
            "monthly": timedelta(weeks=52),
        }
        try:
            if sample not in date_formats:
                raise ValueError(
                    f"allowed sample ranges are {list(date_formats)}"
                )

            args = request.args
            user_conditions = " or ".join(
                f"user={int(uid)}"
                for uid in args.getlist("user")
            )
            if user_conditions == "":
                user_conditions = "1"
            topic_conditions = " or ".join(
                f"tid={int(tid)}"
                for tid in args.getlist("topic")
            )
            if topic_conditions == "":
                topic_conditions = "1"
            board_conditions = " or ".join(
                f"bid={int(bid)}"
                for bid in args.getlist("board")
            )
            if board_conditions == "":
                board_conditions = "1"
            # args.get uses the default value on error (bad)
            # so need to use a more convoluted method that does throw an error
            start_range = args.get("start")
            if start_range is None:
                start_range = datetime.now() - default_range[sample]
            else:
                start_range = datetime.fromisoformat(start_range)
            end_range = args.get("end")
            if end_range is None:
                end_range = datetime.now()
            else:
                end_range = datetime.fromisoformat(end_range)
            if end_range - start_range > range_limit[sample]:
                raise ValueError("range exceeds limit")
            time_conditions = (
                f"unixepoch(date) > {start_range.timestamp()}"
                f" and unixepoch(date) < {end_range.timestamp()}"
            )

            cur = db.cursor()
            query = cur.execute(
                f"""
                select strftime(:datefmt, date) as time, count(*) as count
                from Messages
                    join Topics using (tid)
                    join Boards using (bid)
                where ({user_conditions}) and ({topic_conditions})
                    and ({board_conditions}) and ({time_conditions})
                group by time
                """,
                {"datefmt": date_formats[sample]}
            ).fetchall()

            return {
                "conditions": {
                    "user": user_conditions,
                    "topic": topic_conditions,
                    "board": board_conditions,
                },
                "start": start_range.isoformat(timespec="seconds"),
                "end": end_range.isoformat(timespec="seconds"),
                "counts": query,
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
                end="ISOdate...")
    })

    api.register_blueprint(stats_api, path="/")
