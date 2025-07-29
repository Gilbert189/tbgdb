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


api = g.blueprints.get("api", None)
if api is not None:
    stats_api = Blueprint('stats', __name__)

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
            "daily": timedelta(months=6),
            "weekly": timedelta(years=3),
            "monthly": timedelta(years=20),
        }
        default_range = {
            "hourly": timedelta(days=1),
            "daily": timedelta(months=1),
            "weekly": timedelta(months=3),
            "monthly": timedelta(years=1),
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
            topic_conditions = " or ".join(
                f"tid={int(tid)}"
                for tid in args.getlist("topic")
            )
            board_conditions = " or ".join(
                f"bid={int(bid)}"
                for bid in args.getlist("board")
            )
            start_range = args.get(
                "start",
                default=datetime.now() - default_range[sample],
                type=datetime.fromisoformat,
            )
            end_range = args.get(
                "end",
                default=datetime.now(),
                type=datetime.fromisoformat,
            )
            if end_range - start_range > range_limit[sample]:
                raise ValueError("range exceeds limit")
            time_conditions = (
                f"unixepoch(date) > {start_range.timestamp()}"
                f" and unixepoch(date) < {end_range.timestamp()}"
            )

            cur = g.db.cursor()
            query = cur.execute(
                f"""
                select strftime(:datefmt, date) as time, count(*)
                from Messages
                    join Boards using (tid)
                where {user_conditions} or {topic_conditions}
                    or {board_conditions} and {time_conditions}
                group by time
                """,
                {"datefmt": date_formats[sample]}
            ).fetchall()

            if query == []:
                return query, 404
            return query
        except (ValueError, sqlite3.Error) as e:
            return {type(e).__name__: str(e)}, 400

    current_app.config.other_api_examples.update({
        "message_counts_over_time":
        partial(url_for,
                "api.stats.message_count",
                sample="'hourly,daily,weekly,monthly'",
                user="...",
                topic="...",
                board="...",
                start="ISOdate...",
                end="ISOdate...")
    })

    api.register_blueprint(stats_api, path="/")
