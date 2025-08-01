"""
Serves more statistics of the TBGDB's data, mostly in chart form.

Matplotlib is required to create the charts. If not present, all view functions
using them will return a 501.
"""

from flask import current_app, Blueprint, g, request, url_for, send_file
from io import BytesIO
from datetime import datetime, timedelta
import sqlite3
import re
from functools import partial


logger = current_app.logger.getChild("more_stats")


try:
    import matplotlib  # noqa
except ImportError:
    logger.warning(
        "Cannot import matplotlib, plotting functions will not work"
    )
    pass


db = current_app.config.db
api = g.blueprints.get("api", None)
if api is not None:
    stats_api = Blueprint('stats', __name__, url_prefix="/stats")

    DATE_FORMATS = {
        "hourly": "%Y-%m-%dT%H",
        "daily": "%Y-%m-%d",
        "weekly": "%G-W%V",
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
    MIME_MPL_TYPES = {
        "image/svg+xml": "svg",
        "application/postscript": "ps",
        "application/pdf": "pdf",
        "application/x-pdf": "pdf",
        "image/png": "png",
        "image/gif": "gif",
    }
    MPL_MIME_TYPES = {v: k for k, v in MIME_MPL_TYPES.items()}

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
            }, 200
        except ValueError as e:
            return {type(e).__name__: str(e)}, 400
        except sqlite3.Error as e:
            return {type(e).__name__: str(e)}, 422

    @stats_api.route("/plot/counts/<sample>")
    def plot_message_count(sample):  # noqa
        try:
            from matplotlib import pyplot as plt, animation
        except ImportError as e:
            __import__("traceback").print_exc()
            return {type(e).__name__: str(e)}, 501

        # Retrieve the desired content type.
        if len(accept_types := request.accept_mimetypes) > 0:
            mime_format = accept_types.best_match(MIME_MPL_TYPES)
            if mime_format is None:
                return {
                    "TypeError":
                    "unsupported type(s)"
                    f" (supported types: {', '.join(MIME_MPL_TYPES)})"
                }, 406
            mpl_format = MIME_MPL_TYPES[mime_format]
        else:
            mpl_format = request.args.get("type", default="svg")
            if mpl_format not in MPL_MIME_TYPES:
                return {
                    "TypeError":
                    "unsupported type(s)"
                    f" (supported types: {', '.join(MPL_MIME_TYPES)})"
                }, 400
            mime_format = MPL_MIME_TYPES[mpl_format]

        result, code = message_count(sample)
        if code != 200:
            return result, code
        counts = result["counts"]

        # Make the plot.
        plt.ioff()
        fig, ax = plt.subplots()
        times = sorted(counts)
        mpl_times = [
            datetime.strptime(time, DATE_FORMATS[sample])
            # %V requires %w to work properly
            if sample != "weekly"
            else datetime.strptime(time+";0", DATE_FORMATS[sample]+";%w")
            for time in times
        ]
        print(mpl_times)
        for label in next(iter(counts.values()), {}).keys():
            plt.plot(
                mpl_times, [counts[time][label] for time in times],
                label=label
            )

        ax.legend()

        # Prepare to send the plot.
        image = BytesIO()
        if mpl_format == "gif":
            # fig.savefig doesn't support saving to GIFs directly, so we need
            # to make a one-frame animation instead.

            # HACK: need to write the file into a temporary file since
            # AnimationWriters can't write to file objects for some bizarre
            # reason
            from tempfile import NamedTemporaryFile
            import os
            with NamedTemporaryFile(delete=False, suffix="."+mpl_format) as f:
                anim = animation.FuncAnimation(fig, lambda t: (ax,), frames=1)
                anim.save(f.name, writer="pillow")
                f.seek(0)
                image.write(f.read())
            os.remove(f.name)
        else:
            fig.savefig(image, format=mpl_format)
        plt.close()
        image.seek(0)

        return send_file(image, mime_format), 200

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
