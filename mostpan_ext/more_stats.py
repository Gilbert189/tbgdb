"""
Serves more statistics of the TBGDB's data.
"""

from flask import current_app, Blueprint, g, request, url_for

from datetime import datetime, timedelta
import re
from functools import partial
import json  # to stringify strings


logger = current_app.logger.getChild("more_stats")


init_db = current_app.config.init_db
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
        "weekly": timedelta(weeks=200),
        "monthly": timedelta(weeks=1500),
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

    def first(x, *args):  # noqa
        "Gets the first item of an iterator."
        if len(args) == 1:
            return next(iter(x), args[0])
        else:
            return next(iter(x))

    def datetime_range(start, end, step,  # noqa
                        /, extra=False, inclusive=False):  # noqa
        """Creates a generator yielding a range of datetimes.

        :param extra: Include the first value not smaller than `end`.
                      This doesn't necessarily equal to `end`.
        :param inclusive: Include the first value not smaller than `end`
                          only if it's equal to `end`."""
        value = start
        while value < end:
            yield value
            value += step
        if (inclusive and value == end) or extra:
            yield value

    @stats_api.route("/counts/<sample>")
    def message_count_over_time(sample):  # noqa
        """Count how many messages are posted under a certain time range.

        :param sample: The sample range for each data point.
        :param ISOdate start: Count messages posted after this time.
        :param ISOdate end: Count messages posted before this time.
        :param list[str] user: Select these user IDs.
                               If not included, all users will be selected.
        :param list[str] topic: Select these topic IDs.
                                If not included, all topics will be selected.
        :param list[str] board: Select these board IDs.
                                If not included, all boards will be selected.
        :param bool combine_users: Combine multiple users into a single
                                   category. Defaults to ``True``.
        :param bool combine_topics: Combine multiple topics into a single
                                    category. Defaults to ``True``.
        :param bool combine_boards: Combine multiple boards into a single
                                    category. Defaults to ``True``.
        :param bool cumulative: Whether to accumulate the count over time.
                                Defaults to ``False``.
        :param bool fill: Whether to fill missing dates.
                          Defaults to ``True``.
        """
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
            "sum(count(case when %s then 1 else null end))"
            " over (order by strftime(:datefmt, date))"
            if cumulative
            else "count(case when %s then 1 else null end)"
        )

        # Make sure we don't query the database too much
        if (
            len(user_conditions)
            * len(topic_conditions)
            * len(board_conditions)
            > 100
        ):
            raise ValueError("too many conditions")
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

        # Query the database.
        if args.get("fill", default=True, type=to_bool):
            result = {
                dt.strftime(DATE_FORMATS[sample]): {}
                for dt in datetime_range(
                    start_range, end_range, (
                        timedelta(hours=1) if sample == "hourly"
                        else timedelta(days=1) if sample == "daily"
                        else timedelta(weeks=1) if sample == "weekly"
                        else timedelta(days=28)  # if sample == "monthly"
                    ),
                    extra=False
                )
            }
        else:
            result = {}
        with init_db() as db:
            cur = db.cursor()
            query = cur.execute(
                f"""
                select
                    strftime(:datefmt, date) as time,
                    {",".join(
                        ("%s as %%s" % count_criteria)
                        % (cond, repr(cond))
                        for cond in combinations
                    )}
                from Messages
                    join Topics using (tid)
                    join Boards using (bid)
                where ({time_conditions})
                group by time
                """,
                {"datefmt": DATE_FORMATS[sample]}
            ).fetchall()
            for item in query:
                time = item["time"]
                del item["time"]
                result[time] = item
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

    @stats_api.route("/counts/topic")
    def message_count_by_topic(custom_defaults={}):  # noqa
        """Count how many messages are posted on a topic.

        :param ISOdate start: Count messages posted after this time.
        :param ISOdate end: Count messages posted before this time.
        :param int limit: Only include this many top-posted topics.
                          Defaults to 100.
        :param list[str] user: Select these user IDs.
                               If not included, all users will be selected.
        :param list[str] board: Select these board IDs.
                                If not included, all boards will be selected.
        :param bool combine_users: Combine multiple users into a single
                                   category. Defaults to ``True``.
        :param bool others: Also count topics that exceeds the limit,
                            categorized as ``(other)``. Defaults to ``False``.
        :param bool shared: Only select topics that all users have posted on.
                            Defaults to ``False``.
        :param key: What value to use as the key. Could be either ``topic``or
                    ``topic_name`` (the default).
        """
        args = request.args

        limit = args.get(
            "limit",
            default=custom_defaults.get("limit", 100),
            type=int
        )
        include_others = args.get(
            "others",
            default=custom_defaults.get("others", False),
            type=to_bool
        )
        shared_only = args.get(
            "shared",
            default=custom_defaults.get("shared", False),
            type=to_bool
        )
        key_name = args.get(
            "key",
            default=custom_defaults.get("key", "topic_name"),
            type=str
        )

        if key_name not in ("tid", "topic_name"):
            raise ValueError('key should either be "tid" or "topic_name"')

        # Assemble the user conditions
        user_conditions = [
            f"user={int(uid)}"
            for uid in args.getlist("user")
        ]
        if user_conditions == []:
            user_conditions = ["1"]
        if args.get("combine_users", default=True, type=to_bool):
            user_conditions = [" or ".join(user_conditions)]
        # Assemble the board conditions
        # Unlike message_count_over_time(), combine_board is always True
        board_conditions = " or ".join(
            f"bid={int(bid)}"
            for bid in args.getlist("board")
        )
        if board_conditions == "":
            board_conditions = "1"
        # Assemble the time conditions
        time_conditions = []
        start_range = args.get("start", type=datetime.fromisoformat)
        if start_range is not None:
            time_conditions.append(start_range.timestamp())
        end_range = args.get("end", type=datetime.fromisoformat)
        if end_range is not None:
            time_conditions.append(end_range.timestamp())
        time_conditions = " and ".join(time_conditions)
        if time_conditions == "":
            time_conditions = "1"

        if len(user_conditions) > 100:
            raise ValueError("too much conditions")

        # Query the database
        with init_db() as db:
            result = {}
            cur = db.cursor()
            # SQL wants quotes for names, Python uses apostrophes
            row_names = [json.dumps(user) for user in user_conditions]
            if shared_only:
                having_clause = "having " + " and ".join(
                    row + ">0"
                    for row in row_names
                )
            else:
                having_clause = ""
            order_expr = "+".join(row_names)
            if include_others:
                # We would handle the limiting in this function
                limit_clause = ""
            else:
                # Let SQLite limit the result
                limit_clause = f"limit {limit}"
            query = cur.execute(
                f"""
                select
                    {key_name} as key,
                    {",".join(
                        "count(case when %s then 1 else null end) as %s"
                        % (user, row)
                        for user, row in zip(user_conditions, row_names)
                    )}
                from Messages
                    join Topics using (tid)
                where ({board_conditions}) and ({time_conditions})
                group by tid
                {having_clause}
                order by {order_expr} desc
                {limit_clause}
                """
            )
            for i, row in enumerate(query):
                key = row["key"]
                del row["key"]
                if i < limit:
                    result[key] = row
                else:
                    category = result.setdefault("(other)", {})
                    # TODO: rewrite this
                    for user, count in row.items():
                        category[user] = category.get(user, 0) + count

        return {
            "conditions": {
                "user": user_conditions,
                "board": board_conditions,
                "time": time_conditions,
            },
            "counts": result
        }, 200

    @stats_api.route("/complete")
    def completeness():  # noqa
        """Give statistics of how complete the database is."""
        with init_db() as db:
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
                "api.stats.message_count_over_time",
                sample="hourly,daily,weekly,monthly",
                user="...",
                topic="...",
                board="...",
                start="ISOdate...",
                end="ISOdate...",
                cumulative="true,false"),
        "message_counts_by_topic":
        partial(url_for,
                "api.stats.message_count_by_topic",
                user="...",
                board="...",
                start="ISOdate...",
                end="ISOdate...",
                shared="true,false",
                key="tid,topic_name",
                others="true,false"),
    })

    api.register_blueprint(stats_api, path="/")
