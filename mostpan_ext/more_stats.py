"""
Serves more statistics of the TBGDB's data, mostly in chart form.

Matplotlib is required to create the charts. If not present, all view functions
using them will return a 501.
"""

from flask import current_app, Blueprint, g, request, url_for, send_file
from werkzeug.exceptions import NotAcceptable

from io import BytesIO
from datetime import datetime, timedelta
import re
from functools import partial, wraps, lru_cache
from collections import Counter


logger = current_app.logger.getChild("more_stats")


try:
    import matplotlib  # noqa
    import numpy as np
    matplotlib.use('agg')
except ImportError:
    logger.warning(
        "Cannot import matplotlib, plotting functions will not work"
    )


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
        "weekly": timedelta(weeks=200),
        "monthly": timedelta(weeks=1500),
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
    MAX_PLOT_DOTS = 20_000_000
    "Maximum area for plots in square dots."

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

    def make_figure(**kwargs):  # noqa
        from matplotlib import pyplot as plt

        args = request.args
        fig = plt.figure(
            figsize=(
                args.get("width", default=6.4, type=float),
                args.get("height", default=4.8, type=float),
            ),
            dpi=args.get("dpi", default=96, type=float),
            **kwargs,
        )
        dimensions = fig.get_size_inches() * fig.get_dpi()
        if dimensions[0] * dimensions[1] > MAX_PLOT_DOTS:
            raise ValueError("dimensions too large")
        return fig

    def process_figure(func):  # noqa
        @wraps(func)  # noqa
        def wrapper(*args, **kwargs):  # noqa
            from matplotlib import pyplot as plt, animation
            from matplotlib.figure import Figure

            # Retrieve the desired content type.
            if len(accept_types := request.accept_mimetypes) > 0:
                # from the Accept header (this is given priority)
                mime_format = accept_types.best_match(MIME_MPL_TYPES)
                if mime_format is None:
                    raise NotAcceptable(
                        TypeError(
                            "unsupported type(s)"
                            f" (supported types: {', '.join(MPL_MIME_TYPES)})"
                        )
                    )
                mpl_format = MIME_MPL_TYPES[mime_format]
            else:
                # or from the type=... search query
                mpl_format = args.get("type", default="svg")
                if mpl_format not in MPL_MIME_TYPES:
                    raise TypeError(
                        "unsupported type(s)"
                        f" (supported types: {', '.join(MPL_MIME_TYPES)})"
                    )
                mime_format = MPL_MIME_TYPES[mpl_format]

            plt.ioff()
            fig, code = func(*args, **kwargs)

            if not isinstance(fig, Figure):
                return fig, code
            image = BytesIO()
            if mpl_format == "gif":
                # fig.savefig doesn't support saving to GIFs directly, so
                # we need to make a one-frame animation instead.

                # HACK: need to write the file into a temporary file since
                # AnimationWriters can't write to file objects for some
                # bizarre reason
                from tempfile import NamedTemporaryFile
                import os
                with NamedTemporaryFile(delete=False,
                                        suffix="."+mpl_format) as f:
                    anim = animation.FuncAnimation(
                        fig, lambda t: fig.axes, frames=1
                    )
                    anim.save(f.name, writer="pillow")
                    f.seek(0)
                    image.write(f.read())
                os.remove(f.name)
            else:
                fig.savefig(image, format=mpl_format)
            image.seek(0)
            plt.close()

            return send_file(image, mime_format), code
        return wrapper

    def to_human_conditions(cond):  # noqa
        cur = db.cursor()

        @lru_cache(1000)
        def topic(match):  # noqa
            return cur.execute("select topic_name from Topics where tid=?",
                               (int(match.group(1)),)).fetchone()["topic_name"]
        cond = re.sub(r"tid=(\d+)", topic, cond)

        @lru_cache(1000)
        def user(match):  # noqa
            return cur.execute("select name from Users where uid=?",
                               (int(match.group(1)),)).fetchone()["name"]
        cond = re.sub(r"user=(\d+)", user, cond)

        @lru_cache(1000)
        def board(match):  # noqa
            return cur.execute("select board_name from Boards where bid=?",
                               (int(match.group(1)),)).fetchone()["board_name"]
        cond = re.sub(r"bid=(\d+)", board, cond)

        return cond

    @stats_api.route("/counts/<sample>")
    def message_count_over_time(sample):  # noqa
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

        if (
            len(user_conditions)
            * len(topic_conditions)
            * len(board_conditions)
            > 100
        ):
            raise ValueError("too many conditions")

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

    @stats_api.route("/plot/counts/<sample>")
    @process_figure
    def plot_message_count_over_time(sample):  # noqa
        from matplotlib import dates

        args = request.args
        human_readable = args.get("human", default=True, type=to_bool)

        result, code = message_count_over_time(sample)
        if code != 200:
            return result, code
        counts = result["counts"]

        # Make the plot.
        fig = make_figure(layout="tight")
        ax = fig.subplots()

        ax.xaxis.set_minor_locator(
            dates.MonthLocator()
            if sample == "monthly"
            else dates.WeekdayLocator()
            if sample == "weekly"
            else dates.DayLocator()
            if sample == "daily"
            else dates.HourLocator()
            # if sample == "hourly"
        )
        ax.xaxis.set_major_locator(
            dates.AutoDateLocator(maxticks=round(fig.get_figwidth()))
        )
        ax.set_ylabel(
            "Posts"
            if args.get("cumulative", default=False, type=to_bool)
            else "Posts per %s" % (
                # of all the adjectives that we support and has the -ly suffix,
                # only "daily" is the irregular one
                "day"
                if sample == "daily"
                else sample[:-2]
            ),
        )
        fig.suptitle(
            "Messages posted over time",
            fontsize=16
        )
        ax.set_title(f"from {result['start']} to {result['end']}", fontsize=10)

        times = sorted(counts)
        mpl_times = [
            datetime.strptime(time, DATE_FORMATS[sample])
            # %V requires %w to work properly
            if sample != "weekly"
            else datetime.strptime(time+";0", DATE_FORMATS[sample]+";%w")
            for time in times
        ]
        # Fill 'er up!
        for label in first(counts.values(), {}).keys():
            if human_readable:
                label = to_human_conditions(label)
            ax.plot(
                mpl_times, [counts[time][label] for time in times],
                label=label,
                marker=(
                    "." if args.get("dots", default=False, type=to_bool)
                    else ""
                ),
            )

        for label in ax.get_xticklabels():
            label.set_rotation(25)
            label.set_horizontalalignment('right')
        ax.grid(which="both")
        ax.legend()

        return fig, 200

    @stats_api.route("/plot/activity")
    @process_figure
    def plot_activity():  # noqa
        """Creates a GitHub-style activity chart.

        """
        from matplotlib import colors
        args = request.args

        # This plot is only meant for a single user.
        users = len(args.getlist("user"))
        if users != 1:
            error = ValueError("can only plot a single user")
            if users > 1:
                error.add_note(
                    "For multi-user plots, use"
                    # f" {url_for('api.stats.plot_stripes')}"
                    " (NOT IMPLEMENTED YET)"
                )
            raise error
        # Likewise, this plot only presents a single dimension over time.
        if (
            not args.get("combine_users", default=True, type=to_bool)
            or not args.get("combine_topics", default=True, type=to_bool)
            or not args.get("combine_boards", default=True, type=to_bool)
        ):
            raise ValueError("can only plot a single condition")

        # Query the database.
        now = datetime.now()
        with current_app.test_request_context(
            query_string={**args,
                          "start": (now - RANGE_LIMIT["daily"]).isoformat(),
                          "end": now.isoformat()}
        ):
            result, code = message_count_over_time("daily")
        if code != 200:
            return result, code
        counts = result["counts"]
        username = to_human_conditions(result['conditions']['user'][0])
        # Currently the dates are strings, so convert them
        counts = {
            datetime.strptime(dt, DATE_FORMATS["daily"]): count
            for dt, count in counts.items()
        }
        start = datetime.fromisoformat(result["start"]).date()
        end = datetime.fromisoformat(result["end"]).date()
        date_range = (start + timedelta(days=i)
                      for i in range((end - start).days + 1))

        # Make the table.
        activity = {dt.isocalendar()[:-1]: [float("nan") for _ in range(7)]
                    for dt in date_range}
        for date in counts:
            count = first(counts[date].values())
            year, week, week_day = date.isocalendar()
            activity[year, week][week_day - 1] = count

        # Make the plot.
        fig = make_figure(layout="tight")
        ax = fig.subplots()
        # Make stripes to be put atop the heatmap (to distinguish 0 with NaN)
        if args.get("hatches", default=False, type=to_bool):
            ax.axhspan(
                -1, 8,
                hatch=r"xx", zorder=-1,
                facecolor="magenta", edgecolor="black"
            )
        # Transpose to make the week number the first dimension
        heatmap = [[week[week_day] for week in activity.values()]
                   for week_day in range(7)]
        max_heatmap = max(day
                          for week in activity.values()
                          for day in week
                          # all numbers are integers except NaNs
                          if type(day) is int)
        if args.get("discrete", default=False, type=to_bool):
            norm = colors.BoundaryNorm(np.linspace(0, max_heatmap, 6), 256)
        else:
            norm = colors.Normalize(0, max_heatmap)
        image = ax.imshow(heatmap, cmap="Greens", norm=norm)
        # Find places to put the ticks
        weeks = list(activity)
        start_month_idx = start.year*12 + start.month
        end_month_idx = end.year*12 + end.month
        months = (divmod(x, 12) for x in range(start_month_idx, end_month_idx))
        months = (datetime(year, month, 1).date() for year, month in months)
        months = {
            dt.strftime("%b" if (end - start).days < 366 else "%Y %b"):
            weeks.index(dt.isocalendar()[:-1])
            for dt in months
            if start <= dt <= end
        }

        # Make the plot look nice
        ax.set_xticks(list(months.values()), labels=list(months.keys()))
        ax.set_yticks(
            range(7),
            labels=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        )
        ax.set_title(f"Activity of {username}")
        fig.colorbar(image, label="Posts per day")
        for label in ax.get_xticklabels():
            label.set_horizontalalignment('right')
        # Make a grid around the boxes
        ax.set_xticks(np.arange(len(activity)+1) - 0.5, minor=True)
        ax.set_yticks(np.arange(8) - 0.5, minor=True)
        ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
        ax.tick_params(which="minor", bottom=False, left=False)
        ax.grid

        return fig, 200

    @stats_api.route("/counts/topic")
    def message_count_by_topic(custom_defaults={}):  # noqa
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

        # If necessary, assemble the topic conditions
        topic_conditions = "1"
        cur = db.cursor()
        if shared_only:
            topic_conditions = None
            for user in user_conditions:
                query = cur.execute(
                    f"""
                    select tid, count(*) as count
                    from Messages
                    where {user}
                    group by tid
                    order by count desc
                    """
                )
                countie = Counter({row["tid"]: row["count"] for row in query})
                if topic_conditions is None:
                    topic_conditions = countie
                else:
                    topic_conditions &= countie
            topic_conditions = sorted(topic_conditions.items(),
                                      key=lambda x: x[1], reverse=True)
            if not include_others:
                topic_conditions = topic_conditions[:limit]
            topic_conditions = " or ".join(
                f"tid={k}"
                for k, v in topic_conditions
            )

        # Read the database
        result = {}
        for user in user_conditions:
            query = cur.execute(
                f"""
                select {key_name} as key, count(*) as count
                from Messages
                    join Topics using (tid)
                where ({user})
                    and ({topic_conditions})
                    and ({board_conditions})
                    and ({time_conditions})
                group by tid
                order by count desc
                """ + (
                    f"limit {limit}"
                    if not include_others
                    else ""
                )
            )
            for i, row in enumerate(query):
                if i < limit:
                    category = result.setdefault(row["key"], {})
                    category[user] = row["count"]
                else:
                    category = result.setdefault("(other)", {})
                    category[user] = category.get(user, 0) + row["count"]

        return {
            "conditions": {
                "user": user_conditions,
                "board": board_conditions,
                "topic": topic_conditions,
                "time": time_conditions,
            },
            "counts": result
        }, 200

    @stats_api.route("/plot/counts/topic")
    @process_figure
    def plot_message_count_by_topic():  # noqa
        from math import isqrt
        MOSAICS = {
            1: "1",
            2: "12",
            3: "123",
            4: "12;34",
            5: "111222;334455",
            6: "123;456",
            7: "12.;345;.67",
            8: "112233;444555;667788",
            9: "123;456;789",
        }

        args = request.args
        human_readable = args.get("human", default=True, type=to_bool)

        chart_type = args.get("chart", default="bar")
        label_values = args.get("label", default=False, type=to_bool)
        result, code = message_count_by_topic(
            custom_defaults=dict(
                limit=9,
                others=chart_type == "pie"
            )
        )
        if code != 200:
            return result, code
        counts = result["counts"]
        conditions = result["conditions"]["user"]

        fig = make_figure(layout="tight")
        fig.suptitle("Message count by their topics")
        if chart_type == "bar":
            # The bar chart will use the topics as the category (the Y axis)
            ax = fig.subplots()
            width = 1 / (len(conditions) + 0.5)
            cond_offsets = np.arange(len(conditions)) * width
            cond_offsets -= np.mean(cond_offsets)
            cond_offsets = cond_offsets[::-1]
            cat_offsets = np.arange(len(counts))

            for i, cond in enumerate(conditions):
                rects = ax.barh(
                    cat_offsets + cond_offsets[i],
                    [x.get(cond, 0) for x in counts.values()],
                    height=width,
                    label=(
                        to_human_conditions(cond)
                        if human_readable
                        else cond
                    ),
                )
                if label_values:
                    ax.bar_label(rects, padding=3)

            ax.legend()
            ax.set_yticks(cat_offsets, counts.keys())
            ax.grid(axis="x")
            return fig, 200
        elif chart_type == "pie":
            # The pie chart will present the proportion of topics posted by
            # each user
            if len(conditions) > 9:
                raise ValueError("too much conditions to plot")
            if args.get("limit", default=9, type=int) > 9:
                raise ValueError(
                    "too much categories to plot"
                    f" ({len(counts)} > 10)"
                )
            axs = fig.subplot_mosaic(MOSAICS[len(conditions)])
            for i, cond in zip(axs, conditions):
                data = {
                    topic: count[cond]
                    for topic, count in sorted(
                        counts.items(),
                        key=lambda x: x[1].get(cond, 0),
                        reverse=True
                    )
                    if cond in count
                }
                axs[i].pie(
                    data.values(),
                    autopct=("%1.1f%%" if label_values else ""),
                    radius=0.75
                )
                axs[i].set_ylim(-1.5, 0.75)
                axs[i].set_title(cond, fontsize=10)
                axs[i].legend(
                    labels=data.keys(),
                    loc="lower center",
                    ncols=(
                        1
                        if (
                            args.get("key", default="topic_name")
                            == "topic_name"
                        )
                        else isqrt(len(data))
                    ),
                    fontsize="small"
                )

            return fig, 200
        else:
            raise ValueError("invalid chart type")

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
                "api.stats.message_count_over_time",
                sample="hourly,daily,weekly,monthly",
                user="...",
                topic="...",
                board="...",
                start="ISOdate...",
                end="ISOdate...",
                cumulative="true,false"),
        "plot_message_counts_over_time":
        partial(url_for,
                "api.stats.plot_message_count_over_time",
                sample="hourly,daily,weekly,monthly",
                user="...",
                topic="...",
                board="...",
                start="ISOdate...",
                end="ISOdate...",
                cumulative="true,false",
                width="...",
                height="...",
                dpi="...",
                human="true,false",),
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
        "plot_message_counts_by_topic":
        partial(url_for,
                "api.stats.plot_message_count_by_topic",
                user="...",
                board="...",
                start="ISOdate...",
                end="ISOdate...",
                shared="true,false",
                key="tid,topic_name",
                others="true,false",
                width="...",
                height="...",
                dpi="...",
                human="true,false",)
    })

    api.register_blueprint(stats_api, path="/")
