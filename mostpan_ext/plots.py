"""
Serves plots of TBGs statistics.

Matplotlib is required to create the charts. If not present, all view functions
using them will return a 501.

.. _plotting:

Plotting parameters
-------------------

The following query string parameters configures the output figure.

:param type: The output type of the figure.

.. important::

    The ``Accept`` header takes precedence to `type` if present. Newer browsers
    usually include this header, so `type` may have no effect there.

:param width: The width of the figure in inches. Defaults to ``6.4`` inches.
:param height: The height of the figure in inches. Defaults to ``4.8`` inches.
:param dpi: Density of the plot. Defaults to ``96`` dpi.
"""

from flask import current_app, Blueprint, g, request, url_for, send_file
from werkzeug.exceptions import NotAcceptable
from werkzeug.datastructures import MultiDict

from .more_stats import first, to_bool, RANGE_LIMIT, DATE_FORMATS
from . import more_stats as stats

from io import BytesIO
from datetime import datetime, timedelta
import re
from functools import partial, wraps, lru_cache


logger = current_app.logger.getChild("plots")


try:
    import matplotlib  # noqa
    import numpy as np
    matplotlib.use('agg')

except ImportError:
    logger.warning(
        "Cannot import matplotlib, plotting functions will not work"
    )


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

init_db = current_app.config.init_db
api = g.blueprints.get("api", None)


if api is not None:
    plots_api = Blueprint('plots', __name__, url_prefix="/plots")

    # To make the cache actually work, it has to be outside the function
    topic_cache = lru_cache(1000)
    user_cache = lru_cache(1000)
    board_cache = lru_cache(1000)
    def to_human_conditions(cond):  # noqa
        "Turn SQL conditions into human-readable conditions."
        with init_db() as db:
            cur = db.cursor()

            @topic_cache
            def topic(match):  # noqa
                return cur.execute(
                    "select topic_name from Topics where tid=?",
                    (int(match.group(1)),)
                ).fetchone()["topic_name"]
            cond = re.sub(r"tid=(\d+)", topic, cond)

            @user_cache
            def user(match):  # noqa
                return cur.execute(
                    "select name from Users where uid=?",
                    (int(match.group(1)),)
                ).fetchone()["name"]
            cond = re.sub(r"user=(\d+)", user, cond)

            @board_cache
            def board(match):  # noqa
                return cur.execute(
                    "select board_name from Boards where bid=?",
                    (int(match.group(1)),)
                ).fetchone()["board_name"]
            cond = re.sub(r"bid=(\d+)", board, cond)

        if cond == "1":
            cond = "all"

        return cond

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

    @plots_api.route("/counts/<sample>")
    @process_figure
    def message_count_over_time(sample):  # noqa
        """Plot the message count over time under a certain time range.

        In addition to these parameters, this requests uses:
        - :ref:`plotting parameters <plotting>`
        - parameters from :py:func:`stats.message_counts_over_time()`

        :param bool human: Use human-readable labels. Defaults to ``True``.
        :param bool dots: Use dots on data points. Defaults to ``False``.
        """
        from matplotlib import dates

        args = request.args
        human_readable = args.get("human", default=True, type=to_bool)

        result, code = stats.message_count_over_time(sample)
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
        locator = dates.AutoDateLocator(maxticks=round(fig.get_figwidth()))
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(
            dates.AutoDateFormatter(locator)
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
                plot_label = to_human_conditions(label)
            else:
                plot_label = label
            ax.plot(
                mpl_times, [counts[time][label] for time in times],
                label=plot_label,
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

    @plots_api.route("/activity")
    @process_figure
    def activity():  # noqa
        """Plot a GitHub-style activity chart.

        Note that this plot is only for a single user. For a more general
        visualization, try :py:func:`plot_stripes()` instead.

        In addition to these parameters, this requests uses:
        - :ref:`plotting parameters <plotting>`
        - parameters from :py:func:`stats.message_counts_over_time()`

        :param bool hatches: Mark missing data with hatches.
                             Defaults to ``False``.
        :param bool discrete: Use discrete colors instead of a continuous one.
                              Defaults to ``False``.
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
                    f" {url_for('api.plots.stripes')}"
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
            result, code = stats.message_count_over_time("daily")
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

    @plots_api.route("/stripes", defaults={"sample": "monthly"})
    @plots_api.route("/stripes/<sample>")
    @process_figure
    def stripes(sample):  # noqa
        """Creates a heatmap of differing categories over time.

        In addition to these parameters, this requests uses:
        - :ref:`plotting parameters <plotting>`
        - parameters from :py:func:`stats.message_counts_over_time()`

        :param bool hatches: Mark missing data with hatches.
                             Defaults to ``False``.
        :param bool color: Use this matplotlib colormap color.
                           Defaults to ``Greens``.
        """
        from matplotlib import dates
        args = MultiDict(request.args)
        # Set default values for the date limits.
        now = datetime.now()
        if "end" not in args:
            args["end"] = now.isoformat()
        if "start" not in args:
            args["start"] = (now - RANGE_LIMIT[sample] / 3).isoformat()
        human_readable = args.get("human", default=True, type=to_bool)

        # Query the database.
        with current_app.test_request_context(query_string=args):
            result, code = stats.message_count_over_time(sample)
        if code != 200:
            return result, code
        counts = result["counts"]

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
        # Assign titles for the figure.
        fig.suptitle(
            "Messages posted over time",
            fontsize=16
        )
        ax.set_title(f"from {result['start']} to {result['end']}", fontsize=10)

        # Fill 'er up!
        column_idx = {x: i for i, x in enumerate(counts)}
        # bespoke sorted sets
        row_idx = {row: None for columns in counts.values() for row in columns}
        row_idx = {x: i for i, x in enumerate(row_idx)}
        heatmap = np.zeros((len(row_idx), len(column_idx)))
        for column, rows in counts.items():
            for row, item in rows.items():
                heatmap[row_idx[row], column_idx[column]] = item
        ax.imshow(
            heatmap,
            aspect="auto", interpolation="nearest",
            cmap=args.get("color", default="Greens"),
            extent=[datetime.fromisoformat(result['start']),
                    datetime.fromisoformat(result['end']),
                    len(row_idx) - 0.5,
                    -0.5]
        )

        # Assign ticks for the axes.
        ax.xaxis_date()
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
        ax.set_yticks(list(row_idx.values()),
                      labels=[to_human_conditions(cond)
                              if human_readable
                              else cond
                              for cond in row_idx.keys()])

        return fig, 200

    @plots_api.route("/counts/topic")
    @process_figure
    def message_count_by_topic():  # noqa
        """Plot the message count by topic.

        In addition to these parameters, this requests uses:
        - :ref:`plotting parameters <plotting>`
        - parameters from :py:func:`stats.message_counts_over_topic()`

        :param bool human: Use human-readable labels. Defaults to ``True``.
        :param chart: Use this chart type. Supported types are "bar"
                      (the default) and "pie".
        :param bool label: Add a label to each data point.
                           Defaults to ``False``.
        """
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
        result, code = stats.message_count_by_topic(
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
            fig.legend(
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

    current_app.config.other_api_examples.update({
        "plot_message_counts_over_time":
        partial(url_for,
                "api.plots.message_count_over_time",
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
        "plot_message_counts_by_topic":
        partial(url_for,
                "api.plots.message_count_by_topic",
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

    api.register_blueprint(plots_api, path="/")
