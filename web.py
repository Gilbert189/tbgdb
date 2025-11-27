import logging

from flask import g

import server as api
from _creator import app_creator


# NOTE: if your WSGI server already setups logging by itself, you can comment
# this line
logging.basicConfig(
    format="%(levelname)s@%(name)s: %(message)s",
    level=logging.INFO
)
# TODO: the website of TBGDB


def build_config(app):  # noqa
    g.blueprints["api"].url_prefix = "/api"


create_app = app_creator([api.build_config, build_config])
