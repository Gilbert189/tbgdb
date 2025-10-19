from flask import g

import server as api
from _creator import app_creator


# TODO: the website of TBGDB


def build_config(app):  # noqa
    g.blueprints["api"].url_prefix = "/api"


create_app = app_creator([api.build_config, build_config])
