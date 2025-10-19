from flask import Flask, g


def app_creator(callbacks):  # noqa
    """Returns a function that creates a Flask app."""
    def create_app():  # noqa
        app = Flask(__name__)
        blueprints = {}

        with app.app_context():
            g.blueprints = blueprints
            for callback in callbacks:
                callback(app)
            import mostpan_ext  # noqa

        for bp in blueprints.values():
            app.register_blueprint(bp)

        return app
    return create_app
