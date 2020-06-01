# pylint: disable=import-error
from app.factories.app_factory import make_flask

flask_app = make_flask()

if __name__ == "__main__":
    flask_app.run(debug=True)
