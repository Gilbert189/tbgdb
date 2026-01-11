"""
Serves TBGDB database backups. Backups are made weekly.
"""

from flask import current_app, Blueprint, g, send_from_directory

from datetime import datetime, timedelta
import hashlib
from pathlib import Path
from time import sleep
import json
from threading import Thread, Event


logger = current_app.logger.getChild("backup")

BACKUP_DIR = Path("backup")
"Directory to store backups on."
MAX_BACKUPS = 4
"How much backups to store."


init_db = current_app.config.init_db
clock_running = Event()


def init_backup():  # noqa
    if not BACKUP_DIR.exists():
        logger.info("Backup directory not present, creating one")
        BACKUP_DIR.mkdir()

    if not (manifest_file := BACKUP_DIR / "manifest.json").exists():
        manifest_file.touch()

    with manifest_file.open("r") as f:
        try:
            manifest = json.load(f)
        except json.decoder.JSONDecodeError:
            dangling_files = [
                path
                for path in BACKUP_DIR.iterdir()
                if path.name not in (
                    "manifest.json", "backup.db", "backup.db-journal"
                )
            ]
            if len(dangling_files) != 0:
                logger.critical(
                    "Can't read manifest.json, but backups are found! "
                    "TBGDB can't determine when they are made. As such, "
                    "TBGDB can't recycle them and they will stay stored on "
                    "your machine.\n"
                    "In case you want to delete them, here are the backup "
                    "files:\n"
                    + "\n".join(f"- {file}" for file in dangling_files)
                )
            else:
                logger.info("Can't read manifest.json, writing one")
            manifest = {}

    return manifest


def make_backup():  # noqa
    logger.info("Making database backup")

    manifest = init_backup()

    if len(manifest) == MAX_BACKUPS:
        oldest, db_path = min(manifest.items())
        (BACKUP_DIR / db_path).unlink(missing_ok=True)
        del manifest[oldest]

    new_db_file = BACKUP_DIR / "backup.db"
    if new_db_file.exists():
        # Either there's two clocks or the backup got interrupted
        # It's better to abort the backup for now
        logger.warning(
            f"The file {new_db_file} exists and TBGDB assumes that another"
            " thread is currently using it. For this reason, TBGDB will abort"
            " this backup routine.\n"
            "If this file is still present after quite some time, that means"
            " the backup had stopped unexpectedly. You should delete that"
            " file so that TBGDB can backup the database properly."
        )
        return
    with init_db() as db:
        db.execute("vacuum into ?", (str(new_db_file),))
    with new_db_file.open("rb") as f:
        digest = hashlib.file_digest(f, "md5")
        backup_hash = digest.hexdigest()
        new_db_file = new_db_file.rename(BACKUP_DIR / backup_hash)

        now = datetime.now().isoformat(timespec='seconds')
        manifest[now] = backup_hash

    with open(BACKUP_DIR / "manifest.json", "w") as f:
        json.dump(manifest, f)


def backup_clock():  # noqa
    if clock_running.is_set():
        logger.info("Clock already running, not running again.")
        return
    else:
        logger.info("Starting backup clock")
        clock_running.set()
    while True:
        manifest = init_backup()
        if len(manifest) == 0:
            # fresh from the juice!
            make_backup()
        else:
            # TIP: ISO time are also ordered lexicographically
            latest_time = max(manifest.keys())
            latest_time = datetime.fromisoformat(latest_time)
            if datetime.now() - latest_time >= timedelta(weeks=1):
                make_backup()
        sleep(86400)  # do this daily


clock_thread = Thread(target=backup_clock, name="Backup-Clock", daemon=True)
clock_thread.start()


api = g.blueprints.get("api", None)
if api is not None:
    backup_api = Blueprint('backup', __name__, url_prefix="/backups")

    @backup_api.route("/")
    def list_backups():  # noqa
        """List all stored database backups."""
        manifest = init_backup()
        return manifest

    @backup_api.route("/<name>")
    def download_backup(name):  # noqa
        """List all stored database backups."""
        manifest = init_backup()
        reverse_manifest = {v: k for k, v in manifest.items()}

        return send_from_directory(
            BACKUP_DIR, name,
            as_attachment=True,
            download_name=f"backup-{reverse_manifest[name]}.db"
        )

    api.register_blueprint(backup_api, path="/")
