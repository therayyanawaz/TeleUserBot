from __future__ import annotations

import pytest

import auth
import config
import db


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db_path = tmp_path / "teleuserbot-test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(auth, "DB_PATH", str(db_path))
    monkeypatch.setattr(db, "DB_PATH", str(db_path))
    config._dynamic_digest_source_tiers = None
    db.init_db()
    yield
    config._dynamic_digest_source_tiers = None
