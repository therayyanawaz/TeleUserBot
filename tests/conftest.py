from __future__ import annotations

import importlib

import pytest

import auth
import db


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    db_path = tmp_path / "teleuserbot-test.db"
    monkeypatch.setattr(auth, "DB_PATH", str(db_path))
    monkeypatch.setattr(db, "DB_PATH", str(db_path))
    importlib.reload(db)
    monkeypatch.setattr(db, "DB_PATH", str(db_path))
    db.init_db()
    return db_path
