"""pytest 公共夹具：连接本地 Iceberg 仓库（由 scripts/run_local.sh 先行建仓灌数）。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "jobs"))

CONF_PATH = os.environ.get("PIPELINE_CONF", str(ROOT / "conf/pipeline.local.conf"))


@pytest.fixture(scope="session")
def spark():
    from common.spark_utils import build_spark, load_conf
    conf = load_conf(CONF_PATH)
    sess = build_spark("ltl_pytest", conf)
    yield sess
    sess.stop()


@pytest.fixture(scope="session")
def conf():
    from common.spark_utils import load_conf
    return load_conf(CONF_PATH)
