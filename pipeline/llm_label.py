"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import json
import re

import duckdb

from .embed import text_hash

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if it is not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def _ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Cache + quarantine + the Gold label table (current model/prompt only)."""
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        ticket_id VARCHAR, label VARCHAR, raw_answer VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_quarantine (
        input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        ticket_id VARCHAR, raw_answer VARCHAR, reason VARCHAR)""")


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """LLM labelling step with a hash cache (slide "LLM là một bước transform").

    Cache key = hash(input) + model + prompt version. A re-run with the same key
    makes 0 calls; a new prompt version is a cache miss and re-labels everything
    ON PURPOSE. Every answer is validated against the schema: an off-schema answer
    is cached (so it is not re-asked) and quarantined, never written to Gold.
    """
    model, prompt_version = llm.model, PROMPT_VERSION
    _ensure_tables(con)

    # 1. Which live tickets are already cached under this (model, prompt version)?
    cached = {
        (h, tid) for h, tid in con.execute(
            """SELECT input_hash, ticket_id FROM llm_label_cache
               WHERE model = ? AND prompt_version = ?""",
            [model, prompt_version]).fetchall()
    }

    # 2. Call the model only for the cache misses; write every answer to the cache.
    calls = 0
    for ticket_id, text in live_tickets(con):
        h = text_hash(text)
        if (h, ticket_id) in cached:
            continue
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        calls += 1
        label = parse_label(raw)
        con.execute(
            "INSERT INTO llm_label_cache VALUES (?, ?, ?, ?, ?, ?)",
            [h, model, prompt_version, ticket_id, label, raw])

    # 3. Rebuild Gold (valid labels only) and quarantine for the CURRENT version,
    #    so a prompt-version bump replaces the table with the new labels.
    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels AS
        SELECT ticket_id, label, model, prompt_version
        FROM llm_label_cache
        WHERE model = ? AND prompt_version = ? AND label IS NOT NULL
        ORDER BY ticket_id""", [model, prompt_version])
    con.execute("""CREATE OR REPLACE TABLE llm_label_quarantine AS
        SELECT input_hash, model, prompt_version, ticket_id, raw_answer,
               'answer does not match {"label": bug|billing|other}' AS reason
        FROM llm_label_cache
        WHERE model = ? AND prompt_version = ? AND label IS NULL
        ORDER BY ticket_id""", [model, prompt_version])

    (n_labelled,) = con.execute(
        "SELECT count(*) FROM gold_ticket_labels").fetchone()
    (n_quarantined,) = con.execute(
        "SELECT count(*) FROM llm_label_quarantine").fetchone()
    return {"labeled": n_labelled, "calls": calls, "quarantined": n_quarantined,
            "model": model, "prompt_version": prompt_version}
