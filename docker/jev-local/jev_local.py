#!/usr/bin/env python3
"""
jev_local.py - an offline stand-in for the Jev API (POST /v1/systemone), so the
lab's jev() examples run end to end without an API key and without sending
rows anywhere.

It is NOT a model. It answers with simple word matching plus a small lexicon
that covers the lab's sample data (angry customers, which team handles a
ticket, luxury levels, European countries...). Good enough to see jev() work
inside SQL, Spark and Airflow; for real judgments point jev.api_url at TypeSafe
(or stuntd / laya-server) instead. See Understand.md section 28.

Request (what the jev extension sends):
  {"model": "...", "state": {"condition": "...", "rows": [{...}, ...]},
   "questions": {"r0": {"type": "noul"|"score"|"choice", "instructions": "...",
                        "criteria": [levels] | {option: null}}}}
Response:
  {"answers": {"r0": {"type": "noul", "noul": 0.93}, ...}, "usage": {...}}

Run: python3 jev_local.py [port]      (default 8787, listens on 0.0.0.0)
"""
import json
import math
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STOP = set("""a an the is are was were be been being of to in on at for with by from and or not no
this that these those it its as into about which what who whom whose should would could can
does do did has have had will shall may might must than then there their they them he she his
her we you your our my me i any some all each every record row rows customer""".split())

# concept -> words that count as evidence for it
LEXICON = {
    "angry": "angry furious unacceptable terrible worst awful ridiculous outraged disgusted "
             "lawyer horrible useless scam !!! nobody third again",
    "european": "germany france spain italy portugal netherlands belgium austria poland sweden norway "
                "denmark finland ireland greece czech hungary switzerland romania uk england scotland "
                "europe european berlin paris madrid rome lisbon amsterdam vienna warsaw dublin",
    "billing": "invoice charge charged billing bill payment refund card subscription price overcharged "
               "double charged receipt credit",
    "technical": "error crash bug broken login loading slow api timeout install update app page "
                 "500 not working sync outage",
    "security": "password hacked breach phishing suspicious unauthorized 2fa mfa stolen fraud "
                "compromised locked takeover pretending impersonating logo two-factor authentication",
    "sales": "pricing quote demo plan upgrade enterprise discount trial seats purchase buy contract",
    "healthcare": "nurse doctor physician hospital clinic pharmacist surgeon therapist medical",
    "budget": "cheap basic budget value economy plastic entry",
    "mid-range": "standard everyday reliable solid",
    "premium": "premium leather professional pro steel quality",
    "luxury": "luxury handcrafted gold cashmere silk designer exclusive limited italian swiss",
    "urgent": "urgent asap immediately critical outage blocking emergency locked hacked fraud",
    "high": "broken crash crashes error cannot fails failed timeout refund twice charged",
    "medium": "slow wrong problem issue not working",
    "low": "question how thanks upgrade discount quote renewal address",
}
LEX = {k: set(re.findall(r"[a-z0-9-]+", v)) for k, v in LEXICON.items()}


def words(text):
    text = str(text).lower()
    ws = set(w for w in re.findall(r"[a-z0-9-]+", text) if w not in STOP)
    if "!!!" in text:
        ws.add("!!!")
    return ws


def evidence(concept_text, row_words):
    """How many row words support the concept (its own words + lexicon expansions)."""
    want = words(concept_text)
    for key, syn in LEX.items():
        if key in want or key in concept_text.lower():
            want |= syn
    return len(want & row_words)


def row_text(row):
    """Only the values: column names ("name", "body") would match every row."""
    return " ".join(str(v) for v in row.values()) if isinstance(row, dict) else str(row)


def price_of(row):
    for k in ("price", "list_price", "amount"):
        if isinstance(row, dict) and isinstance(row.get(k), (int, float)):
            return float(row[k])
    return None


def softmax(xs, t=1.0):
    m = max(xs)
    e = [math.exp((x - m) / t) for x in xs]
    s = sum(e)
    return [v / s for v in e]


def answer(q, row, condition):
    rw = words(row_text(row))
    kind = q.get("type")
    if kind == "noul":
        hits = evidence(condition, rw)
        p = 0.05 + 0.9 * (1 - math.exp(-1.2 * hits))
        return {"type": "noul", "noul": round(p, 4)}
    question = q.get("instructions", "").split(":", 1)[-1]
    if kind == "choice":
        opts = list((q.get("criteria") or {}).keys())
        scores = [evidence(o, rw) for o in opts]
        probs = softmax(scores, 0.7)
        k = max(range(len(opts)), key=lambda i: (scores[i], -i))
        return {"type": "choice", "choice": opts[k],
                "probabilities": {o: round(p, 4) for o, p in zip(opts, probs)},
                "confidence": round(probs[k], 4)}
    if kind == "score":
        levels = list(q.get("criteria") or [])
        n = len(levels)
        scores = [float(evidence(lv, rw)) for lv in levels]
        price = price_of(row)
        if price is not None and n > 1:
            # ordered levels + a price: cheaper rows lean to the low end
            pos = min(n - 1, max(0.0, math.log10(max(price, 1.0)) / 4.0 * (n - 1)))
            scores = [s + 2.0 * math.exp(-((i - pos) ** 2)) for i, s in enumerate(scores)]
        probs = softmax(scores, 0.7) if n else []
        score = sum(i * p for i, p in enumerate(probs))
        return {"type": "score", "score": round(score, 4),
                "legend": {str(i): lv for i, lv in enumerate(levels)},
                "probabilities": {str(i): round(p, 4) for i, p in enumerate(probs)},
                "confidence": round(max(probs) if probs else 0.0, 4)}
    raise ValueError(f"unknown question type {kind!r} ({question})")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"          # keep-alive, like the real API

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        self._send(200, {"service": "jev-local", "ok": True})

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            req = json.loads(body)
            state = req.get("state", {})
            rows, cond = state.get("rows", []), state.get("condition", "")
            answers = {qid: answer(q, rows[int(qid[1:])], cond) for qid, q in req["questions"].items()}
        except Exception as e:  # malformed request -> 422 like the real API
            return self._send(422, {"error": f"jev-local: {e}"})
        self._send(200, {"model": "jev-local-lexical", "answers": answers,
                         "usage": {"input_tokens": len(body) // 4, "output_tokens": len(answers)}})

    def _send(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8787
    print(f"jev-local listening on 0.0.0.0:{port} (lexical stand-in, not a model)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
