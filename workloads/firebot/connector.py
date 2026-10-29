"""ANANTA Fire Connector (skeleton). Tails Fire Bot's append-only JSONL, validates, minimizes (drops
anything not allow-listed), batches, and loads into ananta.firebot_events with retry. Fire Bot never
depends on this running: it only appends to a local file.
  python connector.py --file firebot_events.jsonl"""
import argparse, json, time, uuid, clickhouse_connect
from datetime import datetime, timezone

ALLOWED_META = {"cpu_pct", "mem_mb", "noise_level", "model_version", "device"}   # allow-list: no text/voice/ids
TYPES = {"voice_recognition_completed","speaker_rejected","speech_transcription_completed","response_generated",
         "tts_completed","model_loaded","training_run_completed","application_error"}
COLS = ["event_time","event_id","session_id","event_type","model_name","duration_ms","success","confidence","metadata","batch_id"]

def clean(e):
    if e.get("event_type") not in TYPES: return None
    meta = {k: str(v) for k, v in (e.get("metadata") or {}).items() if k in ALLOWED_META}
    return [datetime.fromisoformat(e["event_time"]).astimezone(timezone.utc), uuid.UUID(e.get("event_id") or str(uuid.uuid4())),
            uuid.UUID(e["session_id"]), e["event_type"], e.get("model_name", ""), int(e.get("duration_ms", 0)),
            bool(e.get("success", True)), e.get("confidence"), meta]

def flush(ch, rows, tries=5):
    bid = uuid.uuid4()
    for i in range(tries):
        try:
            ch.insert("ananta.firebot_events", [r + [bid] for r in rows], column_names=COLS,
                      settings={"insert_deduplicate": 1, "insert_deduplication_token": str(bid)}); return True
        except Exception as ex:
            time.sleep(2 ** i); print("retry", i, ex)
    return False   # keep rows; Fire Bot is unaffected

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--file", required=True); ap.add_argument("--host", default="localhost"); a = ap.parse_args()
    ch = clickhouse_connect.get_client(host=a.host, port=8123, username="ananta", password="ananta_dev")
    buf = []
    with open(a.file) as f:
        while True:
            line = f.readline()
            if not line:
                if buf and flush(ch, buf): buf = []
                time.sleep(1); continue
            try: r = clean(json.loads(line))
            except Exception: r = None
            if r: buf.append(r)
            if len(buf) >= 500 and flush(ch, buf): buf = []

if __name__ == "__main__": main()
