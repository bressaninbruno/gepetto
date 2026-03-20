import os
import json
from pathlib import Path
from datetime import datetime, timedelta

try:
    import requests
except Exception:
    requests = None

BASE_DIR = Path(__file__).parent
LOG_FILE = BASE_DIR / "conversation_log.json"
GUEST_FILE = BASE_DIR / "current_guest.json"
INCIDENTS_FILE = BASE_DIR / "incidents.json"
INSIGHT_FILE = BASE_DIR / "guest_insights.json"
STATE_FILE = BASE_DIR / "telegram_digest_state.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()


def read_json(path: Path, default):
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def write_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def should_send_telegram():
    return bool(requests and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram_message(message: str):
    if not should_send_telegram():
        return False, "Telegram não configurado"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        resp = requests.post(url, json=payload, timeout=12)
        return resp.ok, resp.text
    except Exception as e:
        return False, str(e)


def default_state():
    return {
        "last_sent_log_index": 0,
        "last_sent_at": ""
    }


def load_state():
    state = read_json(STATE_FILE, None)
    if state is None:
        state = default_state()
        write_json(STATE_FILE, state)
    for k, v in default_state().items():
        state.setdefault(k, v)
    return state


def save_state(state):
    write_json(STATE_FILE, state)


def split_message(text: str, max_len: int = 3500):
    parts = []
    current = ""

    for line in text.splitlines(True):
        if len(current) + len(line) > max_len:
            if current:
                parts.append(current)
            current = line
        else:
            current += line

    if current:
        parts.append(current)

    return parts


def build_digest():
    logs = read_json(LOG_FILE, [])
    guest = read_json(GUEST_FILE, {})
    insights = read_json(INSIGHT_FILE, {})
    incidents = read_json(INCIDENTS_FILE, [])
    state = load_state()

    start_index = int(state.get("last_sent_log_index", 0))
    if start_index < 0:
        start_index = 0

    new_logs = logs[start_index:]
    if not new_logs:
        return None, state

    nome = guest.get("nome", "").strip() or "Hóspede sem nome definido"
    grupo = guest.get("grupo", "").strip() or "-"
    checkout = guest.get("checkout", "").strip() or "-"

    first_ts = new_logs[0].get("timestamp", "-")
    last_ts = new_logs[-1].get("timestamp", "-")

    header = (
        "📦 CONVERSA ACUMULADA — GEPETTO\n\n"
        f"👤 Hóspede: {nome}\n"
        f"👥 Grupo: {grupo}\n"
        f"🕒 Checkout: {checkout}\n"
        f"📍 Período: {first_ts} → {last_ts}\n"
        f"💬 Interações no lote: {len(new_logs)}\n\n"
    )

    body_lines = []
    for i, item in enumerate(new_logs, start=1):
        ts = item.get("timestamp", "-")
        guest_name = item.get("guest", "") or nome
        intent = item.get("intent", "-")
        message = item.get("message", "")
        response = item.get("response", "")

        block = (
            f"{i}) [{ts}]\n"
            f"👤 Hóspede: {guest_name}\n"
            f"🎯 Intenção: {intent}\n"
            f"💬 Mensagem: {message}\n"
            f"🤖 Resposta: {response}\n"
        )
        body_lines.append(block)

    tail = "\n🔎 Leitura rápida:\n"

    top_insights = sorted(insights.items(), key=lambda x: x[1], reverse=True)[:5]
    if top_insights:
        tail += "• Interesses percebidos: " + ", ".join([f"{k} ({v})" for k, v in top_insights]) + "\n"
    else:
        tail += "• Interesses percebidos: sem dados relevantes\n"

    period_incidents = []
    first_dt = datetime.fromisoformat(first_ts) if first_ts != "-" else None
    if first_dt:
        for inc in incidents:
            try:
                ts = datetime.fromisoformat(inc.get("timestamp", ""))
                if ts >= first_dt:
                    period_incidents.append(inc)
            except Exception:
                pass

    if period_incidents:
        tail += "• Incidentes no período: " + ", ".join([f"{i.get('tipo', '-')}/{i.get('gravidade', '-')}" for i in period_incidents[-5:]]) + "\n"
    else:
        tail += "• Incidentes no período: nenhum\n"

    joined = header + "\n".join(body_lines) + tail
    return joined, state


def should_run_now(state):
    last_sent_at = state.get("last_sent_at", "")
    if not last_sent_at:
        return True

    try:
        dt = datetime.fromisoformat(last_sent_at)
    except Exception:
        return True

    return datetime.now() - dt >= timedelta(hours=4)


def main():
    state = load_state()
    if not should_run_now(state):
        print("Ainda não passou 4h desde o último digest.")
        return

    text, state = build_digest()
    if not text:
        print("Sem novas conversas para enviar.")
        return

    parts = split_message(text, max_len=3500)

    ok_all = True
    for idx, part in enumerate(parts, start=1):
        prefix = f"[Parte {idx}/{len(parts)}]\n\n" if len(parts) > 1 else ""
        ok, detail = send_telegram_message(prefix + part)
        if not ok:
            ok_all = False
            print("Falha ao enviar parte:", detail)
            break

    if ok_all:
        logs = read_json(LOG_FILE, [])
        state["last_sent_log_index"] = len(logs)
        state["last_sent_at"] = datetime.now().isoformat(timespec="seconds")
        save_state(state)
        print("Digest enviado com sucesso.")
    else:
        print("Digest não enviado por completo.")


if __name__ == "__main__":
    main()
