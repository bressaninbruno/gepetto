
from flask import Flask, request, send_from_directory, Response
import os
import json
import random
import re
import unicodedata
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo
import psycopg
from psycopg.rows import dict_row
from functools import wraps

try:
    import requests
except Exception:
    requests = None

app = Flask(__name__, static_folder="static", static_url_path="/static")

BASE_DIR = Path(__file__).parent
KNOWLEDGE_FILE = BASE_DIR / "knowledge_base.json"
GUEST_FILE = BASE_DIR / "current_guest.json"
MEMORY_FILE = BASE_DIR / "conversation_memory.json"
INCIDENTS_FILE = BASE_DIR / "incidents.json"
SESSION_FILE = BASE_DIR / "session_state.json"
LOG_FILE = BASE_DIR / "conversation_log.json"
INTENT_FILE = BASE_DIR / "intent_stats.json"
INSIGHT_FILE = BASE_DIR / "guest_insights.json"
USAGE_FILE = BASE_DIR / "usage_stats.json"

ADMIN_PIN = "2710"
ADMIN_UNLOCKED = False

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "").strip()

APP_TIMEZONE = ZoneInfo("America/Sao_Paulo")


def now_local():
    return datetime.now(APP_TIMEZONE)


def now_iso():
    return now_local().isoformat(timespec="seconds")


def today_local_str():
    return now_local().strftime("%Y-%m-%d")


def time_local_str():
    return now_local().strftime("%H:%M:%S")


def current_local_hour():
    return now_local().hour


def has_database():
    return bool(DATABASE_URL)


def get_db_connection():
    if not has_database():
        raise RuntimeError("DATABASE_URL não configurado")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def db_upsert_guest(data):
    if not has_database():
        return None

    try:
        nome = (data.get("nome") or "").strip()
        grupo = (data.get("grupo") or "").strip()
        perfil_hospede = (data.get("perfil_hospede") or "neutro").strip()
        idioma = (data.get("idioma") or "pt").strip()
        observacoes = (data.get("observacoes") or "").strip()
        preferencias = data.get("preferencias", {}) or {}

        checkin_date = parse_guest_date(data.get("checkin_date", ""))
        checkout_date = parse_guest_date(data.get("checkout_date", ""))
        checkout_time = parse_guest_time(data.get("checkout_time", ""))

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM guests
                    ORDER BY updated_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()

                if row:
                    guest_id = row["id"]
                    cur.execute("""
                        UPDATE guests
                        SET nome = %s,
                            grupo = %s,
                            checkin_date = %s,
                            checkout_date = %s,
                            checkout_time = %s,
                            idioma = %s,
                            observacoes = %s,
                            perfil_hospede = %s,
                            preferencias_json = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (
                        nome,
                        grupo,
                        checkin_date,
                        checkout_date,
                        checkout_time,
                        idioma,
                        observacoes,
                        perfil_hospede,
                        json.dumps(preferencias, ensure_ascii=False),
                        guest_id
                    ))
                else:
                    cur.execute("""
                        INSERT INTO guests (
                            nome, grupo, checkin_date, checkout_date, checkout_time,
                            idioma, observacoes, perfil_hospede, preferencias_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        nome,
                        grupo,
                        checkin_date,
                        checkout_date,
                        checkout_time,
                        idioma,
                        observacoes,
                        perfil_hospede,
                        json.dumps(preferencias, ensure_ascii=False)
                    ))
                    guest_id = cur.fetchone()["id"]

            conn.commit()

        return str(guest_id)
    except Exception as e:
        print("DB UPSERT GUEST ERROR:", e)
        return None
    
def db_get_latest_guest():
    if not has_database():
        return None

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT *
                    FROM guests
                    ORDER BY updated_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()

        if not row:
            return None

        return {
            "nome": row.get("nome") or "",
            "grupo": row.get("grupo") or "",
            "checkin_date": row.get("checkin_date").isoformat() if row.get("checkin_date") else "",
            "checkout_date": row.get("checkout_date").isoformat() if row.get("checkout_date") else "",
            "checkout_time": row.get("checkout_time").strftime("%H:%M") if row.get("checkout_time") else "11:00",
            "idioma": row.get("idioma") or "pt",
            "observacoes": row.get("observacoes") or "",
            "perfil_hospede": row.get("perfil_hospede") or "neutro",
            "preferencias": row.get("preferencias_json") or {}
        }
    except Exception as e:
        print("DB GET GUEST ERROR:", e)
        return None

def db_upsert_session_state(data):
    if not has_database():
        return None

    try:
        guest_id = get_or_create_db_guest()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM session_states
                    WHERE guest_id = %s
                    LIMIT 1
                """, (guest_id,))
                row = cur.fetchone()

                if row:
                    session_id = row["id"]
                    cur.execute("""
                        UPDATE session_states
                        SET last_topic = %s,
                            last_intent = %s,
                            last_followup_hint = %s,
                            last_recommendation_type = %s,
                            last_recommendation_name = %s,
                            last_entity_name = %s,
                            last_entity_category = %s,
                            pending_bruno_contact = %s,
                            pending_incident_context = %s,
                            last_incident_context = %s,
                            active_recommendation_type = %s,
                            active_recommendation_options_json = %s,
                            active_recommendation_index = %s,
                            active_recommendation_updated_at = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (
                        data.get("last_topic", ""),
                        data.get("last_intent", ""),
                        data.get("last_followup_hint", ""),
                        data.get("last_recommendation_type", ""),
                        data.get("last_recommendation_name", ""),
                        data.get("last_entity_name", ""),
                        data.get("last_entity_category", ""),
                        bool(data.get("pending_bruno_contact", False)),
                        bool(data.get("pending_incident_context", False)),
                        data.get("last_incident_context", ""),
                        data.get("active_recommendation_type", ""),
                        json.dumps(data.get("active_recommendation_options", []) or [], ensure_ascii=False),
                        int(data.get("active_recommendation_index", 0) or 0),
                        data.get("active_recommendation_updated_at") or None,
                        session_id
                    ))
                else:
                    cur.execute("""
                        INSERT INTO session_states (
                            guest_id,
                            last_topic,
                            last_intent,
                            last_followup_hint,
                            last_recommendation_type,
                            last_recommendation_name,
                            last_entity_name,
                            last_entity_category,
                            pending_bruno_contact,
                            pending_incident_context,
                            last_incident_context,
                            active_recommendation_type,
                            active_recommendation_options_json,
                            active_recommendation_index,
                            active_recommendation_updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        guest_id,
                        data.get("last_topic", ""),
                        data.get("last_intent", ""),
                        data.get("last_followup_hint", ""),
                        data.get("last_recommendation_type", ""),
                        data.get("last_recommendation_name", ""),
                        data.get("last_entity_name", ""),
                        data.get("last_entity_category", ""),
                        bool(data.get("pending_bruno_contact", False)),
                        bool(data.get("pending_incident_context", False)),
                        data.get("last_incident_context", ""),
                        data.get("active_recommendation_type", ""),
                        json.dumps(data.get("active_recommendation_options", []) or [], ensure_ascii=False),
                        int(data.get("active_recommendation_index", 0) or 0),
                        data.get("active_recommendation_updated_at") or None
                    ))
                    session_id = cur.fetchone()["id"]

            conn.commit()

        return str(session_id)
    except Exception as e:
        print("DB UPSERT SESSION ERROR:", e)
        return None
    
def db_get_latest_session_state():
    if not has_database():
        return None

    try:
        guest_id = get_or_create_db_guest()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT *
                    FROM session_states
                    WHERE guest_id = %s
                    LIMIT 1
                """, (guest_id,))
                row = cur.fetchone()

        if not row:
            return None

        active_options = row.get("active_recommendation_options_json")
        if not isinstance(active_options, list):
            active_options = []

        return {
            "last_topic": row.get("last_topic") or "",
            "last_intent": row.get("last_intent") or "",
            "last_followup_hint": row.get("last_followup_hint") or "",
            "last_recommendation_type": row.get("last_recommendation_type") or "",
            "last_recommendation_name": row.get("last_recommendation_name") or "",
            "last_entity_name": row.get("last_entity_name") or "",
            "last_entity_category": row.get("last_entity_category") or "",
            "pending_bruno_contact": bool(row.get("pending_bruno_contact", False)),
            "pending_incident_context": bool(row.get("pending_incident_context", False)),
            "last_incident_context": row.get("last_incident_context") or "",
            "active_recommendation_type": row.get("active_recommendation_type") or "",
            "active_recommendation_options": active_options,
            "active_recommendation_index": int(row.get("active_recommendation_index", 0) or 0),
            "active_recommendation_updated_at": row.get("active_recommendation_updated_at").isoformat() if row.get("active_recommendation_updated_at") else "",
            "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else ""
        }
    except Exception as e:
        print("DB GET SESSION ERROR:", e)
        return None
    
def db_insert_conversation_message(role, text, topic="", meta=None):
    if not has_database():
        return

    try:
        guest_id = get_or_create_db_guest()
        thread_id = get_or_create_active_thread(guest_id)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO conversation_messages (
                        thread_id, guest_id, role, text, topic, meta_json, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (
                    thread_id,
                    guest_id,
                    role,
                    text,
                    topic or "",
                    json.dumps(meta or {}, ensure_ascii=False)
                ))
            conn.commit()
    except Exception as e:
        print("DB MEMORY INSERT ERROR:", e)

def db_get_recent_conversation_messages(limit=120):
    if not has_database():
        return None

    try:
        guest_id = get_or_create_db_guest()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT role, text, topic, meta_json, timestamp
                    FROM conversation_messages
                    WHERE guest_id = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (guest_id, limit))
                rows = cur.fetchall()

        rows = list(reversed(rows))

        messages = []
        for row in rows:
            timestamp = row.get("timestamp")
            messages.append({
                "role": row.get("role") or "",
                "text": row.get("text") or "",
                "topic": row.get("topic") or "",
                "meta": row.get("meta_json") or {},
                "timestamp": timestamp.isoformat() if timestamp else ""
            })

        return {"messages": messages}
    except Exception as e:
        print("DB MEMORY GET ERROR:", e)
        return None

def get_or_create_db_guest():
    guest = load_guest()

    nome = (guest.get("nome") or "").strip()
    grupo = (guest.get("grupo") or "").strip()
    perfil_hospede = (guest.get("perfil_hospede") or "neutro").strip()
    idioma = (guest.get("idioma") or "pt").strip()
    observacoes = (guest.get("observacoes") or "").strip()
    preferencias = guest.get("preferencias", {}) or {}

    checkin_date = parse_guest_date(guest.get("checkin_date", ""))
    checkout_date = parse_guest_date(guest.get("checkout_date", ""))
    checkout_time = parse_guest_time(guest.get("checkout_time", ""))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM guests
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()

            if row:
                guest_id = row["id"]
                cur.execute("""
                    UPDATE guests
                    SET nome = %s,
                        grupo = %s,
                        checkin_date = %s,
                        checkout_date = %s,
                        checkout_time = %s,
                        idioma = %s,
                        observacoes = %s,
                        perfil_hospede = %s,
                        preferencias_json = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (
                    nome,
                    grupo,
                    checkin_date,
                    checkout_date,
                    checkout_time,
                    idioma,
                    observacoes,
                    perfil_hospede,
                    json.dumps(preferencias, ensure_ascii=False),
                    guest_id
                ))
            else:
                cur.execute("""
                    INSERT INTO guests (
                        nome, grupo, checkin_date, checkout_date, checkout_time,
                        idioma, observacoes, perfil_hospede, preferencias_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    nome,
                    grupo,
                    checkin_date,
                    checkout_date,
                    checkout_time,
                    idioma,
                    observacoes,
                    perfil_hospede,
                    json.dumps(preferencias, ensure_ascii=False)
                ))
                guest_id = cur.fetchone()["id"]

        conn.commit()

    return str(guest_id)


def get_or_create_active_thread(guest_id: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM conversation_threads
                WHERE guest_id = %s AND status = 'active'
                ORDER BY updated_at DESC
                LIMIT 1
            """, (guest_id,))
            row = cur.fetchone()

            if row:
                thread_id = row["id"]
                cur.execute("""
                    UPDATE conversation_threads
                    SET updated_at = NOW()
                    WHERE id = %s
                """, (thread_id,))
            else:
                cur.execute("""
                    INSERT INTO conversation_threads (guest_id, status)
                    VALUES (%s, 'active')
                    RETURNING id
                """, (guest_id,))
                thread_id = cur.fetchone()["id"]

        conn.commit()

    return str(thread_id)


def db_log_conversation(guest, message, intent, response):
    if not has_database():
        return

    try:
        guest_id = get_or_create_db_guest()
        thread_id = get_or_create_active_thread(guest_id)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO conversation_logs (
                        guest_id, thread_id, guest_nome, message, intent, response, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (
                    guest_id,
                    thread_id,
                    guest.get("nome", ""),
                    message,
                    intent or "",
                    response
                ))
            conn.commit()
    except Exception as e:
        print("DB LOG ERROR:", e)


def db_insert_intent_event(intent, topic=""):
    if not has_database():
        return

    try:
        guest_id = get_or_create_db_guest()
        thread_id = get_or_create_active_thread(guest_id)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO intent_events (guest_id, thread_id, intent, topic, timestamp)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (guest_id, thread_id, intent or "", topic or ""))
            conn.commit()
    except Exception as e:
        print("DB INTENT ERROR:", e)


def db_insert_guest_insight_event(insight_key, source_message=""):
    if not has_database():
        return

    try:
        guest_id = get_or_create_db_guest()
        thread_id = get_or_create_active_thread(guest_id)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO guest_insight_events (guest_id, thread_id, insight_key, source_message, timestamp)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (guest_id, thread_id, insight_key, source_message))
            conn.commit()
    except Exception as e:
        print("DB INSIGHT ERROR:", e)


def db_insert_usage_event(topic, used_followup=False, user_text="", assistant_text=""):
    if not has_database():
        return

    try:
        guest_id = get_or_create_db_guest()
        thread_id = get_or_create_active_thread(guest_id)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO usage_events (
                        guest_id, thread_id, topic, used_followup, user_text, assistant_text, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (
                    guest_id,
                    thread_id,
                    topic or "",
                    used_followup,
                    user_text,
                    assistant_text
                ))
            conn.commit()
    except Exception as e:
        print("DB USAGE ERROR:", e)


def db_append_incident(payload):
    if not has_database():
        return

    try:
        guest_id = get_or_create_db_guest()
        thread_id = get_or_create_active_thread(guest_id)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO incidents (
                        guest_id, thread_id, tipo, gravidade, mensagem, detalhe, status, grupo, checkout_label, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    guest_id,
                    thread_id,
                    payload.get("tipo", ""),
                    payload.get("gravidade", ""),
                    payload.get("mensagem", ""),
                    payload.get("detalhe", ""),
                    payload.get("status", ""),
                    payload.get("grupo", ""),
                    payload.get("checkout", ""),
                    payload.get("timestamp")
                ))
            conn.commit()
    except Exception as e:
        print("DB INCIDENT ERROR:", e)


def get_admin_token_from_request(req):
    """
    Extrai token admin da request.
    Ordem de tentativa:
    1) query param ?token=
    2) header X-Admin-Token
    3) header Authorization: Bearer <token>
    """
    token = (req.args.get("token") or "").strip()
    if token:
        return token

    token = (req.headers.get("X-Admin-Token") or "").strip()
    if token:
        return token

    auth_header = (req.headers.get("Authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        bearer_token = auth_header[7:].strip()
        if bearer_token:
            return bearer_token

    return ""


def is_admin_authorized(req):
    """
    Valida se a request está autorizada para acesso admin.
    """
    if not ADMIN_TOKEN:
        # Se o token não estiver configurado no ambiente,
        # o admin fica bloqueado por segurança.
        return False

    provided_token = get_admin_token_from_request(req)
    if not provided_token:
        return False

    return provided_token == ADMIN_TOKEN


def admin_forbidden_response():
    """
    Resposta padrão para acesso admin negado.
    """
    payload = {
        "ok": False,
        "error": "admin_forbidden",
        "message": "Acesso administrativo não autorizado."
    }
    return Response(
        json.dumps(payload, ensure_ascii=False, indent=2),
        status=403,
        mimetype="application/json"
    )


def admin_required(view_func):
    """
    Decorator para proteger rotas administrativas.
    """
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        if not is_admin_authorized(request):
            return admin_forbidden_response()
        return view_func(*args, **kwargs)

    return wrapped_view


# =========================
# JSON / ESTADO
# =========================

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


def knowledge():
    return read_json(KNOWLEDGE_FILE, {})


def default_guest():
    return {
        "nome": "",
        "grupo": "",
        "checkin_date": "",
        "checkout_date": "",
        "checkout_time": "11:00",
        "idioma": "pt",
        "observacoes": "",
        "perfil_hospede": "neutro",
        "preferencias": {
            "japones": 0,
            "doce": 0,
            "praia": 0,
            "mercado": 0,
            "surf": 0,
            "noite": 0,
            "restaurantes": 0
        }
    }


def load_guest():
    data = db_get_latest_guest()
    if data is None:
        data = read_json(GUEST_FILE, None)

    if data is None:
        data = default_guest()
        save_guest(data)

    base = default_guest()
    for key, value in base.items():
        if key not in data:
            data[key] = value

    if not isinstance(data.get("preferencias"), dict):
        data["preferencias"] = base["preferencias"].copy()

    for key, value in base["preferencias"].items():
        data["preferencias"].setdefault(key, value)

    legacy_checkout = (data.get("checkout") or "").strip()
    current_checkout_time = (data.get("checkout_time") or "").strip()

    if legacy_checkout and (not current_checkout_time or current_checkout_time == "11:00"):
        extracted_time = normalize_checkout_time_input(legacy_checkout)
        if extracted_time:
            data["checkout_time"] = extracted_time

    return data


def save_guest(data):
    write_json(GUEST_FILE, data)
    db_upsert_guest (data)


def default_memory():
    return {"messages": []}


def load_memory():
    data = db_get_recent_conversation_messages()
    if data is None:
        data = read_json(MEMORY_FILE, None)
    if data is None:
        data = default_memory()    
        save_memory(data)
    return data


def save_memory(data):
    write_json(MEMORY_FILE, data)


def append_memory(role, text, topic="", meta=None):
    memory = load_memory()
    memory["messages"].append({
        "role": role,
        "text": text,
        "topic": topic,
        "meta": meta or {},
        "timestamp": now_iso()
    })
    memory["messages"] = memory["messages"][-120:]
    save_memory(memory)
    db_insert_conversation_message(role, text, topic, meta or {})


def reset_memory():
    save_memory(default_memory())


def default_session():
    return {
        "last_topic": "",
        "last_intent": "",
        "last_followup_hint": "",
        "last_recommendation_type": "",
        "last_recommendation_name": "",
        "last_entity_name": "",
        "last_entity_category": "",
        "pending_bruno_contact": False,
        "pending_incident_context": False,
        "last_incident_context": "",
        "active_recommendation_type": "",
        "active_recommendation_options": [],
        "active_recommendation_index": 0,
        "active_recommendation_updated_at": "",
        "updated_at": ""
    }


def load_session():
    data = db_get_latest_session_state()
    if data is None:
        data = read_json(SESSION_FILE, None)

    if data is None:
        data = default_session()
        save_session(data)

    for k, v in default_session().items():
        data.setdefault(k, v)

    return data


def save_session(data):
    write_json(SESSION_FILE, data)
    db_upsert_session_state(data)


def reset_session():
    save_session(default_session())


def update_session(
    last_topic="",
    last_intent="",
    last_followup_hint="",
    last_recommendation_type="",
    last_recommendation_name=""
):
    sess = load_session()
    if last_topic:
        sess["last_topic"] = last_topic
    if last_intent:
        sess["last_intent"] = last_intent
    if last_followup_hint:
        sess["last_followup_hint"] = last_followup_hint
    if last_recommendation_type:
        sess["last_recommendation_type"] = last_recommendation_type
    if last_recommendation_name:
        sess["last_recommendation_name"] = last_recommendation_name
    sess["updated_at"] = now_iso()
    save_session(sess)


def set_bruno_pending(value: bool):
    sess = load_session()
    sess["pending_bruno_contact"] = value
    sess["updated_at"] = now_iso()
    save_session(sess)


def set_incident_pending(value: bool, context: str = ""):
    sess = load_session()
    sess["pending_incident_context"] = value
    if context:
        sess["last_incident_context"] = context
    elif not value:
        sess["last_incident_context"] = ""
    sess["updated_at"] = now_iso()
    save_session(sess)


def set_last_entity(name: str, category: str = ""):
    sess = load_session()
    sess["last_entity_name"] = name or ""
    sess["last_entity_category"] = category or ""
    sess["updated_at"] = now_iso()
    save_session(sess)


def set_active_recommendations(rec_type: str, options, current_name: str = ""):
    sess = load_session()

    clean_options = []
    seen = set()

    for opt in options or []:
        if not isinstance(opt, str):
            continue
        name = opt.strip()
        if not name:
            continue

        key = normalize_text(name)
        if key in seen:
            continue

        seen.add(key)
        clean_options.append(name)

    current_index = 0
    if current_name and clean_options:
        current_n = normalize_text(current_name)
        for i, name in enumerate(clean_options):
            if normalize_text(name) == current_n:
                current_index = i
                break

    sess["active_recommendation_type"] = rec_type or ""
    sess["active_recommendation_options"] = clean_options
    sess["active_recommendation_index"] = current_index
    sess["active_recommendation_updated_at"] = now_iso()
    sess["updated_at"] = now_iso()
    save_session(sess)


def clear_active_recommendations():
    sess = load_session()
    sess["active_recommendation_type"] = ""
    sess["active_recommendation_options"] = []
    sess["active_recommendation_index"] = 0
    sess["active_recommendation_updated_at"] = ""
    sess["updated_at"] = now_iso()
    save_session(sess)


def get_active_recommendations():
    sess = load_session()
    options = sess.get("active_recommendation_options", [])
    if not isinstance(options, list):
        options = []

    return {
        "type": sess.get("active_recommendation_type", ""),
        "options": options,
        "index": int(sess.get("active_recommendation_index", 0) or 0),
        "updated_at": sess.get("active_recommendation_updated_at", "")
    }


def get_current_active_recommendation(expected_type: str = ""):
    data = get_active_recommendations()

    if expected_type and normalize_text(data["type"]) != normalize_text(expected_type):
        return ""

    options = data["options"]
    index = data["index"]

    if not options:
        return ""

    if index < 0 or index >= len(options):
        return options[0]

    return options[index]


def get_next_active_recommendation(expected_type: str = "", advance: bool = False):
    data = get_active_recommendations()

    if expected_type and normalize_text(data["type"]) != normalize_text(expected_type):
        return ""

    options = data["options"]
    index = data["index"]

    if not options:
        return ""

    if len(options) == 1:
        return options[0]

    next_index = index + 1
    if next_index >= len(options):
        next_index = 0

    if advance:
        sess = load_session()
        sess["active_recommendation_index"] = next_index
        sess["updated_at"] = now_iso()
        save_session(sess)

    return options[next_index]


def set_current_active_recommendation_by_name(name: str, expected_type: str = ""):
    if not name:
        return

    data = get_active_recommendations()
    if expected_type and normalize_text(data["type"]) != normalize_text(expected_type):
        return

    options = data["options"]
    if not options:
        return

    target_n = normalize_text(name)
    for i, opt in enumerate(options):
        if normalize_text(opt) == target_n:
            sess = load_session()
            sess["active_recommendation_index"] = i
            sess["updated_at"] = now_iso()
            save_session(sess)
            return


def names_from_items(items):
    names = []
    for item in items or []:
        if isinstance(item, dict):
            name = (item.get("nome") or "").strip()
            if name:
                names.append(name)
    return names


def find_item_by_name(items, name: str):
    target_n = normalize_text(name)
    for item in items or []:
        if normalize_text(item.get("nome", "")) == target_n:
            return item
    return None


def build_passeio_active_options(primary_item, all_items, preferred_tipos=None, limit=5):
    preferred_tipos = preferred_tipos or []
    result = []
    seen = set()

    def add_item(item):
        if not isinstance(item, dict):
            return
        nome = (item.get("nome") or "").strip()
        if not nome:
            return
        key = normalize_text(nome)
        if key in seen:
            return
        seen.add(key)
        result.append(nome)

    add_item(primary_item)

    for tipo in preferred_tipos:
        for item in all_items or []:
            if normalize_text(item.get("tipo", "")) == normalize_text(tipo):
                add_item(item)
                if len(result) >= limit:
                    return result

    for item in all_items or []:
        add_item(item)
        if len(result) >= limit:
            return result

    return result


def load_incidents():
    return read_json(INCIDENTS_FILE, [])


def save_incidents(data):
    write_json(INCIDENTS_FILE, data)


def append_incident(payload):
    data = load_incidents()
    data.append(payload)
    data = data[-500:]
    save_incidents(data)
    db_append_incident(payload)


# =========================
# LOGS / STATS
# =========================

def log_conversation(guest, message, intent, response):
    logs = read_json(LOG_FILE, [])
    logs.append({
        "timestamp": now_iso(),
        "guest": guest.get("nome", ""),
        "message": message,
        "intent": intent,
        "response": response[:800]
    })
    logs = logs[-3000:]
    write_json(LOG_FILE, logs)
    db_log_conversation(guest, message, intent, response)


def update_intent_stats(intent):
    stats = read_json(INTENT_FILE, {})
    key = intent or "fallback"
    stats[key] = stats.get(key, 0) + 1
    write_json(INTENT_FILE, stats)
    db_insert_intent_event(intent, intent)


def update_guest_insights(message):
    insights = read_json(INSIGHT_FILE, {})
    msg = normalize_text(message)

    def inc(key):
        insights[key] = insights.get(key, 0) + 1
        db_insert_guest_insight_event(key, message)

    if has_any(msg, ["sushi", "japones", "japonês", "japonesa"]):
        inc("japones")
    if has_any(msg, ["mercado", "supermercado", "mercados", "supermercados", "compras", "mercado dia", "supermercado dia", "dia"]):
        inc("mercado")
    if has_any(msg, ["praia", "praias", "guarda-sol", "cadeira de praia"]):
        inc("praia")
    if has_any(msg, ["bar", "bares", "cerveja", "drink", "drinks", "noite"]):
        inc("noite")
    if has_any(msg, ["doce", "sobremesa", "chocolate", "kopenhagen", "cacau show"]):
        inc("doce")
    if has_any(msg, ["surf", "ondas", "surfar"]):
        inc("surf")
    if has_any(msg, ["restaurante", "restaurantes", "comer", "jantar", "almoco", "almoço", "pizza", "hamburguer", "hambúrguer", "happy hour", "kids"]):
        inc("restaurantes")

    write_json(INSIGHT_FILE, insights)


def update_usage_stats(user_text, assistant_text, topic, used_followup=False):
    stats = read_json(USAGE_FILE, {
        "total_messages": 0,
        "guest_messages": 0,
        "assistant_messages": 0,
        "fallback_count": 0,
        "successful_followups": 0,
        "por_dia": {}
    })

    hoje = today_local_str()
    agora = time_local_str()

    if hoje not in stats["por_dia"]:
        stats["por_dia"][hoje] = {
            "total_messages": 0,
            "guest_messages": 0,
            "assistant_messages": 0,
            "fallback_count": 0,
            "successful_followups": 0,
            "first_activity": agora,
            "last_activity": agora
        }

    stats["total_messages"] += 2
    stats["guest_messages"] += 1
    stats["assistant_messages"] += 1

    stats["por_dia"][hoje]["total_messages"] += 2
    stats["por_dia"][hoje]["guest_messages"] += 1
    stats["por_dia"][hoje]["assistant_messages"] += 1

    if topic == "fallback":
        stats["fallback_count"] += 1
        stats["por_dia"][hoje]["fallback_count"] += 1

    if used_followup:
        stats["successful_followups"] += 1
        stats["por_dia"][hoje]["successful_followups"] += 1

    if not stats["por_dia"][hoje].get("first_activity"):
        stats["por_dia"][hoje]["first_activity"] = agora
    stats["por_dia"][hoje]["last_activity"] = agora

    write_json(USAGE_FILE, stats)
    db_insert_usage_event(topic, used_followup, user_text, assistant_text)


# =========================
# HELPERS
# =========================

def json_response(payload: dict, status: int = 200):
    return Response(
        json.dumps(payload, ensure_ascii=False),
        status=status,
        content_type="application/json; charset=utf-8"
    )


def normalize_text(text: str) -> str:
    text = (text or "").lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text


def phrase_in_text(text: str, term: str) -> bool:
    text_n = normalize_text(text)
    term_n = normalize_text(term)

    if not term_n:
        return False

    if " " in term_n:
        pattern = r"(?<!\w)" + r"\s+".join(re.escape(p) for p in term_n.split()) + r"(?!\w)"
        return re.search(pattern, text_n) is not None

    pattern = r"(?<!\w)" + re.escape(term_n) + r"(?!\w)"
    return re.search(pattern, text_n) is not None


def has_any(text: str, terms) -> bool:
    return any(phrase_in_text(text, term) for term in terms)


def title_case_name(name: str) -> str:
    parts = [p.capitalize() for p in name.strip().split() if p.strip()]
    return " ".join(parts)


def normalize_group_value(value: str) -> str:
    v = normalize_text(value)
    if v in ["familia", "família"]:
        return "familia"
    if v == "casal":
        return "casal"
    if v in ["amigos", "amigo"]:
        return "amigos"
    return value.strip().lower()


def normalize_profile_value(value: str) -> str:
    v = normalize_text(value)

    mapping = {
        "casal": "casal",
        "familia_sem_criancas": "familia_sem_criancas",
        "familia sem criancas": "familia_sem_criancas",
        "familia sem crianças": "familia_sem_criancas",
        "familia_com_criancas": "familia_com_criancas",
        "familia com criancas": "familia_com_criancas",
        "familia com crianças": "familia_com_criancas",
        "amigos": "amigos",
        "grupo": "grupo",
        "neutro": "neutro"
    }

    return mapping.get(v, "neutro")


def get_guest_profile(guest):
    perfil = normalize_profile_value(guest.get("perfil_hospede", ""))

    if perfil != "neutro":
        return perfil

    grupo = normalize_group_value(guest.get("grupo", ""))

    if grupo == "casal":
        return "casal"
    if grupo == "amigos":
        return "amigos"
    if grupo == "familia":
        return "familia_com_criancas"

    return "neutro"


def restaurant_profile_priority_score(item, profile):
    profile_n = normalize_profile_value(profile)
    score = 0

    ideals = normalize_str_list(item.get("ideal_para", []))
    nome = normalize_text(item.get("nome", ""))
    tipo = normalize_text(item.get("tipo", ""))
    subtipo = normalize_text(item.get("subtipo", ""))

    if profile_n == "casal":
        if restaurant_matches_especial(item):
            score += 40
        if restaurant_matches_rooftop(item):
            score += 20
        if "casal" in ideals:
            score += 35
        if restaurant_has_any(item, ["romantico", "romântico", "atmosferico", "atmosférico", "sensorial", "lounge"]):
            score += 20
        if restaurant_matches_kids(item):
            score -= 15

    elif profile_n == "familia_sem_criancas":
        if "familia_sem_criancas" in ideals:
            score += 45
        if "familia" in ideals:
            score += 20
        if restaurant_has_any(item, ["conforto", "refeicao estruturada", "refeição estruturada", "tradicional", "variedade"]):
            score += 25
        if restaurant_matches_especial(item):
            score -= 10

    elif profile_n == "familia_com_criancas":
        if "familia_com_criancas" in ideals:
            score += 55
        if restaurant_matches_kids(item):
            score += 45
        if restaurant_has_any(item, ["variedade", "pizza", "hamburguer", "hambúrguer", "cardapio amplo", "cardápio amplo"]):
            score += 20
        if restaurant_matches_especial(item):
            score -= 20
        if restaurant_has_any(item, ["exotico", "exótico"]):
            score -= 10

    elif profile_n == "amigos":
        if "amigos" in ideals:
            score += 45
        if restaurant_matches_happy_hour(item):
            score += 40
        if restaurant_matches_burger(item):
            score += 20
        if restaurant_has_any(item, ["orla", "rooftop", "drinks", "entretenimento", "boliche", "conversar"]):
            score += 20
        if restaurant_matches_especial(item):
            score -= 25

    elif profile_n == "grupo":
        if "grupo" in ideals:
            score += 55
        if restaurant_matches_happy_hour(item):
            score += 35
        if restaurant_matches_burger(item):
            score += 20
        if restaurant_matches_pizza(item):
            score += 20
        if restaurant_matches_japanese(item):
            score += 10
        if restaurant_has_any(item, ["entretenimento", "conversar", "happy hour", "hamburguer", "hambúrguer", "pizzaria"]):
            score += 20
        if restaurant_has_any(item, ["romantico", "romântico"]):
            score -= 35

    return score


def sort_restaurants_for_profile(items, profile):
    profile_n = normalize_profile_value(profile)

    return sorted(
        items,
        key=lambda item: (
            -restaurant_profile_priority_score(item, profile_n),
            -restaurant_mode_priority_score(item, "happy hour") if profile_n in ["amigos", "grupo"] else 0,
            distance_sort_key(item.get("distancia", ""))
        )
    )


def build_profile_opening_line(profile):
    profile_n = normalize_profile_value(profile)

    if profile_n == "casal":
        return "Pensando no perfil de **casal**, eu tenderia a começar por opções com mais clima, praticidade e experiência 😊"
    if profile_n == "familia_sem_criancas":
        return "Pensando em **família sem crianças**, eu tenderia a começar por lugares mais confortáveis e com refeição mais estruturada 😊"
    if profile_n == "familia_com_criancas":
        return "Pensando em **família com crianças**, eu tenderia a começar por opções mais práticas, confortáveis e com apelo melhor para crianças 😊"
    if profile_n == "amigos":
        return "Pensando em **amigos**, eu tenderia a começar por opções com mais clima de happy hour, conversa e praticidade 😊"
    if profile_n == "grupo":
        return "Pensando em **grupo**, eu tenderia a começar por opções mais sociais, leves e que não prendam tanto no apartamento 😊"

    return "Eu posso começar pelas opções que tendem a funcionar melhor para este tipo de estadia 😊"


def get_recent_messages(limit=10):
    return load_memory().get("messages", [])[-limit:]


def get_last_topic():
    sess = load_session()
    topic = sess.get("last_topic", "")
    if topic and topic not in ["fallback", "admin", "saudacao"]:
        return topic

    for item in reversed(get_recent_messages(30)):
        topic = item.get("topic", "")
        if topic and topic not in ["fallback", "admin", "saudacao"]:
            return topic
    return ""


def current_time_label():
    bucket = get_time_of_day_bucket()

    if bucket in ["madrugada", "manha"]:
        return "manhã"
    if bucket in ["meio_dia", "tarde"]:
        return "tarde"
    return "noite"


def normalize_admin_date_input(value: str) -> str:
    value = (value or "").strip()

    if not value:
        return ""

    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except Exception:
        pass

    try:
        return datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        pass

    try:
        partial = datetime.strptime(value, "%d/%m")
        year = now_local().year
        return date(year, partial.month, partial.day).strftime("%Y-%m-%d")
    except Exception:
        pass

    return ""


def normalize_checkout_time_input(value: str) -> str:
    value = normalize_text(value)

    if not value:
        return ""

    patterns = [
        r"^(\d{1,2}):(\d{2})$",
        r"^(\d{1,2})h(\d{2})$",
        r"^(\d{1,2})h$",
        r"^(\d{1,2})$"
    ]

    for pattern in patterns:
        match = re.match(pattern, value)
        if not match:
            continue

        hour = int(match.group(1))
        minute = 0

        if len(match.groups()) >= 2 and match.group(2):
            minute = int(match.group(2))

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return f"{hour:02d}:{minute:02d}"

    return ""


def parse_guest_date(value: str):
    value = (value or "").strip()
    if not value:
        return None

    normalized = normalize_admin_date_input(value)
    if not normalized:
        return None

    try:
        return datetime.strptime(normalized, "%Y-%m-%d").date()
    except Exception:
        return None


def parse_guest_time(value: str):
    value = (value or "").strip()
    if not value:
        return None

    normalized = normalize_checkout_time_input(value)
    if not normalized:
        return None

    try:
        return datetime.strptime(normalized, "%H:%M").time()
    except Exception:
        return None


def get_time_of_day_bucket(hour=None):
    if hour is None:
        hour = current_local_hour()

    if 0 <= hour <= 4:
        return "madrugada"
    if 5 <= hour <= 10:
        return "manha"
    if 11 <= hour <= 13:
        return "meio_dia"
    if 14 <= hour <= 17:
        return "tarde"
    return "noite"


def extract_temporal_signals(text_raw):
    text_n = normalize_text(text_raw)

    return {
        "references_now": has_any(text_n, [
            "agora", "nesse momento", "neste momento", "por agora",
            "compensa agora", "posso ir agora"
        ]),
        "references_today": has_any(text_n, [
            "hoje", "ainda hoje"
        ]),
        "references_later_today": has_any(text_n, [
            "mais tarde", "ainda hoje", "depois"
        ]),
        "references_soon": has_any(text_n, [
            "daqui a pouco", "ja ja", "já já", "logo mais",
            "ja abre", "já abre", "abre daqui a pouco",
            "ja comeca", "já começa", "comeca daqui a pouco", "começa daqui a pouco",
            "ja inicia", "já inicia", "inicia daqui a pouco"
        ]),
        "references_tonight": has_any(text_n, [
            "essa noite", "esta noite", "hoje a noite", "hoje à noite", "a noite", "à noite"
        ]),
        "references_this_afternoon": has_any(text_n, [
            "hoje a tarde", "hoje à tarde", "essa tarde", "esta tarde"
        ]),
        "references_tomorrow": has_any(text_n, [
            "amanha", "amanhã"
        ]),
        "references_tomorrow_morning": has_any(text_n, [
            "amanha cedo", "amanhã cedo", "amanha de manha", "amanhã de manhã",
            "cedo"
        ])
    }


def get_stay_context(guest, text_raw=""):
    now = now_local()
    today = now.date()
    hour = now.hour
    part_of_day = get_time_of_day_bucket(hour)
    temporal_refs = extract_temporal_signals(text_raw)

    checkin_date = parse_guest_date(guest.get("checkin_date", ""))
    checkout_date = parse_guest_date(guest.get("checkout_date", ""))
    checkout_time = parse_guest_time(guest.get("checkout_time", ""))

    days_since_checkin = None
    days_to_checkout = None

    is_arrival_day = False
    is_first_night = False
    is_checkout_day = False
    is_last_night = False

    stay_phase = "unknown"

    if checkin_date:
        days_since_checkin = (today - checkin_date).days
        is_arrival_day = days_since_checkin == 0

    if checkout_date:
        days_to_checkout = (checkout_date - today).days
        is_checkout_day = days_to_checkout == 0
        is_last_night = days_to_checkout == 1

    if is_arrival_day and hour >= 18:
        is_first_night = True

    if is_checkout_day:
        stay_phase = "dia_checkout"
    elif is_last_night:
        stay_phase = "vespera_saida"
    elif is_first_night:
        stay_phase = "primeira_noite"
    elif is_arrival_day:
        stay_phase = "chegada"
    elif days_since_checkin is not None and days_since_checkin >= 1:
        stay_phase = "meio_estadia"

    return {
        "now": now,
        "today_iso": today.isoformat(),
        "hour": hour,
        "part_of_day": part_of_day,
        "temporal_refs": temporal_refs,
        "checkin_date": checkin_date.isoformat() if checkin_date else "",
        "checkout_date": checkout_date.isoformat() if checkout_date else "",
        "checkout_time": checkout_time.strftime("%H:%M") if checkout_time else "",
        "days_since_checkin": days_since_checkin,
        "days_to_checkout": days_to_checkout,
        "is_arrival_day": is_arrival_day,
        "is_first_night": is_first_night,
        "is_checkout_day": is_checkout_day,
        "is_last_night": is_last_night,
        "stay_phase": stay_phase
    }


def get_praia_service_status(context=None):
    context = context or get_stay_context(load_guest())
    hour = context.get("hour", current_local_hour())

    if hour < 8:
        return "pre_open"
    if 8 <= hour < 9:
        return "opening_soon"
    if 9 <= hour < 17:
        return "active"
    if 17 <= hour < 19:
        return "just_closed"
    return "closed_night"


def get_checkout_day_window(context=None, guest=None):
    if context is None:
        guest = guest or load_guest()
        context = get_stay_context(guest)

    checkout_time_str = (context.get("checkout_time") or "").strip()
    if not checkout_time_str:
        return ""

    try:
        checkout_hour = int(checkout_time_str.split(":", 1)[0])
    except Exception:
        return ""

    if checkout_hour <= 13:
        return "corrido"
    if checkout_hour <= 17:
        return "intermediario"
    return "folgado"


def get_stay_restaurant_moment(context=None, guest=None):
    if context is None:
        guest = guest or load_guest()
        context = get_stay_context(guest)

    phase = context.get("stay_phase", "unknown")

    if phase == "primeira_noite":
        return "primeira_noite"
    if phase == "chegada":
        return "chegada"
    if phase == "vespera_saida":
        return "vespera_saida"
    if phase == "dia_checkout":
        window = get_checkout_day_window(context)
        if window == "corrido":
            return "checkout_corrido"
        if window == "folgado":
            return "checkout_folgado"
        return "checkout_intermediario"
    if phase == "meio_estadia":
        return "meio_estadia"

    return "momento_neutro"


def restaurant_moment_priority_score(item, context, mode=""):
    moment = get_stay_restaurant_moment(context)
    score = 0
    mode_n = normalize_text(mode)
    profile_n = normalize_text(context.get("guest_profile", ""))

    if normalize_text(item.get("tipo_de_saida", "")) == "espontanea":
        score += 8
    if normalize_text(item.get("friccao_logistica", "")) == "baixa":
        score += 10
    elif normalize_text(item.get("friccao_logistica", "")) == "media":
        score += 3
    if item.get("vale_pos_praia") is True:
        score += 5

    if moment in ["chegada", "primeira_noite"]:
        if restaurant_matches_tradicional(item):
            score += 18
        if restaurant_matches_pizza(item):
            score += 16
        if restaurant_matches_burger(item):
            score += 8
        if restaurant_matches_happy_hour(item) and mode_n != "happy hour":
            score -= 10
        if normalize_text(item.get("tipo_de_saida", "")) == "planejada":
            score -= 18
        if restaurant_matches_especial(item) and mode_n not in ["especial", "vista"]:
            score -= 10

    elif moment == "meio_estadia":
        if restaurant_matches_happy_hour(item):
            score += 12
        if restaurant_matches_especial(item):
            score += 8
        if normalize_text(item.get("tipo_de_saida", "")) == "planejada":
            score += 4

    elif moment == "vespera_saida":
        if restaurant_matches_tradicional(item):
            score += 12
        if restaurant_matches_pizza(item):
            score += 10
        if restaurant_matches_happy_hour(item):
            score += 8
        if normalize_text(item.get("friccao_logistica", "")) == "media":
            score -= 3

    elif moment == "checkout_corrido":
        if restaurant_matches_burger(item):
            score += 10
        if restaurant_matches_pizza(item):
            score += 10
        if restaurant_matches_happy_hour(item) and mode_n != "happy hour":
            score -= 22
        if normalize_text(item.get("tipo_de_saida", "")) == "planejada":
            score -= 24
        if normalize_text(item.get("friccao_logistica", "")) == "media":
            score -= 10
        if restaurant_matches_especial(item) and mode_n not in ["especial", "vista"]:
            score -= 16

    elif moment == "checkout_intermediario":
        if restaurant_matches_tradicional(item):
            score += 12
        if restaurant_matches_pizza(item):
            score += 8
        if restaurant_matches_happy_hour(item) and mode_n == "happy hour":
            score += 10

    elif moment == "checkout_folgado":
        if restaurant_matches_happy_hour(item):
            score += 18
        if restaurant_matches_rooftop(item):
            score += 10
        if restaurant_matches_especial(item):
            score += 10

    if mode_n == "happy hour":
        if restaurant_matches_happy_hour(item):
            score += 50
        if restaurant_matches_rooftop(item):
            score += 18
        if restaurant_matches_burger(item) and not restaurant_matches_happy_hour(item):
            score -= 10
        if moment == "checkout_corrido" and normalize_text(item.get("tipo_de_saida", "")) == "planejada":
            score -= 8

    if mode_n in ["", "todos"] and profile_n in ["", "neutro"]:
        item_name_n = normalize_text(item.get("nome", ""))
        item_tipo_n = normalize_text(item.get("tipo", ""))

        if item_tipo_n == "rapido":
            score -= 18
        if restaurant_matches_burger(item):
            score -= 26
        if item_name_n in ["burger king", "mcdonald's enseada", "mcdonalds enseada"]:
            score -= 22
        if restaurant_matches_tradicional(item):
            score += 22
        if restaurant_matches_pizza(item):
            score += 16
        if restaurant_matches_especial(item):
            score += 16
        if restaurant_matches_happy_hour(item):
            score += 10
        if restaurant_matches_rooftop(item):
            score += 8

    return score


def build_checkout_concierge_line(guest, context=None):
    context = context or get_stay_context(guest)
    window = get_checkout_day_window(context, guest)
    checkout_time = (context.get("checkout_time") or guest.get("checkout_time") or "11:00").strip() or "11:00"
    checkout_date = parse_guest_date(guest.get("checkout_date", ""))

    if checkout_date:
        label = f"**{checkout_time}** do dia **{checkout_date.strftime('%d/%m')}**"
    else:
        label = f"**{checkout_time}**"

    if window == "corrido":
        return (
            f"O check-out está previsto para {label}. Como esse horário costuma deixar o dia mais corrido, eu tenderia a pensar o restante da programação com mais leveza 😊"
        )
    if window == "intermediario":
        return (
            f"O check-out está previsto para {label}. Ainda dá para organizar o dia com tranquilidade, só valendo manter um ritmo mais redondo 😊"
        )
    if window == "folgado":
        return (
            f"O check-out está previsto para {label}. Como vocês ainda têm uma janela mais folgada, dá para aproveitar o dia com bem menos pressa 😊"
        )

    return f"O check-out está previsto para {label} 😊"


def get_praia_temporal_followup_reply(guest, text_raw):
    context = get_stay_context(guest, text_raw)
    refs = context.get("temporal_refs", {})
    status = get_praia_service_status(context)
    text_n = normalize_text(text_raw)

    if has_any(text_n, [
        "daqui a pouco ja abre", "daqui a pouco já abre",
        "ja abre", "já abre", "abre daqui a pouco",
        "daqui a pouco ja comeca", "daqui a pouco já começa",
        "ja comeca", "já começa", "comeca daqui a pouco", "começa daqui a pouco",
        "daqui a pouco ja inicia", "daqui a pouco já inicia",
        "ja inicia", "já inicia", "inicia daqui a pouco",
        "comeca", "começa", "inicia", "cedo"
    ]):
        if status == "pre_open":
            return (
                "Sim 😊\n\n"
                "O **serviço de praia começa às 9h**."
            )
        if status == "opening_soon":
            return (
                "Sim 😊\n\n"
                "O **serviço de praia começa às 9h**, então já está perto de iniciar."
            )
        if status == "active":
            return (
                "Agora o **serviço de praia já está funcionando** 😊"
            )
        return (
            "Para hoje, o **serviço de praia já encerrou** 😊"
        )

    if refs.get("references_tonight"):
        return (
            "Hoje à noite a praia pode ser boa para caminhar, passar um pouco pela orla "
            "ou curtir o visual 😊\n\n"
            "Mas o **serviço de praia já não funciona nesse horário**."
        )

    if refs.get("references_this_afternoon"):
        if status in ["pre_open", "opening_soon", "active"]:
            return (
                "Hoje à tarde ainda pode valer a pena sim 😊\n\n"
                "O **serviço de praia funciona das 9h às 17h**."
            )
        return (
            "Hoje à tarde vale considerar que o **serviço de praia vai até as 17h**.\n\n"
            "Depois disso, a praia segue mais como passeio, caminhada ou fim de tarde."
        )

    if refs.get("references_tomorrow_morning"):
        return (
            "Amanhã cedo já pode ser uma boa para começar o dia por lá 😊\n\n"
            "Só vale lembrar que o **serviço de praia começa às 9h**."
        )

    if refs.get("references_tomorrow"):
        return (
            "Amanhã a praia pode ser uma boa sim 😊\n\n"
            "O **serviço de praia funciona das 9h às 17h**."
        )

    if refs.get("references_later_today") or refs.get("references_soon"):
        if status in ["pre_open", "opening_soon", "active"]:
            return (
                "Mais tarde ainda deve dar praia sim 😊\n\n"
                "O **serviço funciona das 9h às 17h**."
            )
        return (
            "Para **mais tarde hoje**, vale considerar que o **serviço de praia funciona até as 17h**.\n\n"
            "Depois disso, a praia segue mais como passeio ou caminhada."
        )

    if refs.get("references_now") or refs.get("references_today"):
        if status == "pre_open":
            return (
                "Agora você até pode ir à praia sim 😊\n\n"
                "Mas o **serviço de praia ainda não começou** — ele funciona a partir das **9h**."
            )
        if status == "opening_soon":
            return (
                "Sim 😊\n\n"
                "O **serviço de praia começa às 9h**, então já está perto de iniciar."
            )
        if status == "active":
            return (
                "Agora compensa sim 😊\n\n"
                "O **serviço de praia está em funcionamento**."
            )
        if status == "just_closed":
            return (
                "Agora, para usar o **serviço de praia**, já não compensa tanto porque ele **encerrou às 17h**.\n\n"
                "Ainda assim, a praia pode valer para caminhar, relaxar um pouco ou curtir o fim do dia."
            )
        return (
            "Agora à noite a praia pode ser boa para passeio visual ou caminhada 😊\n\n"
            "Mas o **serviço de praia já encerrou**."
        )

    if has_any(text_n, [
        "agora", "mais tarde", "ainda hoje",
        "amanha", "amanhã", "amanha cedo", "amanhã cedo",
        "essa noite", "esta noite",
        "daqui a pouco", "logo mais",
        "posso ir agora", "compensa agora",
        "ja abre", "já abre", "abre",
        "ja comeca", "já começa", "comeca", "começa",
        "ja inicia", "já inicia", "inicia",
        "cedo"
    ]):
        if status == "pre_open":
            return "O **serviço de praia ainda não começou** — ele funciona a partir das **9h** 😊"
        if status == "opening_soon":
            return "O **serviço de praia começa às 9h** 😊"
        if status == "active":
            return "Agora o **serviço de praia está em funcionamento** 😊"
        if status == "just_closed":
            return "Para hoje, o **serviço de praia já encerrou às 17h** 😊"
        return "Neste horário a praia pode até ser boa para passeio, mas o **serviço de praia já não funciona** 😊"

    return ""


def guest_checkout_label(guest):
    checkout_time = (guest.get("checkout_time") or "").strip()
    checkout_date = parse_guest_date(guest.get("checkout_date", ""))

    if checkout_date and checkout_time:
        return f"{checkout_time} de {checkout_date.strftime('%d/%m')}"
    if checkout_time:
        return checkout_time

    legacy_checkout = (guest.get("checkout") or "").strip()
    if legacy_checkout:
        return legacy_checkout

    return "-"


def guest_group_label(guest):
    grupo = normalize_group_value(guest.get("grupo", ""))
    if grupo == "familia":
        return "família"
    if grupo == "amigos":
        return "amigos"
    if grupo == "casal":
        return "casal"
    return ""


def guest_language(guest):
    idioma = normalize_text(guest.get("idioma", "pt"))
    if idioma.startswith("en"):
        return "en"
    return "pt"


def saudacao_personalizada(guest):
    nome = (guest.get("nome") or "").strip()
    grupo = normalize_group_value(guest.get("grupo", ""))

    if not nome:
        return "Hello 😊" if guest_language(guest) == "en" else "Olá 😊"

    if guest_language(guest) == "en":
        if grupo == "familia":
            return f"Hello {nome} and family 😊"
        if grupo == "amigos":
            return f"Hello {nome} and friends 😄"
        return f"Hello {nome} 😊"

    if grupo == "familia":
        return f"Olá {nome} e família 😊"
    if grupo == "amigos":
        return f"Olá {nome} e amigos 😄"
    if grupo == "casal":
        return f"Olá {nome} 😊"

    return f"Olá {nome} 😊"


def sort_restaurants_for_moment(items, profile, context, mode=""):
    context = dict(context or {})
    context["guest_profile"] = normalize_profile_value(profile)

    return sorted(
        unique_restaurants(items),
        key=lambda item: (
            -restaurant_profile_priority_score(item, profile),
            -restaurant_moment_priority_score(item, context, mode),
            -restaurant_mode_priority_score(item, mode) if normalize_text(mode) not in ["", "todos"] else 0,
            distance_sort_key(item.get("distancia", ""))
        )
    )


def build_restaurant_concierge_intro(profile, context, mode=""):
    profile_n = normalize_profile_value(profile)
    moment = get_stay_restaurant_moment(context)
    mode_n = normalize_text(mode)

    if mode_n == "happy hour":
        if moment == "checkout_corrido":
            return "Mesmo com a saída pedindo um pouco mais de leveza hoje, ainda dá para pensar em um happy hour que não complique o ritmo do dia 😊"
        if moment == "checkout_folgado":
            return "Como vocês ainda têm uma boa margem no dia, dá para olhar happy hour com mais tranquilidade 😊"
        return "Se a ideia for happy hour, eu tenderia a abrir opções com clima gostoso e boa fluidez para este momento 😊"

    if moment == "primeira_noite":
        if profile_n == "casal":
            return "Como é a primeira noite, eu tenderia a começar por algo gostoso, acolhedor e com um pouco mais de clima 😊"
        if profile_n == "familia_com_criancas":
            return "Como é a primeira noite, eu tenderia a começar por algo mais confortável e fácil de encaixar para todo mundo 😊"
        return "Como é a primeira noite, eu tenderia a começar por algo gostoso, acolhedor e sem muita fricção 😊"

    if moment == "chegada":
        return "Como vocês ainda estão entrando no ritmo da estadia, eu tenderia a começar por algo simples de encaixar e agradável 😊"

    if moment == "meio_estadia":
        return build_profile_opening_line(profile_n)

    if moment == "vespera_saida":
        return "Como vocês já entram na reta final da hospedagem, eu tenderia a olhar algo gostoso, mas ainda confortável de encaixar 😊"

    if moment == "checkout_corrido":
        return "Como hoje tende a ser um pouco mais corrido, eu olharia primeiro para algo gostoso e mais simples de encaixar 😊"

    if moment == "checkout_intermediario":
        return "Como hoje ainda permite aproveitar com alguma folga, eu tenderia a buscar algo agradável e sem pesar a logística 😊"

    if moment == "checkout_folgado":
        return "Como vocês ainda têm uma boa margem para aproveitar o dia, dá para pensar em algo com mais calma 😊"

    return build_profile_opening_line(profile_n)


def get_restaurant_kids_highlight(item):
    if not isinstance(item, dict):
        return ""

    blob = restaurant_search_blob(item)

    if phrase_in_text(blob, "area kids") or phrase_in_text(blob, "área kids"):
        return "Esse lugar conta com **área kids**, o que costuma ajudar bastante para famílias com crianças 😊"
    if phrase_in_text(blob, "espaco kids") or phrase_in_text(blob, "espaço kids"):
        return "Esse lugar conta com **espaço kids**, o que costuma funcionar muito bem para famílias com crianças 😊"
    if phrase_in_text(blob, "espaco criança") or phrase_in_text(blob, "espaço criança"):
        return "Esse lugar conta com **espaço para crianças**, o que costuma deixar a experiência mais confortável para o grupo 😊"
    if restaurant_matches_kids(item):
        return "Esse lugar costuma funcionar bem para **famílias com crianças** 😊"

    return ""


def pick_place_followup_close(index=None):
    options = [
        "Se quiser, posso ajudar mais na experiência de vocês por aqui. É só me perguntar sobre esse lugar 😊",
        "Se quiser, também posso te passar mais detalhes sobre esse lugar e te ajudar a entender se ele combina com o momento de vocês 😊",
        "Caso queira, posso continuar te ajudando por aqui com mais informações sobre esse lugar 😊"
    ]

    if index is None:
        return random.choice(options)

    return options[index % len(options)]


def get_roteiro_style_label(text_raw, guest=None):
    text_n = normalize_text(text_raw)
    guest = guest or load_guest()

    if has_any(text_n, ["casal", "romantico", "romântico", "a dois"]):
        return "casal"
    if has_any(text_n, ["familia", "família", "crianca", "criança", "criancas", "crianças"]):
        return "familia"
    if has_any(text_n, ["grupo", "amigos", "galera", "pessoal"]):
        return "grupo"

    perfil = get_guest_profile(guest)
    if perfil == "casal":
        return "casal"
    if perfil in ["familia_sem_criancas", "familia_com_criancas"]:
        return "familia"
    if perfil in ["grupo", "amigos"]:
        return "grupo"

    return ""

def observacao_especial(guest):
    obs = normalize_text(guest.get("observacoes", ""))

    if has_any(obs, ["aniversario", "aniversário"]):
        return "E feliz aniversário!! 🎉✨ Espero que você tenha um dia incrível por aqui!\n\n"
    if has_any(obs, ["lua de mel"]):
        return "Que especial receber vocês em lua de mel ✨ Espero que aproveitem muito!\n\n"
    if has_any(obs, ["natal"]):
        return "E desejo um ótimo Natal para vocês 🎄✨\n\n"
    if has_any(obs, ["ano novo", "reveillon", "réveillon"]):
        return "Espero que vocês tenham uma virada incrível por aqui ✨🎆\n\n"

    if has_any(obs, ["pascoa", "páscoa"]):
        return "E desejo uma ótima Páscoa para vocês ✨🐣\n\n"

    return ""


def top_guest_preference(guest):
    prefs = guest.get("preferencias", {})
    if not isinstance(prefs, dict) or not prefs:
        return ""

    top_key = max(prefs, key=lambda k: prefs.get(k, 0))
    if prefs.get(top_key, 0) <= 0:
        return ""
    return top_key


def update_guest_preferences(text_raw):
    guest = load_guest()
    prefs = guest.get("preferencias", default_guest()["preferencias"])
    text_n = normalize_text(text_raw)

    def inc(key):
        prefs[key] = prefs.get(key, 0) + 1

    if has_any(text_n, ["sushi", "japones", "japonês", "japonesa"]):
        inc("japones")
        inc("restaurantes")
    if has_any(text_n, ["doce", "sobremesa", "chocolate", "kopenhagen", "cacau show"]):
        inc("doce")
    if has_any(text_n, ["praia", "praias", "guarda-sol", "servico de praia", "serviço de praia"]):
        inc("praia")
    if has_any(text_n, ["mercado", "mercados", "supermercado", "supermercados", "compras", "mercado dia", "supermercado dia", "dia"]):
        inc("mercado")
    if has_any(text_n, ["surf", "ondas", "surfar"]):
        inc("surf")
    if has_any(text_n, ["bar", "bares", "drink", "drinks", "cerveja", "noite", "happy hour"]):
        inc("noite")
    if has_any(text_n, ["restaurante", "restaurantes", "comer", "jantar", "almoco", "almoço", "pizza", "hamburguer", "hambúrguer", "kids"]):
        inc("restaurantes")

    guest["preferencias"] = prefs
    save_guest(guest)
    return guest


def should_send_telegram():
    return bool(requests and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram_message(message):
    if not should_send_telegram():
        return False, "Telegram não configurado"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    try:
        resp = requests.post(url, json=payload, timeout=8)
        return resp.ok, resp.text
    except Exception as e:
        return False, str(e)


def send_telegram_log(title, lines):
    if not should_send_telegram():
        return False, "Telegram não configurado"
    text = f"📋 {title}\n\n" + "\n".join(lines)
    return send_telegram_message(text)


def notify_conversation_to_telegram(guest, message, intent, response):
    nome = guest.get("nome", "").strip() or "Hóspede sem nome definido"
    grupo = guest.get("grupo", "").strip() or "-"
    checkout = guest_checkout_label(guest)
    agora = now_iso()

    lines = [
        f"👤 Hóspede: {nome}",
        f"👥 Grupo: {grupo}",
        f"🕒 Checkout: {checkout}",
        f"🎯 Intenção: {intent or '-'}",
        f"⏰ Horário: {agora}",
        "",
        "💬 Mensagem:",
        message[:700],
        "",
        "🤖 Resposta:",
        response[:900]
    ]
    return send_telegram_log("NOVA INTERAÇÃO — GEPETTO", lines)


def get_knowledge_list(key):
    value = knowledge().get(key, [])
    return value if isinstance(value, list) else []


def find_item_by_type(items, tipo):
    tipo_n = normalize_text(tipo)
    for item in items:
        if normalize_text(item.get("tipo", "")) == tipo_n:
            return item
    return None


def get_restaurants_data():
    return get_knowledge_list("restaurantes")


def normalize_str_list(values):
    if not isinstance(values, list):
        return []
    return [normalize_text(v) for v in values if isinstance(v, str)]


def restaurant_search_blob(item):
    parts = [
        item.get("nome", ""),
        item.get("tipo", ""),
        item.get("subtipo", ""),
        item.get("perfil", ""),
        item.get("observacao", ""),
        item.get("endereco", ""),
        item.get("faixa_preco", ""),
        item.get("distancia", "")
    ]

    parts.extend(item.get("ideal_para", []) if isinstance(item.get("ideal_para", []), list) else [])
    parts.extend(item.get("melhor_momento", []) if isinstance(item.get("melhor_momento", []), list) else [])

    hh = item.get("happy_hour", {})
    if isinstance(hh, dict):
        parts.append("happy hour")
        parts.append(hh.get("observacao", ""))
        parts.append(hh.get("horario", ""))

    return normalize_text(" | ".join([str(p) for p in parts if p]))


def restaurant_has_any(item, terms):
    blob = restaurant_search_blob(item)
    return any(phrase_in_text(blob, term) for term in terms)


def restaurant_matches_kids(item):
    return (
        restaurant_has_any(item, [
            "crianca", "criança", "criancas", "crianças",
            "kids", "espaco kids", "espaço kids", "area kids", "área kids"
        ])
        or "familia_com_criancas" in normalize_str_list(item.get("ideal_para", []))
    )


def restaurant_matches_happy_hour(item):
    hh = item.get("happy_hour", {})
    if isinstance(hh, dict) and hh.get("ativo") is True:
        return True

    return restaurant_has_any(item, [
        "happy hour", "drinks em dobro", "double drinks",
        "double gin", "double caipirinha", "promocoes de chopp",
        "promoções de chopp", "chopp", "rooftop"
    ])


def restaurant_matches_burger(item):
    return restaurant_has_any(item, [
        "hamburguer", "hambúrguer", "burger", "lanche",
        "hamburguer_artesanal"
    ])


def restaurant_matches_chocolate(item):
    return restaurant_has_any(item, [
        "doce", "sobremesa", "chocolate", "chocolateria",
        "kopenhagen", "cacau show"
    ])


def restaurant_matches_rooftop(item):
    return restaurant_has_any(item, [
        "rooftop", "vista", "drinks", "happy hour"
    ])


def restaurant_matches_tradicional(item):
    return restaurant_has_any(item, [
        "tradicional", "classico", "clássico", "frutos do mar"
    ])


def restaurant_matches_especial(item):
    return restaurant_has_any(item, [
        "experiencia", "experiência", "ocasiao especial", "ocasião especial",
        "jantar especial", "sofisticado", "elegante", "atmosferico",
        "atmosférico", "sensorial", "lounge", "tematico", "temático"
    ])


def restaurant_matches_japanese(item):
    return restaurant_has_any(item, [
        "japones", "japonês", "sushi"
    ])


def restaurant_matches_pizza(item):
    return restaurant_has_any(item, [
        "pizza", "pizzaria"
    ])


def unique_restaurants(items):
    result = []
    seen = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        nome = (item.get("nome") or "").strip()
        if not nome:
            continue
        key = normalize_text(nome)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)

    return result


def build_restaurant_line(item):
    nome = item.get("nome", "")
    perfil = item.get("perfil", "")
    dist = item.get("distancia", "")
    line = f"• **{nome}**"
    extras = []

    if perfil:
        extras.append(perfil)
    if dist:
        extras.append(format_distance(dist))

    if extras:
        line += " → " + " | ".join(extras)

    return line


def get_restaurant_candidates_by_mode(restaurantes, mode):
    mode_n = normalize_text(mode)

    if mode_n in ["rapido", "rápido", "lanche", "pratico", "prático"]:
        return [
            r for r in restaurantes
            if normalize_text(r.get("tipo", "")) == "rapido"
            or restaurant_has_any(r, ["lanche", "praticidade", "sem complicacao", "sem complicação"])
        ]

    if mode_n in ["especial", "romantico", "romântico", "sofisticado", "premium"]:
        return [r for r in restaurantes if restaurant_matches_especial(r)]

    if mode_n in ["tradicional", "classico", "clássico"]:
        return [
            r for r in restaurantes
            if normalize_text(r.get("tipo", "")) == "tradicional"
            or restaurant_has_any(r, ["tradicional", "classico", "clássico"])
        ]

    if mode_n in ["frutos do mar"]:
        return [
            r for r in restaurantes
            if normalize_text(r.get("subtipo", "")) == "frutos_do_mar"
            or restaurant_has_any(r, ["frutos do mar", "camarão", "camarao", "litoranea", "litorânea"])
        ]

    if mode_n in ["japones", "japonês", "sushi"]:
        return [r for r in restaurantes if restaurant_matches_japanese(r)]

    if mode_n in ["pizza", "pizzaria"]:
        return [r for r in restaurantes if restaurant_matches_pizza(r)]

    if mode_n in ["doce", "sobremesa", "chocolate"]:
        return [r for r in restaurantes if restaurant_matches_chocolate(r)]

    if mode_n in ["hamburguer", "hambúrguer", "burger", "lanche_burger"]:
        return [r for r in restaurantes if restaurant_matches_burger(r)]

    if mode_n in ["kids", "crianca", "criança", "criancas", "crianças", "familia", "família"]:
        return [
            r for r in restaurantes
            if restaurant_matches_kids(r)
            or "familia_com_criancas" in normalize_str_list(r.get("ideal_para", []))
        ]

    if mode_n in ["happy hour", "happy_hour", "drinks", "rooftop"]:
        return [
            r for r in restaurantes
            if (
                isinstance(r.get("happy_hour", {}), dict)
                and r.get("happy_hour", {}).get("ativo") is True
            )
        ]

    if mode_n in ["vista", "mirante", "lugar bonito", "lugar com vista", "rooftop_vista"]:
        return [
            r for r in restaurantes
            if restaurant_matches_rooftop(r)
            or restaurant_has_any(r, ["vista", "mirante"])
        ]

    return []

def restaurant_mode_priority_score(item, mode):
    mode_n = normalize_text(mode)
    score = 0

    ideals = normalize_str_list(item.get("ideal_para", []))
    subtipo = normalize_text(item.get("subtipo", ""))
    tipo = normalize_text(item.get("tipo", ""))

    if mode_n == "kids":
        if "familia_com_criancas" in ideals:
            score += 50
        if restaurant_has_any(item, ["area kids", "área kids", "kids", "espaco criança", "espaço criança", "espaco kids", "espaço kids"]):
            score += 40
        if restaurant_has_any(item, ["pizza", "variedade", "familia", "família"]):
            score += 15

    elif mode_n == "happy hour":
        hh = item.get("happy_hour", {})
        if isinstance(hh, dict) and hh.get("ativo") is True:
            score += 60
            if hh.get("horario"):
                score += 10
        if restaurant_has_any(item, ["rooftop", "drinks", "chopp", "musica ao vivo", "música ao vivo"]):
            score += 20

    elif mode_n == "frutos do mar":
        if subtipo == "frutos_do_mar":
            score += 60
        if restaurant_has_any(item, ["frutos do mar", "camarão", "camarao", "litoranea", "litorânea"]):
            score += 25
        if tipo == "tradicional":
            score += 10

    elif mode_n == "hamburguer":
        if restaurant_has_any(item, ["hamburguer", "hambúrguer", "burger"]):
            score += 50

    elif mode_n == "doce":
        if restaurant_has_any(item, ["chocolate", "chocolateria", "sobremesa", "doce"]):
            score += 50

    elif mode_n == "pizza":
        if subtipo == "pizzaria" or restaurant_has_any(item, ["pizza", "pizzaria"]):
            score += 50

    elif mode_n == "japones":
        if subtipo == "sushi" or restaurant_has_any(item, ["japones", "japonês", "sushi"]):
            score += 50

    elif mode_n == "especial":
        if restaurant_matches_especial(item):
            score += 40

    elif mode_n == "tradicional":
        if tipo == "tradicional":
            score += 40

    return score


def get_markets_data():
    return get_knowledge_list("mercados")


def get_farmacias_data():
    return get_knowledge_list("farmacias")


def get_passeios_data():
    return get_knowledge_list("passeios")


def filter_passeios_by_tipo_or_categoria(items, value):
    value_n = normalize_text(value)
    return [
        p for p in items
        if normalize_text(p.get("tipo", "")) == value_n
        or normalize_text(p.get("categoria", "")) == value_n
    ]


def filter_passeios_by_ideal(items, target):
    target_n = normalize_text(target)
    result = []

    for p in items:
        ideals = p.get("ideal_para", [])
        ideals_n = [normalize_text(i) for i in ideals if isinstance(i, str)]
        if target_n in ideals_n:
            result.append(p)

    return result


def filter_passeios_by_clima(items, target):
    target_n = normalize_text(target)
    result = []

    for p in items:
        climas = p.get("clima_ideal", [])
        climas_n = [normalize_text(i) for i in climas if isinstance(i, str)]
        if target_n in climas_n:
            result.append(p)

    return result


def build_passeio_line(item):
    nome = item.get("nome", "")
    perfil = item.get("perfil", "")
    obs = item.get("observacao", "")

    line = f"• **{nome}**"
    if perfil:
        line += f" → {perfil}"
    elif obs:
        line += f" → {obs}"

    return line


def format_distance(dist):
    if not dist:
        return ""
    dist_n = normalize_text(str(dist))
    if has_any(dist_n, ["a pe", "a pé", "metros", "ao lado", "menos de", "andando", "km", "regiao", "região", "enseada", "casa grande"]):
        return str(dist)
    if has_any(dist_n, ["min", "minuto", "minutos"]):
        if has_any(dist_n, ["carro", "pé", "a pe", "a pé"]):
            return str(dist)
        return f"{dist} de carro"
    return str(dist)


def distance_sort_key(distance):
    if not distance:
        return 9999
    text = normalize_text(str(distance))
    if has_any(text, ["ao lado"]):
        return 0
    if has_any(text, ["metros"]):
        m = re.search(r"(\d+)", text)
        if m:
            return int(m.group(1)) / 100.0
    if has_any(text, ["km"]):
        m = re.search(r"(\d+(?:[.,]\d+)?)", text)
        if m:
            return float(m.group(1).replace(",", ".")) * 10
    if has_any(text, ["min", "minuto", "minutos"]):
        m = re.search(r"(\d+)", text)
        if m:
            return int(m.group(1))
    return 9999


def best_closest_item(items):
    if not items:
        return None
    return sorted(items, key=lambda x: distance_sort_key(x.get("distancia", "")))[0]


def get_requested_detail_field(text_raw):
    text_n = normalize_text(text_raw)

    if has_any(text_n, ["endereco", "endereço", "onde fica", "localizacao", "localização"]):
        return "endereco"
    if has_any(text_n, [
        "horario", "horário", "horarios", "horários",
        "que horas abre", "que horas funciona", "funciona ate", "funciona até"
    ]):
        return "horario"
    if has_any(text_n, ["telefone", "numero", "número", "fone"]):
        return "telefone"
    if has_any(text_n, ["site", "link", "pagina", "página"]):
        return "site"
    if has_any(text_n, ["instagram", "insta"]):
        return "instagram"
    if has_any(text_n, ["whatsapp", "zap", "whats"]):
        return "whatsapp"
    if has_any(text_n, ["delivery", "entrega"]):
        return "delivery"
    if has_any(text_n, ["takeout", "retirada"]):
        return "takeout"
    if has_any(text_n, ["drive through", "drive-through", "drive thru"]):
        return "drive_through"

    return ""


def looks_like_detail_question(text_raw):
    return bool(get_requested_detail_field(text_raw))


def build_entity_catalog():
    k = knowledge()
    catalog = []

    for item in k.get("restaurantes", []):
        catalog.append({"category": "restaurantes", "item": item})

    for item in k.get("mercados", []):
        catalog.append({"category": "mercado", "item": item})

    for item in k.get("farmacias", []):
        catalog.append({"category": "farmacia", "item": item})

    padaria = k.get("padaria", {})
    if padaria:
        catalog.append({"category": "padaria", "item": padaria})

    saude = k.get("saude", {})
    if saude.get("upa"):
        catalog.append({"category": "saude", "item": saude["upa"]})
    if saude.get("hospital"):
        catalog.append({"category": "saude", "item": saude["hospital"]})

    for item in k.get("passeios", []):
        catalog.append({"category": "passeio", "item": item})

    for item in k.get("bares", []):
        if isinstance(item, dict):
            catalog.append({"category": "bares", "item": item})

    return catalog


def entity_aliases(entity):
    item = entity.get("item", {})
    name = item.get("nome", "")
    aliases = set()

    if name:
        aliases.add(name)
        aliases.add(normalize_text(name))

    manual = {
        "McDonald's Enseada": ["mcdonald", "mcdonalds", "mc donald", "mc donalds"],
        "Burger King": ["burguer king", "burger king", "bk"],
        "Madero & Jeronimo Burger Guarujá": ["madero", "jeronimo", "jerônimo", "jeronimo burger", "jeronimo track"],
        "Alcide’s": ["alcides", "alcide's", "alcides restaurante"],
        "Alcides Pizzaria": ["alcides pizzaria", "pizzaria alcides"],
        "Thai Lounge Bar": ["thai lounge", "thai"],
        "Restaurante Atlântico Signature": ["atlantico signature", "atlântico signature", "atlantico", "atlântico"],
        "Dati": ["dati"],
        "Sushi Katoshi 23": ["sushi katoshi", "katoshi"],
        "Kopenhagen Enseada": ["kopenhagen"],
        "Cacau Show": ["cacau show"],
        "Restaurante Mirante Bela Vista": ["mirante bela vista", "bela vista"],
        "Mercado Dia": ["dia", "mercado dia", "supermercado dia"],
        "Pão de Açúcar - Enseada": ["pao de acucar", "pão de açúcar", "pao de acucar enseada"],
        "Carrefour - Enseada": ["carrefour"],
        "Extra": ["extra"],
        "Padaria Pitangueiras": ["padaria pitangueiras", "pitangueiras"],
        "Drogasil": ["drogasil"],
        "Drogaria São Paulo": ["drogaria sao paulo", "drogaria são paulo", "sao paulo", "são paulo"],
        "Droga Raia": ["droga raia", "raia"],
        "Poupafarma": ["poupafarma"],
        "UPA Enseada": ["upa", "upa enseada"],
        "Hospital Santo Amaro": ["hospital", "hospital santo amaro", "santo amaro"],
        "Shopping La Plage": ["la plage", "shopping la plage"],
        "Shopping Enseada": ["shopping enseada"],
        "Cinema Cine Guarujá": ["cine guaruja", "cine guarujá", "cinema"],
        "Acqua Mundo - Aquário Guarujá": ["acqua mundo", "aquario", "aquário"],
        "Feira da Enseada": ["feira da enseada", "feira"],
        "Morro do Maluf - Mirante da Campina": ["morro do maluf", "mirante da campina", "maluf", "mirante"],
        "Dona Eva - Restaurante, Bar e Chopperia": ["dona eva"],
        "Boteco Burgman Enseada": ["burgman", "boteco burgman"],
        "Parque Ecológico Renan C. Teixeira": ["parque ecológico", "parque ecologico", "parque renan", "parque"],
        "Villa Di Phoenix Praia": ["villa", "villa di phoenix", "villa phoenix", "phoenix", "villa di phoenix praia"],
        "Dolores Bar & Restaurante": ["dolores", "dolores bar", "dolores restaurante"],
        "Pirata’s Burger Rooftop": ["piratas", "pirata's", "piratas burger", "pirata's burger", "piratas rooftop"]
    }

    for a in manual.get(name, []):
        aliases.add(a)

    return sorted(aliases, key=lambda x: len(normalize_text(x)), reverse=True)


GENERIC_ENTITY_ALIASES = {
    "hospital",
    "upa",
    "shopping",
    "cinema",
    "mirante",
    "feira",
    "parque"
}


def is_generic_entity_alias(alias: str) -> bool:
    return normalize_text(alias) in GENERIC_ENTITY_ALIASES


def contextual_entity_category(last_topic: str, inferred_intent: str = "") -> str:
    topic = normalize_text(last_topic or inferred_intent or "")

    mapping = {
        "restaurantes": "restaurantes",
        "mercado": "mercado",
        "farmacia": "farmacia",
        "padaria": "padaria",
        "passeio": "passeio",
        "shopping": "passeio",
        "feira": "passeio",
        "bares": "bares",
        "saude": "saude"
    }

    return mapping.get(topic, "")


def resolve_entity_from_text(text_raw, allow_generic_aliases=True, preferred_category=""):
    text_n = normalize_text(text_raw)
    catalog = build_entity_catalog()

    ranked = []
    for entity in catalog:
        category = entity.get("category", "")

        for alias in entity_aliases(entity):
            alias_n = normalize_text(alias)

            if not allow_generic_aliases and is_generic_entity_alias(alias_n):
                continue

            if phrase_in_text(text_n, alias):
                score = len(alias_n)

                if preferred_category and category == preferred_category:
                    score += 100

                if is_generic_entity_alias(alias_n):
                    score -= 40

                ranked.append((score, entity))
                break

    if not ranked:
        return None

    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1]


def resolve_entities_from_text(text_raw, allow_generic_aliases=False, preferred_category=""):
    text_n = normalize_text(text_raw)
    catalog = build_entity_catalog()
    ranked = []

    for entity in catalog:
        category = entity.get("category", "")
        best_score = None

        for alias in entity_aliases(entity):
            alias_n = normalize_text(alias)

            if not allow_generic_aliases and is_generic_entity_alias(alias_n):
                continue

            if phrase_in_text(text_n, alias):
                score = len(alias_n)

                if preferred_category and category == preferred_category:
                    score += 100

                if is_generic_entity_alias(alias_n):
                    score -= 40

                if best_score is None or score > best_score:
                    best_score = score

        if best_score is not None:
            ranked.append((best_score, entity))

    ranked.sort(key=lambda x: x[0], reverse=True)

    result = []
    seen = set()
    for _, entity in ranked:
        name_key = normalize_text(entity.get("item", {}).get("nome", ""))
        if name_key and name_key not in seen:
            seen.add(name_key)
            result.append(entity)

    return result


def resolve_last_entity_from_session():
    sess = load_session()
    target_name = (sess.get("last_entity_name") or sess.get("last_recommendation_name") or "").strip()
    if not target_name:
        return None

    target_n = normalize_text(target_name)
    for entity in build_entity_catalog():
        nome = normalize_text(entity.get("item", {}).get("nome", ""))
        if nome == target_n:
            return entity

    return None


def get_entity_detail_reply(entity, field):
    if not entity or not field:
        return ""

    item = entity.get("item", {})
    category = entity.get("category", "")
    nome = item.get("nome", "Local")

    label_map = {
        "endereco": "Endereço",
        "horario": "Horário",
        "telefone": "Telefone",
        "site": "Site",
        "instagram": "Instagram",
        "whatsapp": "WhatsApp",
        "delivery": "Delivery",
        "takeout": "Takeout / retirada",
        "drive_through": "Drive-through"
    }

    value = ""

    if field == "telefone":
        if item.get("telefone"):
            value = item.get("telefone")
        elif item.get("telefones"):
            value = ", ".join(item.get("telefones", []))
    else:
        value = item.get(field, "")

    if not value:
        if field == "endereco":
            return f"Não encontrei o endereço do **{nome}** na base neste momento."
        if field == "horario":
            return f"Não encontrei o horário do **{nome}** na base neste momento."
        if field == "telefone":
            return f"Não encontrei telefone do **{nome}** na base neste momento."
        return f"Não encontrei essa informação do **{nome}** na base neste momento."

    set_last_entity(nome, category)

    return (
        "Claro 😊\n\n"
        f"**{nome}**\n"
        f"• {label_map.get(field, field.title())}: {value}"
    )


def get_entity_summary_reply(entity):
    if not entity:
        return ""

    item = entity.get("item", {})
    category = entity.get("category", "")
    nome = item.get("nome", "Local")
    perfil = item.get("perfil", "")
    obs = item.get("observacao", "")
    endereco = item.get("endereco", "")
    horario = item.get("horario", "")

    set_last_entity(nome, category)

    reply = f"Claro 😊\n\n**{nome}**"

    if category == "restaurantes":
        details = []

        if perfil:
            reply += f"\n\n{perfil}."

        if item.get("distancia"):
            details.append(f"• Distância: {format_distance(item.get('distancia', ''))}")
        if item.get("tempo_a_pe"):
            details.append(f"• A pé: {item.get('tempo_a_pe')}")
        if item.get("tempo_de_carro"):
            details.append(f"• De carro: {item.get('tempo_de_carro')}")
        if horario:
            details.append(f"• Horário: {horario}")
        if endereco:
            details.append(f"• Endereço: {endereco}")

        hh = item.get("happy_hour", {})
        if isinstance(hh, dict) and hh.get("ativo") is True:
            hh_parts = ["• Happy hour: sim"]
            if hh.get("horario"):
                hh_parts[-1] += f" ({hh.get('horario')})"
            details.extend(hh_parts)

        if details:
            reply += "\n\n" + "\n".join(details)

        if obs:
            reply += f"\n\n{obs}"

        return reply

    if perfil:
        reply += f"\n\n{perfil}."
    elif obs:
        reply += f"\n\n{obs}"

    details = []
    if endereco:
        details.append(f"• Endereço: {endereco}")
    if horario:
        details.append(f"• Horário: {horario}")

    if details:
        reply += "\n\n" + "\n".join(details)

    return reply


def get_entity_comparison_reply(entity_a, entity_b):
    if not entity_a or not entity_b:
        return ""

    item_a = entity_a.get("item", {})
    item_b = entity_b.get("item", {})
    cat_a = entity_a.get("category", "")
    cat_b = entity_b.get("category", "")

    if cat_a != "restaurantes" or cat_b != "restaurantes":
        return ""

    nome_a = item_a.get("nome", "Opção A")
    nome_b = item_b.get("nome", "Opção B")

    set_last_entity(nome_a, "restaurantes")
    set_active_recommendations("restaurantes", [nome_a, nome_b], current_name=nome_a)
    update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome_a)

    def dist_text(item):
        parts = []
        if item.get("distancia"):
            parts.append(format_distance(item.get("distancia", "")))
        if item.get("tempo_de_carro"):
            parts.append(f"carro: {item.get('tempo_de_carro')}")
        if item.get("tempo_a_pe"):
            parts.append(f"a pé: {item.get('tempo_a_pe')}")
        return " | ".join(parts)

    def hh_text(item):
        hh = item.get("happy_hour", {})
        if isinstance(hh, dict) and hh.get("ativo") is True:
            if hh.get("horario"):
                return f"happy hour: {hh.get('horario')}"
            return "happy hour: sim"
        if restaurant_matches_happy_hour(item):
            return "happy hour: sim"
        return ""

    linhas_a = [f"**{nome_a}**"]
    linhas_b = [f"**{nome_b}**"]

    if item_a.get("perfil"):
        linhas_a.append(f"• Perfil: {item_a.get('perfil')}")
    if item_b.get("perfil"):
        linhas_b.append(f"• Perfil: {item_b.get('perfil')}")

    da = dist_text(item_a)
    db = dist_text(item_b)
    if da:
        linhas_a.append(f"• Distância: {da}")
    if db:
        linhas_b.append(f"• Distância: {db}")

    hha = hh_text(item_a)
    hhb = hh_text(item_b)
    if hha:
        linhas_a.append(f"• {hha}")
    if hhb:
        linhas_b.append(f"• {hhb}")

    if item_a.get("observacao"):
        linhas_a.append(f"• Observação: {item_a.get('observacao')}")
    if item_b.get("observacao"):
        linhas_b.append(f"• Observação: {item_b.get('observacao')}")

    return (
        f"{pick_comparison_intro()}\n\n"
        + "\n".join(linhas_a)
        + "\n\n"
        + "\n".join(linhas_b)
        + "\n\nSe quiser, eu também posso te dizer **qual faz mais sentido para o que vocês estão afim agora**."
    )


def should_use_entity_detail_mode(text_raw, inferred_intent="", last_topic=""):
    text_n = normalize_text(text_raw)
    field = get_requested_detail_field(text_raw)

    if not field:
        return False

    if last_topic == "praia" and has_any(text_n, [
        "horario", "horário", "horarios", "horários",
        "que horas", "que horas funciona",
        "funciona que horas", "ate que horas", "até que horas",
        "endereco", "endereço",
        "servico de praia", "serviço de praia",
        "como funciona", "funciona"
    ]):
        return False

    explicit_entity = resolve_entity_from_text(text_raw)
    if explicit_entity:
        return True

    sess = load_session()
    has_last_entity = bool((sess.get("last_entity_name") or "").strip())
    if has_last_entity:
        if has_any(text_n, [
            "horario de silencio", "horário de silêncio",
            "servico de praia", "serviço de praia",
            "contato no predio", "contato no prédio",
            "com quem falar no predio", "com quem falar no prédio",
            "quem contactar no predio", "quem contactar no prédio",
            "ajuda no condominio", "ajuda no condomínio",
            "ajuda no predio", "ajuda no prédio",
            "todos", "todas",
            "farmacia", "farmácia", "farmacias", "farmácias",
            "restaurantes", "mercados", "supermercados",
            "shopping", "cinema", "mirante", "feira",
            "upa", "hospital"
        ]):
            return False

        if not is_safe_context_for_generic_detail(last_topic, field) and not resolve_entity_from_text(text_raw):
            return False

        return True

    return False


def is_social_checkin(text_raw):
    text_n = normalize_text(text_raw)

    exacts = {
        "tudo bem", "td bem", "como vai", "como vc ta", "como voce ta",
        "como vc esta", "como voce esta", "como você tá", "como você está",
        "ta tudo bem", "tá tudo bem", "vc ta bem", "você tá bem"
    }
    if text_n in exacts:
        return True

    return has_any(text_n, [
        "tudo bem?", "td bem?", "como vai?", "como vc ta?", "como voce ta?",
        "como vc esta?", "como voce esta?", "como você tá?", "como você está?"
    ])


# =========================
# VOZ / MICROCOPY GEPETTO
# =========================

def gepetto_line(key):
    lines = {
        "welcome_1": "Olá 😊 Eu sou o Gepetto — seu concierge digital durante a estadia.\nFui projetado para ajudar com praia, comida, conforto e imprevistos leves. Mergulho, por enquanto, segue indisponível.",
        "welcome_2": "Bem-vindo 😊\nEu sou o Gepetto: um robô de praia surpreendentemente útil e rigorosamente não impermeável.",
        "welcome_3": "Olá 😊 Eu sou o Gepetto, concierge digital do Apto 14B.\nEstou por aqui para tornar sua experiência mais prática, leve e bem cuidada — com um toque de camisa de praia.",
        "welcome_4": "Olá 😊 Eu sou o Gepetto.\nPosso te ajudar com Wi-Fi, praia, mercados, restaurantes, regras da casa e qualquer dúvida útil da estadia.",
        "welcome_5": "Bem-vindo 😊\nSou o Gepetto — metade concierge, metade personagem improvável da sua viagem.",

        "incident_1": "Entendi 👍 Já deixei isso encaminhado por aqui.\nSó me ajuda com uma informação: isso aconteceu agora ou vocês já tinham percebido antes?",
        "incident_2": "Entendi 👍 Já deixei isso sinalizado por aqui.\nEu não subo até o apartamento, mas faço a informação subir bem rápido.",
        "incident_3": "Entendi 😊 Já deixei o acompanhamento acionado por aqui.\nMe ajuda só com um detalhe para registrar melhor: isso aconteceu do nada ou já estava assim antes?",
        "incident_4": "Perfeito, já deixei isso encaminhado por aqui.\nSó preciso confirmar: está totalmente sem funcionar ou ainda funciona parcialmente?",
        "incident_5": "Entendi 👍 Já deixei isso em acompanhamento.\nSe puder, me diga só se isso aconteceu agora ou se vocês já haviam notado antes.",

        "fallback_1": "Não entendi perfeitamente 😅\nSe você me contar de outro jeito, eu tento te ajudar melhor.",
        "fallback_2": "Ainda estou em fase beta — mas com ótima vontade e excelente camisa de praia 😄\nSe puder reformular, eu sigo com você.",
        "fallback_3": "Não peguei exatamente o que você quis dizer 😊\nPode me escrever de outro jeito ou me dizer se o assunto é praia, comida, mercado, regras ou apartamento?",
        "fallback_4": "Não entendi muito bem essa parte 😅\nSe me der um pouco mais de contexto, eu consigo te orientar melhor.",
        "fallback_5": "Posso te ajudar melhor se você me disser o tema principal 😊\nPraia, restaurante, mercado, regras, incidente ou falar com Bruno?",

        "bruno_1": "Claro 😊 Posso avisar o Bruno agora.\nTem algum assunto que você queira que eu adiante na notificação?",
        "bruno_2": "Claro 😊 Eu faço a ponte com o Bruno agora.\nSe quiser, já me diga o assunto que eu adianto tudo por aqui.",
        "bruno_3": "Claro 😊 Posso acionar o Bruno agora.\nSe quiser, já me passe o contexto e eu deixo a notificação mais completa.",
        "bruno_4": "Claro 😊 Posso avisá-lo agora.\nSe preferir, também pode só me responder: **envie**.",
        "bruno_5": "Claro 😊 Eu cuido da ponte com o Bruno.\nSe quiser, já me diga o assunto e eu adianto isso por aqui.",

        "praia_1": "Boa escolha 😄\nPosso te orientar sobre localização, horário e como funciona o serviço de praia — sem me aproximar demais da água, por razões técnicas.",
        "praia_2": "Praia eu conheço bem — à distância segura 😊\nSe quiser, te explico rapidinho como funciona o serviço.",
        "praia_3": "Posso te passar a localização da praia, o horário do serviço e a forma mais prática de aproveitar 👍",
        "praia_4": "Se quiser, eu te oriento sobre a praia de forma bem objetiva: onde fica, como funciona e o melhor jeito de aproveitar.",
        "praia_5": "Praia eu acompanho muito bem. Entrar no mar, ainda não 😄\nMas te explico tudo que você precisa saber.",

        "checkout_1": "Claro 😊 Posso te lembrar dos principais pontos antes do check-out.\nA ideia é deixar tudo simples e sem correria no fim da estadia.",
        "checkout_2": "Quando chegar a hora, eu também posso te ajudar com os avisos antes da saída 😊\nSou melhor em organização do que em esportes aquáticos.",
        "checkout_3": "Se quiser, eu já te passo os avisos importantes para antes do check-out 👍",
        "checkout_4": "Posso te orientar sobre os pontos finais da saída para que o check-out fique leve e organizado 😊",
        "checkout_5": "Também acompanho o check-out 😊\nMeu trabalho é fazer a estadia terminar bem — e não dramaticamente.",

        "identity_1": "Eu sou o Gepetto 😊\nSeu concierge digital durante a estadia.",
        "identity_2": "Eu sou o Gepetto — um robô de praia com habilidades surpreendentemente úteis.",
        "identity_3": "Eu sou o Gepetto 😊\nMetade concierge, metade lembrança improvável da sua viagem.",
        "identity_4": "Eu sou o Gepetto, concierge digital do Apto 14B.\nSempre por aqui para facilitar a estadia.",
        "identity_5": "Eu sou o Gepetto 😊\nPouco resistente à água salgada, bastante comprometido com a sua experiência.",

        "social_1": "Tudo certo por aqui 😊 E com você?\nSe precisar, estou por aqui para ajudar no que for útil durante a estadia.",
        "social_2": "Tudo bem por aqui 😄 Sempre à disposição.\nSe quiser, já posso te ajudar com praia, comida, mercado ou qualquer dúvida do apartamento."
    }
    return lines.get(key, "")


def get_gepetto_welcome_line():
    return gepetto_line("welcome_1")


def get_gepetto_fallback_line():
    return gepetto_line("fallback_2")


def get_gepetto_identity_line():
    return gepetto_line("identity_2")


def get_gepetto_praia_line():
    return gepetto_line("praia_1")


def get_gepetto_checkout_line():
    return gepetto_line("checkout_1")


def get_gepetto_incident_ack():
    return gepetto_line("incident_3")


def get_gepetto_bruno_intro():
    return gepetto_line("bruno_5")


def get_social_reply():
    return gepetto_line("social_1")


def pick_confirmation_intro():
    options = [
        "Boa escolha 😊",
        "Ótima escolha 😊",
        "Perfeito 😊",
        "Boa decisão 😊"
    ]
    return random.choice(options)


def pick_alternative_intro():
    options = [
        "Claro 😊",
        "Sem problema 😊",
        "Com certeza 😊",
        "Posso sim 😊"
    ]
    return random.choice(options)


def pick_comparison_intro():
    options = [
        "Depende mais do estilo que você quer agora 😊",
        "Depende do que faria mais sentido para este momento 😊",
        "As opções mudam bastante conforme o perfil que você está buscando 😊",
        "Dá para afinar isso melhor dependendo do tipo de experiência que você quer 😊"
    ]
    return random.choice(options)


def pick_recommendation_intro(topic=""):
    options = {
        "restaurantes": [
            "Se eu fosse te direcionar por aqui 😊",
            "Uma boa linha para seguir seria esta 😊",
            "Se eu tivesse que te apontar um bom caminho agora 😊"
        ],
        "mercado": [
            "Para isso, eu seguiria por aqui 😊",
            "Se fosse para resolver isso de forma prática 😊",
            "Uma escolha bem segura para agora seria esta 😊"
        ],
        "passeio": [
            "Para esse tipo de passeio, eu seguiria por aqui 😊",
            "Se a ideia for algo que funcione bem agora 😊",
            "Uma boa direção para este momento seria esta 😊"
        ],
        "farmacia": [
            "Para isso, eu começaria por aqui 😊",
            "Uma boa referência para agora seria esta 😊",
            "Se eu fosse te orientar de forma prática 😊"
        ],
        "generic": [
            "Eu seguiria por aqui 😊",
            "Uma boa direção seria esta 😊",
            "Se eu fosse te orientar agora 😊"
        ]
    }

    pool = options.get(topic, options["generic"])
    return random.choice(pool)


def pick_followup_soft_close(topic=""):
    closes = {
        "restaurantes": [
            "Se quiser, eu também posso afinar isso pelo estilo que você está procurando.",
            "Se quiser, eu posso te dizer qual combina mais com o momento.",
            "Se quiser, eu também posso te indicar o que eu escolheria sem erro."
        ],
        "mercado": [
            "Se quiser, eu também posso resumir qual faz mais sentido agora.",
            "Se quiser, eu afino isso para praticidade ou variedade.",
            "Se quiser, eu também posso te dizer qual seria a melhor escolha para o que você precisa."
        ],
        "passeio": [
            "Se quiser, eu também posso afinar isso por clima ou perfil.",
            "Se quiser, eu posso te dizer qual combina mais com chuva, família ou algo mais leve.",
            "Se quiser, eu também posso te direcionar para a opção mais prática agora."
        ],
        "farmacia": [
            "Se quiser, eu também posso te direcionar pela urgência ou praticidade.",
            "Se quiser, eu posso te dizer qual faz mais sentido para agora.",
            "Se quiser, eu sigo por aqui e afino isso melhor."
        ],
        "generic": [
            "Se quiser, eu sigo com você por aqui.",
            "Se quiser, eu posso afinar isso melhor.",
            "Se quiser, eu continuo com você nessa."
        ]
    }

    pool = closes.get(topic, closes["generic"])
    return random.choice(pool)


def mensagem_boas_vindas():
    guest = load_guest()
    inicio = saudacao_personalizada(guest)
    especial = observacao_especial(guest)

    if guest_language(guest) == "en":
        return (
            f"{inicio}\n\n"
            f"{especial}"
            "🌴 Welcome to Enseada beach!\n\n"
            "It is a pleasure to have you here 😊 I hope you had a great trip!\n\n"
            "I am **Gepetto**, your personal concierge during your stay.\n\n"
            "I can help with:\n"
            "• **Apartment and building guidance**\n"
            "• **House and building rules**\n"
            "• **Restaurant recommendations**\n"
            "• **Markets and convenience**\n"
            "• **Beach, local tips and activities**\n"
            "• **Weather and day suggestions**\n\n"
            "Feel free to call me anytime 😉"
        )

    return (
        f"{inicio}\n\n"
        f"{especial}"
        f"{get_gepetto_welcome_line()}\n\n"
        "Posso te ajudar com:\n"
        "• **Guia do apartamento e do condomínio**\n"
        "• **Regras da casa e do condomínio**\n"
        "• **Recomendações de restaurantes**\n"
        "• **Mercados e conveniências**\n"
        "• **Praia, passeios e dicas locais**\n"
        "• **Clima e sugestões para o dia**\n\n"
        "Fique à vontade para me chamar a qualquer momento 😉"
    )


def proactive_prompt(guest):
    grupo = guest_group_label(guest)
    top_pref = top_guest_preference(guest)

    if guest_language(guest) == "en":
        if top_pref == "japones":
            return "If you want, I can already point you to a very good **Japanese restaurant** nearby 🍣"
        if top_pref == "doce":
            return "If you want, I can already point you to a nice **dessert/chocolate option** nearby 🍫"
        if top_pref == "praia":
            return "If you want, I can already guide you about the **beach** and beach service here 🏖️"
        return "If you want, I can already help with **restaurants**, **markets**, **beach**, **house rules** or **weather today** 😉"

    if top_pref == "japones":
        return "Se quiser, já posso te indicar um **japonês** muito bom por aqui 🍣"
    if top_pref == "doce":
        return "Se quiser, já posso te indicar uma opção gostosa de **doce ou chocolataria** 🍫"
    if top_pref == "praia":
        return "Se quiser, já posso te orientar sobre a **praia** e o serviço por aqui 🏖️"
    if top_pref == "mercado":
        return "Se quiser, já posso te indicar um **mercado rápido** ou um mais **completo** 🛒"

    if grupo == "família":
        return (
            "Se quiser, posso te indicar agora:\n"
            "• uma boa opção de **restaurante** 🍽️\n"
            "• um **mercado próximo** 🛒\n"
            "• como funciona a **praia** 🏖️\n"
            "• ou as principais **regras da casa** 📋"
        )

    if grupo == "amigos":
        return (
            "Se quiser, posso te indicar agora:\n"
            "• um lugar bom pra **comer ou jantar** 🍽️\n"
            "• uma opção rápida de **mercado** 🛒\n"
            "• como funciona a **praia** 🏖️\n"
            "• ou te passar as principais **regras da casa** 📋"
        )

    if grupo == "casal":
        return (
            "Se quiser, posso te indicar agora:\n"
            "• um restaurante mais **especial** ✨\n"
            "• uma opção rápida de **mercado** 🛒\n"
            "• te orientar sobre a **praia** 🏖️\n"
            "• ou te passar as principais **regras da casa** 📋"
        )

    options = [
        "Se quiser, posso te indicar agora um **restaurante**, um **mercado**, a **praia**, te passar as **regras da casa** ou até a **previsão do tempo** 😉",
        "Posso te ajudar agora com **praia**, **mercado**, **restaurantes**, **regras da casa**, **clima** ou qualquer dúvida do apartamento 😄",
        "Se preferir, já posso começar te orientando sobre **praia**, **comida**, **compras rápidas**, **regras da casa** ou **tempo hoje** 👍"
    ]
    return random.choice(options)


def remember_guest_details(text_raw):
    guest = load_guest()
    changed = False
    text_n = normalize_text(text_raw)

    strong_name_patterns = [
        r"\bmeu nome (?:e|eh|é)\s+([a-zA-ZÀ-ÿ' ]{2,40})",
        r"\bme chamo\s+([a-zA-ZÀ-ÿ' ]{2,40})",
        r"\baqui (?:e|eh|é)\s+([a-zA-ZÀ-ÿ' ]{2,40})"
    ]

    soft_name_patterns = [
        r"\bsou o\s+([a-zA-ZÀ-ÿ' ]{2,30})",
        r"\bsou a\s+([a-zA-ZÀ-ÿ' ]{2,30})"
    ]

    blocked = [
        "gepetto", "concierge", "hospede", "hóspede",
        "anfitriao", "anfitrião", "bruno", "cara", "amigo",
        "casal", "familia", "família", "amigos",
        "do rio", "do interior", "fa de", "fã de"
    ]

    if not guest.get("nome"):
        for pattern in strong_name_patterns + soft_name_patterns:
            match = re.search(pattern, text_raw, flags=re.IGNORECASE)
            if match:
                possible_name = match.group(1).strip()
                if (
                    possible_name
                    and len(possible_name.split()) <= 3
                    and not has_any(possible_name, blocked)
                    and not has_any(possible_name, ["japones", "japonês", "sushi", "praia", "mercado"])
                ):
                    guest["nome"] = title_case_name(possible_name)
                    changed = True
                    break

    if not guest.get("grupo"):
        if has_any(text_n, ["somos um casal", "somos casal", "vim com minha esposa", "vim com meu marido"]):
            guest["grupo"] = "casal"
            changed = True
        elif has_any(text_n, ["estamos em familia", "estamos em família", "vim com minha familia", "vim com minha família"]):
            guest["grupo"] = "familia"
            changed = True
        elif has_any(text_n, ["estou com amigos", "somos amigos", "vim com amigos"]):
            guest["grupo"] = "amigos"
            changed = True

    if changed:
        save_guest(guest)

    return guest, changed


# =========================
# INTENÇÃO / CONTEXTO
# =========================

def infer_contextual_followup(text_raw, last_topic):
    text_n = normalize_text(text_raw)

    if not last_topic:
        return ""

    if last_topic == "praia":
        if has_any(text_n, [
            "onde fica", "localizacao", "localização",
            "horario", "horário", "horarios", "horários",
            "que horas", "que horas funciona",
            "funciona que horas", "ate que horas", "até que horas",
            "como funciona", "funciona",
            "e o horario", "e o horário",
            "e o endereco", "e o endereço",
            "endereco", "endereço",
            "agora", "mais tarde", "ainda hoje",
            "amanha", "amanhã", "amanha cedo", "amanhã cedo",
            "essa noite", "esta noite",
            "daqui a pouco", "logo mais",
            "ja abre", "já abre", "abre",
            "ja comeca", "já começa", "comeca", "começa",
            "ja inicia", "já inicia", "inicia"
        ]):
            return "praia"

    if last_topic == "saude":
        if has_any(text_n, [
            "farmacia", "farmácia", "farmacias", "farmácias",
            "upa", "hospital", "todos", "todas",
            "24h", "vinte e quatro", "entrega", "delivery"
        ]):
            return "saude"

    if last_topic == "roteiro":
        if has_any(text_n, ["casal", "familia", "família", "grupo", "amigos", "galera", "pessoal"]):
            return "roteiro"

    if last_topic == "airbnb_info":
        if has_any(text_n, ["envie o anuncio", "envie anuncio", "anuncio", "anúncio", "falar com bruno"]):
            return "airbnb_info"

    if has_any(text_n, [
        "mais perto", "perto", "mais barato", "barato",
        "mais especial", "especial", "mais completo", "completo",
        "mais rapido", "mais rápido", "rapido", "rápido",
        "em conta", "mais em conta",
        "algo leve", "algo melhor",
        "qual melhor", "qual voce indica", "qual você indica",
        "qual vc indica", "qual voce recomenda", "qual você recomenda",
        "qual vc recomenda", "mais tranquilo", "mais animado",
        "vale a pena", "compensa", "e esse", "e essa",
        "o outro", "a outra", "outro", "outra",
        "esse lugar", "essa opcao", "essa opção",
        "esse local", "esse ai", "esse aí", "essa ai", "essa aí",
        "qual deles", "qual delas", "tem outro", "tem outra",
        "supermercados", "mercados", "outro mercado", "outros mercados",
        "restaurantes", "outro restaurante", "outros restaurantes",
        "farmacia", "farmácia", "farmacias", "farmácias",
        "upa", "hospital", "todos", "todas",
        "pizza", "japones", "japonês", "doce", "vista", "24h", "entrega",
        "shopping", "cinema", "mirante", "chuva", "familia", "família",
        "tradicional", "classico", "clássico",
        "happy hour", "hamburguer", "hambúrguer", "kids", "chocolate", "animado", "conversar",
        "lugar para conversar", "lugar pra conversar", "lugar animado",
        "e o endereco", "e o endereço", "e o horario", "e o horário",
        "e entrega", "e delivery"
    ]):
        return last_topic

    very_short_contextual = [
        "qual", "melhor", "barato", "perto", "especial",
        "completo", "tranquilo", "animado", "leve",
        "rapido", "rápido", "em conta",
        "esse", "essa", "entao", "então", "vc indica", "casal", "familia", "família", "grupo",
        "envie o anuncio", "envie anuncio", "anuncio", "anúncio", "falar com bruno",
        "localizacao", "localização", "horario", "horário", "horarios", "horários",
        "servico", "serviço", "envie", "manda", "pode mandar",
        "farmacia", "farmácia", "upa", "hospital", "todos", "todas",
        "pizza", "japones", "japonês", "doce", "vista", "happy hour",
        "hamburguer", "hambúrguer", "kids", "chocolate", "animado", "conversar",
        "que horas", "como funciona", "shopping", "cinema", "mirante", "feira",
        "o outro", "a outra", "outro", "outra",
        "esse lugar", "essa opcao", "essa opção",
        "esse ai", "esse aí", "essa ai", "essa aí",
        "qual deles", "qual delas", "tem outro", "tem outra",
        "endereco", "endereço",
        "tradicional", "familia", "família", "chuva",
        "agora", "mais tarde", "ainda hoje",
        "amanha", "amanhã", "amanha cedo", "amanhã cedo",
        "essa noite", "esta noite",
        "daqui a pouco", "logo mais",
        "abre", "ja abre", "já abre",
        "comeca", "começa", "ja comeca", "já começa",
        "inicia", "ja inicia", "já inicia"
    ]
    if text_n in very_short_contextual:
        return last_topic

    return ""


def is_followup_candidate(text_raw, last_topic, inferred_intent):
    if not last_topic:
        return False

    if should_prefer_new_intent_over_context(text_raw, last_topic, inferred_intent):
        return False

    if should_ask_for_followup_reference(text_raw, last_topic, inferred_intent):
        return False

    text_n = normalize_text(text_raw)

    strong_new_intents = [
        "wifi", "regras", "localizacao", "tempo", "identidade",
        "saude", "incidente", "chaves", "garagem", "checkout",
        "restaurantes", "mercado", "farmacia", "praia", "apoio_predio",
        "bares", "shopping", "feira", "passeio", "eventos", "surf", "bruno"
    ]
    if inferred_intent in strong_new_intents and inferred_intent != last_topic:
        return False

    if infer_contextual_followup(text_raw, last_topic):
        return True

    exact_short = [
        "sim", "isso", "esse", "essa", "pode ser", "manda", "quero esse",
        "quero essa", "qual", "melhor", "barato", "perto", "especial",
        "vc indica", "vcs indicam", "envie", "enviar", "mandar", "mande",
        "pode avisar", "avise", "avisar", "encaminhe", "encaminhar",
        "rapido", "rápido", "em conta", "farmacia", "farmácia", "upa", "hospital",
        "restaurantes", "outros restaurantes", "todos", "todas", "pizza", "japones",
        "japonês", "doce", "vista", "24h", "entrega", "shopping", "cinema", "mirante", "feira",
        "happy hour", "hamburguer", "hambúrguer", "kids", "chocolate",
        "o outro", "a outra", "outro", "outra",
        "esse lugar", "essa opcao", "essa opção",
        "esse ai", "esse aí", "essa ai", "essa aí",
        "qual deles", "qual delas",
        "endereco", "endereço", "horario", "horário", "horarios", "horários", "delivery",
        "agora", "mais tarde", "ainda hoje",
        "amanha", "amanhã", "amanha cedo", "amanhã cedo",
        "essa noite", "esta noite",
        "daqui a pouco", "logo mais",
        "abre", "ja abre", "já abre",
        "comeca", "começa", "ja comeca", "já começa",
        "inicia", "ja inicia", "já inicia"
    ]
    if text_n in exact_short:
        return True

    return False


def is_ambiguous_reference_message(text_raw):
    text_n = normalize_text(text_raw).strip()

    exacts = {
        "esse", "essa", "esse ai", "esse aí", "essa ai", "essa aí",
        "o outro", "a outra", "outro", "outra",
        "qual", "qual deles", "qual delas",
        "qual?", "qual deles?", "qual delas?",
        "endereco", "endereço",
        "endereco?", "endereço?",
        "horario", "horário", "horarios", "horários",
        "horario?", "horário?", "horarios?", "horários?",
        "entrega", "delivery",
        "entrega?", "delivery?",
        "compensa", "vale a pena",
        "compensa?", "vale a pena?"
    }

    return text_n in exacts


def has_reference_anchor_for_topic(last_topic):
    sess = load_session()

    if last_topic in ["restaurantes", "mercado", "passeio"]:
        current_active = get_current_active_recommendation(last_topic)
        if current_active:
            return True

    if (sess.get("last_entity_name") or "").strip():
        return True

    if (sess.get("last_recommendation_name") or "").strip():
        return True

    return False


def is_safe_context_for_generic_detail(last_topic, field):
    topic_n = normalize_text(last_topic)

    if field in ["endereco", "horario"]:
        return topic_n in [
            "praia", "localizacao", "saude",
            "restaurantes", "mercado", "farmacia",
            "padaria", "passeio"
        ]

    if field in ["delivery", "takeout", "drive_through"]:
        return topic_n in ["restaurantes", "mercado", "farmacia", "saude"]

    if field in ["telefone", "site", "instagram", "whatsapp"]:
        return topic_n in [
            "restaurantes", "mercado", "farmacia",
            "padaria", "passeio", "saude"
        ]

    return False


def should_prefer_new_intent_over_context(text_raw, last_topic, inferred_intent):
    if not last_topic or not inferred_intent:
        return False

    if inferred_intent == last_topic:
        return False

    text_n = normalize_text(text_raw)

    explicit_markers = {
        "wifi": ["wifi", "wi-fi", "internet", "senha do wifi", "senha da internet"],
        "regras": ["regra", "regras", "silencio", "silêncio", "barulho", "lixo", "fumar", "festa"],
        "localizacao": [
            "qual o endereco", "qual o endereço", "endereco daqui", "endereço daqui",
            "endereco para entrega", "endereço para entrega",
            "endereco para delivery", "endereço para delivery",
            "onde estamos", "onde fica aqui"
        ],
        "saude": ["estou doente", "doente", "passando mal", "mal estar", "mal-estar", "dor", "febre", "vomito", "vômito", "enjoo"],
        "incidente": ["quebrou", "nao funciona", "não funciona", "problema", "defeito", "porta nao abre", "porta não abre", "sem energia"],
        "checkout": ["checkout", "check-out", "ir embora", "antes de sair"],
        "chaves": ["chave", "chaves", "tag", "portao", "portão", "portaria"],
        "garagem": ["garagem", "vaga", "estacionar", "estacionamento"],
        "bruno": ["bruno", "anfitriao", "anfitrião", "host"],
        "praia": ["praia", "servico de praia", "serviço de praia", "guarda-sol", "cadeira de praia"],
        "farmacia": ["farmacia", "farmácia", "farmacias", "farmácias", "remedio", "remédio"],
        "mercado": ["mercado", "mercados", "supermercado", "supermercados", "compras"],
        "restaurantes": [
            "restaurante", "restaurantes", "jantar", "comer", "pizza", "japones", "japonês", "sushi",
            "hamburguer", "hambúrguer", "happy hour", "kids", "chocolate"
        ],
        "tempo": ["tempo", "clima", "vai chover", "previsao", "previsão"],
        "passeio": ["o que fazer", "passeio", "passeios", "cinema", "mirante", "shopping", "feira", "chuva"],
        "shopping": ["shopping", "la plage"],
        "feira": ["feira", "feirinha"],
        "bares": ["bar", "bares", "drink", "drinks", "noite", "cerveja"]
    }

    markers = explicit_markers.get(inferred_intent, [])
    if markers and has_any(text_n, markers):
        return True

    return False


def should_ask_for_followup_reference(text_raw, last_topic, inferred_intent):
    if not last_topic:
        return False

    if should_prefer_new_intent_over_context(text_raw, last_topic, inferred_intent):
        return False

    if not is_ambiguous_reference_message(text_raw):
        return False

    field = get_requested_detail_field(text_raw)

    if field:
        if not is_safe_context_for_generic_detail(last_topic, field):
            return True

        if field in ["endereco", "horario", "telefone", "site", "instagram", "whatsapp"]:
            if not has_reference_anchor_for_topic(last_topic) and last_topic not in ["praia", "localizacao", "saude"]:
                return True

        if field in ["delivery", "takeout", "drive_through"]:
            if last_topic in ["farmacia", "saude"]:
                return False
            if not has_reference_anchor_for_topic(last_topic):
                return True

    if normalize_text(text_raw) in [
        "esse", "essa", "esse ai", "esse aí", "essa ai", "essa aí",
        "o outro", "a outra", "outro", "outra",
        "qual", "qual deles", "qual delas",
        "qual?", "qual deles?", "qual delas?",
        "compensa", "vale a pena",
        "compensa?", "vale a pena?"
    ]:
        if not has_reference_anchor_for_topic(last_topic):
            return True

    return False


def get_followup_reference_clarifier(text_raw, last_topic):
    text_n = normalize_text(text_raw)
    field = get_requested_detail_field(text_raw)
    topic_n = normalize_text(last_topic)

    if field == "endereco":
        return "Posso te passar isso sim 😊\n\nSó me diga de qual lugar você quer o **endereço**."
    if field == "horario":
        return "Posso te passar isso sim 😊\n\nSó me diga de qual lugar você quer o **horário**."
    if field in ["delivery", "takeout", "drive_through"]:
        return "Posso verificar isso 😊\n\nSó me diga de qual lugar ou opção você quer esse detalhe."

    if text_n in ["esse", "essa", "esse ai", "esse aí", "essa ai", "essa aí"]:
        return "Posso seguir por aqui 😊\n\nSó me diga qual opção ou lugar você quer considerar."

    if text_n in ["o outro", "a outra", "outro", "outra"]:
        return "Posso te mostrar outra opção sim 😊\n\nSó me diga de qual tema você está falando."

    if text_n in [
        "qual", "qual deles", "qual delas",
        "qual?", "qual deles?", "qual delas?",
        "compensa", "vale a pena",
        "compensa?", "vale a pena?"
    ]:
        return "Posso te ajudar a comparar isso 😊\n\nSó me diga entre quais opções ou sobre qual tema você quer que eu te oriente."

    if topic_n in ["restaurantes", "mercado", "passeio"]:
        return "Posso seguir por aqui 😊\n\nSó me diga qual opção você quer considerar."
    if topic_n == "farmacia":
        return "Posso seguir por aqui 😊\n\nSó me diga qual farmácia ou qual tipo de opção você quer considerar."
    if topic_n == "saude":
        return "Posso seguir por aqui 😊\n\nSó me diga se você quer **farmácia**, **UPA** ou **hospital**."

    return "Posso te ajudar com isso 😊\n\nSó me diga qual local, opção ou tema você quer que eu detalhe."


def score_intents(text_raw, last_topic=""):
    text_n = normalize_text(text_raw)
    scores = {}

    def add(intent, points):
        scores[intent] = scores.get(intent, 0) + points

    if has_any(text_n, [
        "gepetto", "gepeto", "qual seu nome", "como voce chama", "como você chama",
        "quem e voce", "quem é você", "quem te fez", "quem te criou",
        "quem fez voce", "quem fez você", "quem criou voce", "quem criou você",
        "qm e voce", "qm é você"
    ]):
        add("identidade", 12)

    if has_any(text_n, [
        "onde estamos", "qual o endereco", "qual o endereço", "me passa o endereco",
        "me passa o endereço", "endereco daqui", "endereço daqui", "onde fica aqui",
        "endereco para delivery", "endereço para delivery",
        "endereco para entrega", "endereço para entrega",
        "para entrega", "pro delivery", "para o delivery"
    ]):
        add("localizacao", 11)

    if has_any(text_n, ["upa", "hospital", "hospital santo amaro", "upa enseada"]):
        add("localizacao", 10)

    if has_any(text_n, [
        "desmaiou", "desmaio", "nao consegue respirar", "não consegue respirar",
        "falta de ar", "dor no peito", "muita dor", "dor forte", "sangrando",
        "dor", "doente", "febre", "passando mal", "mal estar", "mal-estar",
        "vomito", "vômito", "enjoo", "to mal", "tô mal", "estou doente"
    ]):
        add("saude", 10)

    if has_any(text_n, [
        "fogo", "incendio", "incêndio", "fumaca", "fumaça", "gas", "gás",
        "curto", "cheiro de queimado", "queimando", "vazamento", "sem energia",
        "porta nao abre", "porta não abre", "queimou", "queimado",
        "sofa queimando", "sofá queimando", "pegando fogo",
        "defeito", "quebrou", "quebrado", "parou de funcionar", "nao funciona", "não funciona",
        "nao esta funcionando", "não está funcionando", "nao esta abrindo", "não está abrindo",
        "nao liga", "não liga", "travou", "bugou", "problema", "estragou",
        "microondas", "micro-ondas", "chuveiro", "ar condicionado", "ar-condicionado", "ar",
        "televisao", "televisão", "tv", "fogao", "fogão", "geladeira", "forno",
        "acabou o gas", "acabou o gás", "gas da cozinha", "gás da cozinha",
        "botijao de gas", "botijão de gás", "botijao", "botijão"
    ]):
        add("incidente", 12)

    if has_any(text_n, ["wifi", "wi-fi", "wi fi", "internet", "senha da internet", "senha do wifi"]):
        add("wifi", 12)

    if has_any(text_n, [
        "regra", "regras", "condominio", "condomínio", "silencio", "silêncio",
        "barulho", "som alto", "musica", "música", "musica alta", "música alta",
        "caixa de som", "ruido", "ruído", "areia", "lixo", "louca", "louça", "louca suja", "louça suja",
        "lavar louca", "lavar louça", "fumar", "festa", "festas",
        "reciclagem", "reciclavel", "reciclável", "pode fumar", "pode festa",
        "horario de silencio", "horário de silêncio"
    ]):
        add("regras", 10)

    if phrase_in_text(text_n, "onde fica a praia") or (phrase_in_text(text_n, "onde fica") and phrase_in_text(text_n, "servico de praia")):
        add("praia_local", 12)

    if has_any(text_n, ["praia", "praias", "servico de praia", "serviço de praia", "guarda-sol", "guarda sol", "cadeira de praia"]):
        add("praia", 9)

    if has_any(text_n, ["roteiro", "o que fazer hoje", "plano pro dia", "sugestao de roteiro", "sugestão de roteiro", "o que fazer agora"]):
        add("roteiro", 9)

    if has_any(text_n, [
        "restaurante", "restaurantes", "outro restaurante", "outros restaurantes",
        "almoco", "almoço", "jantar", "comer", "comida", "fome",
        "pizza", "japones", "japonês", "sushi",
        "doce", "sobremesa", "chocolate", "chocolateria",
        "hamburguer", "hambúrguer", "burger", "lanche",
        "happy hour", "drinks", "rooftop", "lugar animado", "animado", "conversar", "lugar para conversar",
        "ambiente animado", "ambiente legal", "mesa para conversar",
        "crianca", "criança", "criancas", "crianças", "kids", "area kids", "área kids", "espaco kids", "espaço kids",
        "kopenhagen", "cacau show", "mcdonald", "burger king",
        "alcides", "thai lounge", "atlantico signature", "atlântico signature", "dati",
        "villa", "villa di phoenix", "dolores", "piratas", "pirata's", "burgman",
        "tradicional", "classico", "clássico", "frutos do mar"
    ]):
        add("restaurantes", 9)

    if has_any(text_n, [
        "mercado", "mercados", "supermercado", "supermercados", "compras",
        "pao de acucar", "pão de açúcar", "carrefour", "extra",
        "agua", "água", "mercado dia", "supermercado dia",
        "outro mercado", "outros mercados", "outras opcoes de mercado", "outras opções de mercado"
    ]):
        add("mercado", 9)

    if phrase_in_text(text_n, "dia") and last_topic == "mercado":
        add("mercado", 7)

    if has_any(text_n, ["padaria", "padarias", "cafe da manha", "café da manhã", "cafe", "café"]):
        add("padaria", 8)

    if has_any(text_n, [
        "farmacia", "farmácia", "farmacias", "farmácias",
        "remedio", "remédio", "dor de cabeca", "dor de cabeça",
        "droga raia", "drogasil", "drogaria sao paulo", "drogaria são paulo", "poupafarma"
    ]):
        add("farmacia", 8)

    if has_any(text_n, [
        "quem contactar no predio", "quem contactar no prédio",
        "quem pode ajudar no predio", "quem pode ajudar no prédio",
        "com quem falar no predio", "com quem falar no prédio",
        "contato no predio", "contato no prédio",
        "auxilio no predio", "auxílio no prédio",
        "apoio no predio", "apoio no prédio",
        "funcionarios do predio", "funcionários do prédio",
        "quem me ajuda no predio", "quem me ajuda no prédio",
        "portaria pode ajudar",
        "ajuda no predio", "ajuda no prédio",
        "ajuda no condominio", "ajuda no condomínio"
    ]):
        add("apoio_predio", 14)

    if has_any(text_n, ["garagem", "vaga", "estacionar", "estacionamento", "trocar de vaga"]):
        add("garagem", 9)

    if has_any(text_n, ["chave", "chaves", "portaria", "tag", "portao", "portão", "deixar a chave"]):
        add("chaves", 10)

    if has_any(text_n, [
        "checkout", "check-out", "check out",
        "antes do checkout", "antes do check-out",
        "ir embora", "antes de sair", "o que fazer antes de sair",
        "avisos antes do checkout", "preciso fazer algo antes de sair"
    ]):
        add("checkout", 9)

    if has_any(text_n, ["bruno", "anfitriao", "anfitrião", "host"]):
        add("bruno", 8)

    if has_any(text_n, ["bar", "bares", "pub", "cerveja", "noite", "beber", "drink", "drinks"]):
        add("bares", 8)

    if has_any(text_n, ["shopping", "shoppings", "la plage"]):
        add("shopping", 8)

    if has_any(text_n, ["feira", "feiras", "artesanato", "feirinha", "feirinhas"]):
        add("feira", 7)

    if has_any(text_n, [
        "tempo", "clima", "previsao", "previsão", "meteorologia",
        "vai chover", "vai fazer sol", "como esta o tempo", "como está o tempo"
    ]):
        add("tempo", 10)

    if has_any(text_n, [
        "passeio", "passeios", "o que fazer", "o que fazer hoje",
        "o que fazer agora", "algum passeio", "alguma ideia de passeio",
        "lugar para ir", "lugares para ir", "algo para fazer",
        "o que fazer com chuva", "o que fazer se chover",
        "mirante", "cinema", "acqua mundo", "aquario", "aquário",
        "parque", "morro do maluf", "shopping la plage",
        "shopping enseada", "feira da enseada", "familia", "família", "chuva"
    ]):
        add("passeio", 9)

    if has_any(text_n, ["evento", "eventos", "show", "shows", "festa na cidade"]):
        add("eventos", 7)

    if has_any(text_n, ["surf", "ondas", "mar", "pico de surf", "surfar"]):
        add("surf", 8)

    if has_any(text_n, [
        "zelador", "paulo", "claudio", "cláudio", "edson",
        "funcionario", "funcionário",
        "alguem no predio", "alguém no prédio",
        "com quem falar", "quem contactar", "contato no prédio", "contato no predio"
    ]):
        add("apoio_predio", 8)

    contextual = infer_contextual_followup(text_raw, last_topic)
    if contextual:
        add(contextual, 4)

    return scores


def infer_primary_intent(text_raw, last_topic=""):
    scores = score_intents(text_raw, last_topic)
    if not scores:
        return ""

    priority = [
        "incidente", "saude", "localizacao", "wifi", "regras", "praia_local",
        "praia", "chaves", "restaurantes", "mercado", "tempo", "padaria", "farmacia",
        "apoio_predio", "garagem", "checkout", "roteiro", "passeio", "surf", "bares",
        "shopping", "feira", "eventos", "bruno", "identidade"
    ]

    best_score = max(scores.values())
    tied = [k for k, v in scores.items() if v == best_score]

    for item in priority:
        if item in tied:
            return item

    return tied[0]


# =========================
# INCIDENTES / SAÚDE
# =========================

def classify_incident(text):
    text_n = normalize_text(text)

    high = [
        "vazamento", "sem energia", "porta nao abre", "porta não abre", "nao entra", "não entra",
        "curto", "fogo", "incendio", "incêndio", "fumaca", "fumaça",
        "gas", "gás", "explosao", "explosão", "cheiro de queimado",
        "queimando", "sofa queimando", "sofá queimando", "pegando fogo"
    ]

    medium = [
        "chuveiro", "ar nao funciona", "ar não funciona", "tv nao liga", "tv não liga",
        "wifi nao funciona", "wifi não funciona", "internet nao funciona", "internet não funciona",
        "parou de funcionar", "quebrou", "quebrado", "defeito", "problema",
        "nao funciona", "não funciona", "nao esta funcionando", "não está funcionando",
        "nao liga", "não liga", "nao esta abrindo", "não está abrindo",
        "queimou", "queimado", "esquentando demais", "travou", "bugou",
        "microondas", "micro-ondas", "fogao", "fogão", "geladeira", "forno",
        "acabou o gas", "acabou o gás", "gas da cozinha", "gás da cozinha",
        "botijao de gas", "botijão de gás", "botijao", "botijão"
    ]

    if has_any(text_n, high):
        return "alta"
    if has_any(text_n, medium):
        return "media"
    return "baixa"


def classify_health(text):
    text_n = normalize_text(text)

    high = [
        "desmaiou", "desmaio", "nao consegue respirar", "não consegue respirar",
        "falta de ar", "muita dor", "dor forte", "dor no peito",
        "sangrando", "muito mal", "urgente", "emergencia", "emergência"
    ]

    medium = [
        "dor", "doente", "febre", "passando mal", "mal estar", "mal-estar",
        "enjoo", "vomito", "vômito", "cansaco", "cansaço", "estou doente"
    ]

    if has_any(text_n, high):
        return "alta"
    if has_any(text_n, medium):
        return "media"
    return "baixa"


def append_incident_record(kind, raw_message, guest, severity):
    payload = {
        "tipo": kind,
        "gravidade": severity,
        "mensagem": raw_message,
        "hospede": guest.get("nome", ""),
        "grupo": guest.get("grupo", ""),
        "checkout": guest_checkout_label(guest),
        "timestamp": now_iso(),
        "status": "aberto"
    }
    append_incident(payload)
    return payload


def send_incident_telegram(kind, raw_message, guest, severity):
    label_guest = guest.get("nome") or "Hóspede sem nome definido"
    emoji = "🚨" if severity == "alta" else "⚠️"
    tg_msg = (
        f"{emoji} {kind.upper()} NO APTO 14B\n\n"
        f"Gravidade: {severity.upper()}\n"
        f"Hóspede: {label_guest}\n"
        f"Mensagem: {raw_message}\n"
        f"Horário: {now_iso()}"
    )
    return send_telegram_message(tg_msg)


def maybe_notify(kind, raw_message, guest, severity):
    if severity not in ["alta", "media"]:
        return False, "gravidade baixa"

    append_incident_record(kind, raw_message, guest, severity)
    ok, detail = send_incident_telegram(kind, raw_message, guest, severity)
    return ok, detail


def is_incident_like_message(text):
    return infer_primary_intent(text, get_last_topic()) == "incidente"


def detect_incident_context_reply(text):
    text_n = normalize_text(text)

    if has_any(text_n, [
        "do nada", "aconteceu agora", "foi agora", "agora pouco",
        "acabou de acontecer", "acabou de rolar", "aconteceu do nada",
        "foi do nada", "agora", "neste momento"
    ]):
        return "O hóspede informou que aconteceu agora / de repente."

    if has_any(text_n, [
        "ja estava assim", "já estava assim", "ja estava", "já estava",
        "ja veio assim", "já veio assim", "ja tinha percebido", "já tinha percebido",
        "percebemos antes", "percebi antes", "desde antes", "isso ja estava assim", "isso já estava assim"
    ]):
        return "O hóspede informou que isso já estava assim antes."

    if has_any(text_n, [
        "esta totalmente sem funcionar", "está totalmente sem funcionar",
        "totalmente sem funcionar", "nao funciona nada", "não funciona nada", "parou de vez"
    ]):
        return "O hóspede informou que está totalmente sem funcionar."

    if has_any(text_n, [
        "funciona parcialmente", "ainda funciona",
        "ainda funciona parcialmente", "funciona mais ou menos", "meio funcionando"
    ]):
        return "O hóspede informou que ainda funciona parcialmente."

    return ""


def append_incident_context_record(raw_message, guest, detail):
    payload = {
        "tipo": "incidente_complemento",
        "gravidade": "info",
        "mensagem": raw_message,
        "detalhe": detail,
        "hospede": guest.get("nome", ""),
        "grupo": guest.get("grupo", ""),
        "checkout": guest_checkout_label(guest),
        "timestamp": now_iso(),
        "status": "complemento"
    }
    append_incident(payload)
    return payload


def notify_incident_context_to_telegram(guest, raw_message, detail):
    nome = guest.get("nome", "").strip() or "Hóspede sem nome definido"
    agora = now_iso()

    msg = (
        "🛠️ COMPLEMENTO DE INCIDENTE — APTO 14B\n\n"
        f"Hóspede: {nome}\n"
        f"Detalhe: {detail}\n"
        f"Mensagem original do hóspede: {raw_message}\n"
        f"Horário: {agora}"
    )
    return send_telegram_message(msg)


def handle_incident_context_followup(guest, text_raw):
    detail = detect_incident_context_reply(text_raw)
    if not detail:
        return ""

    append_incident_context_record(text_raw, guest, detail)
    ok, _ = notify_incident_context_to_telegram(guest, text_raw, detail)
    set_incident_pending(False)

    if ok:
        return (
            "Perfeito 😊\n\n"
            "Já registrei essa informação complementar no acompanhamento e deixei isso sinalizado por aqui."
        )

    return (
        "Perfeito 😊\n\n"
        "Já registrei essa informação complementar por aqui, embora eu não tenha conseguido atualizar a notificação neste momento."
    )


# =========================
# WEATHER / CLIMA
# =========================

def weather_code_to_text(code):
    mapping = {
        0: "céu limpo",
        1: "predominantemente limpo",
        2: "parcialmente nublado",
        3: "nublado",
        45: "neblina",
        48: "neblina com geada",
        51: "garoa leve",
        53: "garoa moderada",
        55: "garoa intensa",
        61: "chuva leve",
        63: "chuva moderada",
        65: "chuva forte",
        71: "neve leve",
        73: "neve moderada",
        75: "neve forte",
        80: "pancadas leves",
        81: "pancadas moderadas",
        82: "pancadas fortes",
        95: "trovoadas"
    }
    return mapping.get(code, "tempo variável")


def build_weather_recommendation(temp=None, apparent=None, weather_code=None, weather_text="", rain=None):
    text = normalize_text(weather_text)

    rainy_codes = [51, 53, 55, 61, 63, 65, 80, 81, 82, 95]
    is_rainy = (
        (isinstance(rain, (int, float)) and rain > 0)
        or (weather_code in rainy_codes)
        or has_any(text, ["chuva", "garoa", "pancadas", "trovoadas", "instavel", "instável"])
    )

    base_temp = None
    if isinstance(apparent, (int, float)):
        base_temp = apparent
    elif isinstance(temp, (int, float)):
        base_temp = temp

    is_hot = base_temp is not None and base_temp >= 28
    is_very_hot = base_temp is not None and base_temp >= 30
    is_cold = base_temp is not None and base_temp <= 22

    if is_hot and is_rainy:
        if is_very_hot:
            return "\n\n**Não esqueça do protetor solar** e, se for sair, **um guarda-chuva pode ajudar** se o tempo virar. ☀️☔"
        return "\n\n**Vale usar protetor solar** e, se for sair, **levar um guarda-chuva** também. ☀️☔"

    if is_cold and is_rainy:
        return "\n\n**Recomendo se agasalhar** e **levar guarda-chuva** se for sair. 🧥☔"

    if is_very_hot:
        return "\n\n**Não esqueça do protetor solar** e tente se hidratar bem ao longo do dia. ☀️"

    if is_hot:
        return "\n\n**Vale usar protetor solar** se você for sair durante o dia. ☀️"

    if is_cold:
        return "\n\n**Recomendo se agasalhar** se for sair, principalmente no começo da manhã, à noite ou se ventar mais. 🧥"

    if is_rainy:
        return "\n\n**Um guarda-chuva é recomendado** se você for sair. ☔"

    return ""


def get_weather_reply():
    k = knowledge()
    clima = k.get("clima", {})
    lat = clima.get("latitude", -23.9786)
    lon = clima.get("longitude", -46.2337)

    if not requests:
        return (
            "No momento eu não consegui consultar a previsão em tempo real 🌦️\n\n"
            "Mas se quiser, eu ainda posso te sugerir um plano de praia ou passeio pela região."
        )

    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,apparent_temperature,weather_code,wind_speed_10m,rain",
            "forecast_days": 1,
            "timezone": "America/Sao_Paulo"
        }
        resp = requests.get(url, params=params, timeout=8)
        data = resp.json()

        current = data.get("current", {})
        temp = current.get("temperature_2m")
        apparent = current.get("apparent_temperature")
        code = current.get("weather_code")
        wind = current.get("wind_speed_10m")
        rain = current.get("rain", 0)

        cond = weather_code_to_text(code)

        chuva_hint = ""
        if isinstance(rain, (int, float)) and rain > 0:
            chuva_hint = "\n\nSe a ideia for algo fora da praia, eu posso te sugerir um passeio coberto hoje 😉"
        elif code in [61, 63, 65, 80, 81, 82, 95]:
            chuva_hint = "\n\nSe quiser, hoje faz sentido pensar em algo fora da praia ou sair com mais flexibilidade ☔"

        referencia = clima.get("referencia", "região")

        weather_response = (
            f"🌦️ **Clima agora na {referencia}**\n\n"
            f"• Condição: {cond}\n"
            f"• Temperatura: **{temp}°C**\n"
            f"• Sensação térmica: **{apparent}°C**\n"
            f"• Vento: **{wind} km/h**"
            f"{chuva_hint}"
        )

        weather_tip = build_weather_recommendation(
            temp=temp,
            apparent=apparent,
            weather_code=code,
            weather_text=weather_response,
            rain=rain
        )

        return weather_response + weather_tip

    except Exception:
        return (
            "No momento eu não consegui consultar a previsão em tempo real 🌦️\n\n"
            "Mas se quiser, eu ainda posso te sugerir um plano de praia, mercado ou passeio pela região."
        )


# =========================
# RESPOSTAS
# =========================

def get_wifi_reply():
    wifi = knowledge().get("wifi", {})
    obs = wifi.get("observacao", "")
    suffix = f"\n\n{obs}" if obs else ""
    return (
        "Claro 😊\n\n"
        f"📶 Usuário: **{wifi.get('rede', 'Volare Hal')}**\n"
        f"🔑 Senha: **{wifi.get('senha', 'Guaruja123@')}**"
        f"{suffix}"
    )


def get_regras_reply(text=""):
    regras = knowledge().get("regras", {})
    text_n = normalize_text(text)

    silencio = regras.get("silencio", "23h às 7h")
    areia = regras.get("areia", "usar lava-pés antes de entrar no elevador")
    lixo = regras.get("lixo", "há ponto de descarte no térreo (possui coleta de recicláveis ♻️)")
    fumar = regras.get("fumar", "proibido fumar nas dependências internas do apartamento 🚭")
    festas = regras.get("festas", "não são permitidas festas ou eventos")
    obs = regras.get("observacao", "")

    if has_any(text_n, ["fumar", "cigarro", "pode fumar", "posso fumar"]):
        return f"Não é permitido fumar. {fumar}"

    if has_any(text_n, ["festa", "festas", "evento", "eventos", "pode fazer festa", "posso fazer festa"]):
        return f"Não são permitidas festas ou eventos. {festas}"

    if has_any(text_n, ["reciclagem", "reciclavel", "reciclável", "lixo", "onde joga o lixo", "coleta", "onde descarta o lixo"]):
        return f"O lixo deve ser descartado conforme esta orientação: {lixo}"

    if has_any(text_n, ["louca", "louça", "louca suja", "louça suja", "lavar louca", "lavar louça"]):
        return "Pedimos que não deixem louça suja na cozinha; favor lavar antes do checkout."

    if has_any(text_n, [
        "silencio", "silêncio", "barulho", "som alto", "musica alta", "música alta",
        "caixa de som", "ruido", "ruído", "perturbar", "incomodar vizinhos",
        "horario de silencio", "horário de silêncio", "pode musica", "pode música",
        "posso por musica", "posso pôr música", "posso colocar musica", "posso colocar música",
        "pode som alto", "posso som alto"
    ]):
        return f"O horário de silêncio deve ser respeitado das {silencio}."

    if has_any(text_n, ["areia", "lava pes", "lava-pes", "lava pés", "e a areia"]):
        return f"A orientação sobre areia é esta: {areia}"

    return (
        "Claro 😊\n\n"
        "Algumas informações importantes:\n"
        f"• Silêncio: {silencio}\n"
        f"• Areia: {areia}\n"
        f"• Lixo: {lixo}\n"
        f"• Fumar: {fumar}\n"
        f"• Festas: {festas}"
        + (f"\n\n{obs}" if obs else "")
    )


def get_identidade_reply(text):
    text_n = normalize_text(text)
    extras = knowledge().get("extras", {})
    concierge_nome = extras.get("concierge_nome", "Gepetto")
    anfitriao = extras.get("anfitriao", "Bruno")

    if has_any(text_n, [
        "quem te fez", "quem te criou", "qm te criou",
        "quem fez voce", "quem fez você", "quem criou voce", "quem criou você"
    ]):
        return f"O **{anfitriao}** me criou para proporcionar a melhor experiência possível por aqui ✨"

    if has_any(text_n, [
        "qual seu nome", "como voce chama", "como você chama",
        "quem e voce", "quem é você", "gepetto", "gepeto",
        "qm e voce", "qm é você"
    ]):
        return get_gepetto_identity_line()

    return f"Oi 😊 Eu sou o **{concierge_nome}**. Em que posso te ajudar?"


def get_localizacao_reply(text):
    text_n = normalize_text(text)
    k = knowledge()
    apt = k.get("apartamento", {})
    endereco = apt.get("endereco", {})
    saude = k.get("saude", {})
    upa = saude.get("upa", {})
    hospital = saude.get("hospital", {})

    if has_any(text_n, [
        "endereco para delivery", "endereço para delivery",
        "endereco para entrega", "endereço para entrega",
        "para entrega", "pro delivery", "para o delivery"
    ]):
        return (
            "Claro 😊\n\n"
            f"📍 **{apt.get('nome', 'Residencial Volare – Apto 14B')}**\n"
            f"{endereco.get('rua', 'Avenida da Saudade, 335')}\n"
            f"{endereco.get('bairro', 'Jardim São Miguel')}\n"
            f"{endereco.get('cidade', 'Praia da Enseada, Guarujá')}\n"
            f"CEP: {endereco.get('cep', '11440-180')}\n\n"
            "Se quiser, também posso te passar um texto pronto para copiar no app de delivery 👍"
        )

    if has_any(text_n, ["upa", "upa enseada"]):
        perfil = upa.get("perfil", "")
        horario = upa.get("horario", "")
        endereco_upa = upa.get("endereco", "")
        telefones = upa.get("telefones", [])
        tel_text = f"\n• Telefones: {', '.join(telefones)}" if telefones else ""
        perfil_text = f"\n\n{perfil}" if perfil else ""
        return (
            "Claro 😊\n\n"
            f"**{upa.get('nome', 'UPA Enseada')}**\n"
            f"• Endereço: {endereco_upa or 'Rua Luiz Rodrigues Pedro, 267, Cidade Atlântica'}\n"
            f"• Atendimento: {horario or 'Emergências 24h'}"
            f"{tel_text}"
            f"{perfil_text}\n\n"
            "Se quiser, eu também posso te orientar para hospital ou farmácia."
        )

    if has_any(text_n, ["hospital", "hospital santo amaro"]):
        perfil = hospital.get("perfil", "")
        endereco_h = hospital.get("endereco", "")
        perfil_text = f"\n\n{perfil}" if perfil else ""
        return (
            "Claro 😊\n\n"
            f"**{hospital.get('nome', 'Hospital Santo Amaro')}**\n"
            f"• Endereço: {endereco_h or 'Rua Quinto Bertoldi, 40 - Vila Maia, Guarujá - SP, 11410-908'}"
            f"{perfil_text}\n\n"
            "Se quiser, eu também posso te orientar para UPA ou farmácia."
        )

    if has_any(text_n, ["qual o endereco", "qual o endereço", "me passa o endereco", "me passa o endereço", "endereco daqui", "endereço daqui", "onde fica aqui"]):
        return (
            "Claro 😊\n\n"
            f"📍 **{apt.get('nome', 'Residencial Volare – Apto 14B')}**\n"
            f"{endereco.get('rua', 'Avenida da Saudade, 335')}\n"
            f"{endereco.get('bairro', 'Jardim São Miguel')}\n"
            f"{endereco.get('cidade', 'Praia da Enseada, Guarujá')}\n"
            f"CEP: {endereco.get('cep', '11440-180')}"
        )

    return (
        "Estamos na deliciosa **praia da Enseada, no Guarujá** 😊\n\n"
        f"No **{apt.get('nome', 'Residencial Volare – Apto 14B')}**, o apartamento do Bruno.\n\n"
        "Se quiser, posso te passar o endereço completo para pedidos, Uber ou compras."
    )


def get_praia_reply(guest=None, text_raw=""):
    guest = guest or load_guest()
    context = get_stay_context(guest, text_raw)
    praia_status = get_praia_service_status(context)

    k = knowledge()
    praia = k.get("praia", {})
    servico = praia.get("servico_praia", {})

    distancia = praia.get("distancia", "280 metros (4 a 5 minutos a pé)")
    horario_servico = servico.get("horario", "9h às 17h")
    localizacao = servico.get("localizacao", "ao lado do Thai Lounge, em frente ao Casa Grande Hotel")
    como_funciona = servico.get("como_funciona", "Os itens ficam montados na areia durante o horário do serviço.")
    melhor_horario = praia.get("melhor_horario", "")
    dica = praia.get("dica", "")
    serv_obs = servico.get("observacao", "")

    intro = ""
    if praia_status == "pre_open":
        intro = (
            "A praia já pode ser uma boa para começar o dia 😊\n\n"
            "Só vale lembrar que o **serviço de praia começa às 9h**."
        )
    elif praia_status == "opening_soon":
        intro = (
            "A praia já está entrando num bom momento 😊\n\n"
            "O **serviço de praia começa em breve, às 9h**."
        )
    elif praia_status == "active":
        intro = (
            "Agora é um bom momento para aproveitar a praia 😊\n\n"
            "O **serviço de praia está funcionando normalmente**."
        )
    elif praia_status == "just_closed":
        intro = (
            "Para hoje, o **serviço de praia já encerrou**.\n\n"
            "Mesmo assim, a praia ainda pode ser boa para caminhar, relaxar um pouco ou curtir o fim de tarde 😊"
        )
    else:
        intro = (
            "À noite, a praia segue como uma boa referência para passeio leve, caminhada ou visual da orla 😊\n\n"
            "Mas o **serviço de praia já não funciona nesse horário**."
        )

    extra_parts = []
    if melhor_horario:
        extra_parts.append(f"• Melhor horário: {melhor_horario}")
    if dica:
        extra_parts.append(f"• Dica: {dica}")
    if serv_obs:
        extra_parts.append(f"• Observação: {serv_obs}")

    extra_text = ""
    if extra_parts:
        extra_text = "\n\n" + "\n".join(extra_parts)

    return (
        f"{intro}\n\n"
        f"A praia fica a **{distancia}**.\n"
        f"O serviço de praia funciona das **{horario_servico}**.\n"
        f"Ele fica **{localizacao}**.\n\n"
        f"{como_funciona}"
        f"{extra_text}"
    )


def get_servico_praia_localizacao_reply():
    servico = knowledge().get("praia", {}).get("servico_praia", {})
    return (
        "Claro 😊\n\n"
        f"O serviço de praia fica {servico.get('localizacao', 'em frente ao Casa Grande Hotel')}."
    )


def get_restaurantes_reply(text):
    text_n = normalize_text(text)
    restaurantes = get_restaurants_data()
    guest = load_guest()
    guest_profile = get_guest_profile(guest)
    context = get_stay_context(guest, text)

    if not restaurantes:
        return "Posso te ajudar com restaurantes 😊 Mas ainda não encontrei opções cadastradas na base neste momento."

    mode = ""

    if has_any(text_n, [
        "happy hour", "drinks", "rooftop",
        "lugar animado", "animado", "ambiente animado",
        "lugar para conversar", "conversar", "mesa para conversar"
    ]):
        mode = "happy hour"
    elif has_any(text_n, ["crianca", "criança", "criancas", "crianças", "kids", "area kids", "área kids", "espaco kids", "espaço kids"]):
        mode = "kids"
    elif has_any(text_n, ["hamburguer", "hambúrguer", "burger", "lanche"]):
        mode = "hamburguer"
    elif has_any(text_n, ["doce", "sobremesa", "chocolate", "kopenhagen", "cacau show"]):
        mode = "doce"
    elif has_any(text_n, ["pizza", "pizzaria"]):
        mode = "pizza"
    elif has_any(text_n, ["japones", "japonês", "sushi"]):
        mode = "japones"
    elif has_any(text_n, ["frutos do mar"]):
        mode = "frutos do mar"
    elif has_any(text_n, ["tradicional", "classico", "clássico"]):
        mode = "tradicional"
    elif has_any(text_n, ["especial", "romantico", "romântico", "sofisticado", "premium", "atmosferico", "atmosférico", "sensorial"]):
        mode = "especial"
    elif has_any(text_n, ["vista", "mirante", "lugar bonito", "lugar com vista"]):
        mode = "vista"
    elif has_any(text_n, ["rapido", "rápido", "barato", "economico", "econômico", "leve", "em conta", "mais em conta", "pratico", "prático"]):
        mode = "rapido"
    elif has_any(text_n, ["todos", "todas", "outros restaurantes", "outro restaurante", "restaurantes"]):
        mode = "todos"

    intro = build_restaurant_concierge_intro(guest_profile, context, mode)

    if mode == "todos":
        ordered = sort_restaurants_for_moment(restaurantes, guest_profile, context, mode)

        set_active_recommendations(
            "restaurantes",
            names_from_items(ordered),
            current_name=ordered[0].get("nome", "") if ordered else ""
        )

        linhas = [build_restaurant_line(r) for r in ordered[:12]]
        return (
            "Claro 😊\n\n"
            + intro
            + "\n\n"
            + "Aqui vão algumas boas referências gastronômicas para este momento:\n\n"
            + "\n".join(linhas)
            + "\n\nSe quiser, eu também posso filtrar isso por **happy hour**, **hambúrguer**, **pizza**, **japonês**, **doce**, **algo tradicional**, **frutos do mar** ou **lugar bom para criança**."
        )

    candidates = unique_restaurants(get_restaurant_candidates_by_mode(restaurantes, mode)) if mode else []

    if mode and candidates:
        ordered = sort_restaurants_for_moment(candidates, guest_profile, context, mode)

        top = ordered[0]
        nome = top.get("nome", "")
        perfil = top.get("perfil", "")
        obs = top.get("observacao", "")
        dist = format_distance(top.get("distancia", ""))

        set_last_entity(nome, "restaurantes")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items(ordered),
            current_name=nome
        )

        intro_map = {
            "rapido": f"Se a ideia for algo mais prático, eu tenderia a começar pelo **{nome}**.",
            "especial": f"Se vocês quiserem algo mais especial, o **{nome}** pode funcionar muito bem ✨",
            "tradicional": f"Se a ideia for algo mais tradicional, uma boa linha é começar pelo **{nome}**.",
            "frutos do mar": f"Se a ideia for **frutos do mar** 🌊\n\nUma boa referência é o **{nome}**.",
            "japones": f"Se a vontade for japonês 🍣\n\nUma boa referência é o **{nome}**.",
            "pizza": f"Se a ideia for pizza 🍕\n\nUma boa pedida é o **{nome}**.",
            "doce": f"Se a ideia for um doce ou chocolate 🍫\n\nUma boa referência é a **{nome}**.",
            "hamburguer": f"Se vocês quiserem hambúrguer 🍔\n\nEu começaria pelo **{nome}**.",
            "kids": f"Se a ideia for um lugar bom para criança e confortável para o grupo 😊\n\nEu começaria pelo **{nome}**.",
            "happy hour": f"Se vocês quiserem happy hour 🍻\n\nUma boa linha é começar pelo **{nome}**.",
            "vista": f"Se a ideia for um lugar com clima ou vista ✨\n\nO **{nome}** pode funcionar muito bem."
        }

        reply = "Claro 😊\n\n" + intro + "\n\n" + intro_map.get(mode, f"Uma boa opção por aqui é o **{nome}**.")

        if dist:
            reply += f"\n\n• Distância: {dist}"
        if top.get("tempo_a_pe"):
            reply += f"\n• A pé: {top.get('tempo_a_pe')}"
        if top.get("tempo_de_carro"):
            reply += f"\n• De carro: {top.get('tempo_de_carro')}"
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"

        kids_context = (mode == "kids") or guest_profile == "familia_com_criancas" or has_any(text_n, ["crianca", "criança", "criancas", "crianças", "kids", "familia", "família"])
        if kids_context:
            kids_line = get_restaurant_kids_highlight(top)
            if kids_line:
                reply += f"\n\n{kids_line}"

        if len(ordered) > 1:
            extras = [f"• **{r.get('nome', '')}**" for r in ordered[1:4]]
            reply += "\n\nOutras opções nessa linha:\n" + "\n".join(extras)

        reply += "\n\n" + pick_place_followup_close(0)
        return reply

    ordered = sort_restaurants_for_moment(restaurantes, guest_profile, context, mode)
    top = ordered[0]
    nome = top.get("nome", "")

    set_last_entity(nome, "restaurantes")
    update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
    set_active_recommendations(
        "restaurantes",
        names_from_items(ordered),
        current_name=nome
    )

    linhas = [build_restaurant_line(r) for r in ordered[:6]]
    closing = pick_place_followup_close(1)
    neutral_hint = ""
    if normalize_profile_value(guest_profile) == "neutro":
        neutral_hint = "\n\nProcurei abrir isso de um jeito mais equilibrado para o momento da estadia, sem te prender só a opções mais rápidas."

    return (
        "Claro 😊\n\n"
        + intro
        + neutral_hint
        + "\n\n"
        + "Estas seriam as sugestões imediatas para começar:\n\n"
        + "\n".join(linhas)
        + "\n\nSe quiser, eu também posso afinar isso por **happy hour**, **hambúrguer**, **pizza**, **japonês**, **doce**, **algo tradicional**, **frutos do mar** ou **lugar bom para criança**."
        + "\n\n" + closing
    )

def get_passeios_reply(text=""):
    text_n = normalize_text(text)
    passeios = get_passeios_data()

    if not passeios:
        return (
            "Posso te ajudar com passeios 😊\n\n"
            "Mas ainda não encontrei opções cadastradas na base neste momento."
        )

    if has_any(text_n, ["chuva", "chovendo", "dia de chuva", "com chuva"]):
        items = filter_passeios_by_ideal(passeios, "chuva")
        if not items:
            items = filter_passeios_by_clima(passeios, "chuva")

        if items:
            linhas = [build_passeio_line(p) for p in items]
            set_active_recommendations(
                "passeio",
                names_from_items(items),
                current_name=items[0].get("nome", "") if items else ""
            )
            return (
                "Se a ideia for algo bom para um dia de chuva ☔\n\n"
                + "\n".join(linhas)
                + "\n\nSe quiser, eu também posso te afinar isso para família, cinema ou shopping."
            )

    if has_any(text_n, ["familia", "família", "crianca", "criança", "criancas", "crianças"]):
        items = filter_passeios_by_ideal(passeios, "familia")
        if not items:
            items = filter_passeios_by_ideal(passeios, "criancas")

        if items:
            linhas = [build_passeio_line(p) for p in items]
            set_active_recommendations(
                "passeio",
                names_from_items(items),
                current_name=items[0].get("nome", "") if items else ""
            )
            return (
                "Se você quiser algo legal para família 😊\n\n"
                + "\n".join(linhas)
                + "\n\nSe quiser, eu também posso separar isso melhor para chuva, fim de tarde ou algo mais leve."
            )

    if has_any(text_n, ["mirante", "vista", "por do sol", "pôr do sol", "foto", "fotos"]):
        items = filter_passeios_by_tipo_or_categoria(passeios, "mirante")
        if items:
            item = items[0]
            set_last_entity(item.get("nome", ""), "passeio")
            set_active_recommendations(
                "passeio",
                names_from_items(items),
                current_name=item.get("nome", "")
            )
            return (
                f"Se a ideia for vista ou mirante ✨\n\n"
                f"Uma ótima referência é o **{item.get('nome', 'Morro do Maluf - Mirante da Campina')}**.\n\n"
                f"{item.get('observacao', 'Boa escolha para fotos e fim de tarde.')}"
            )

    if has_any(text_n, ["cinema"]):
        items = filter_passeios_by_tipo_or_categoria(passeios, "cinema")
        if items:
            item = items[0]
            set_last_entity(item.get("nome", ""), "passeio")
            set_active_recommendations(
                "passeio",
                build_passeio_active_options(
                    item,
                    passeios,
                    preferred_tipos=["shopping", "feira", "mirante", "aquario", "parque"],
                    limit=5
                ),
                current_name=item.get("nome", "")
            )
            return (
                f"Se quiser cinema 🎬\n\n"
                f"Uma boa opção é o **{item.get('nome', 'Cinema Cine Guarujá')}**.\n\n"
                f"{item.get('perfil', item.get('observacao', 'Boa opção para passeio coberto.'))}"
            )

    if has_any(text_n, ["shopping"]):
        items = filter_passeios_by_tipo_or_categoria(passeios, "shopping")
        if items:
            linhas = [build_passeio_line(p) for p in items]
            set_active_recommendations(
                "passeio",
                names_from_items(items),
                current_name=items[0].get("nome", "") if items else ""
            )
            return (
                "Se você quiser shopping 🛍️\n\n"
                + "\n".join(linhas)
                + "\n\nSe quiser, eu também posso te dizer qual faz mais sentido para chuva, família ou combinar com cinema."
            )

    if has_any(text_n, ["feira", "feirinha", "feiras"]):
        items = filter_passeios_by_tipo_or_categoria(passeios, "feira")
        if items:
            item = items[0]
            set_last_entity(item.get("nome", ""), "passeio")
            set_active_recommendations(
                "passeio",
                names_from_items(items),
                current_name=item.get("nome", "")
            )
            return (
                f"Se você quiser algo mais local 😊\n\n"
                f"Uma boa pedida é a **{item.get('nome', 'Feira da Enseada')}**.\n\n"
                f"{item.get('perfil', item.get('observacao', 'Boa opção para passeio leve no fim do dia.'))}"
            )

    if has_any(text_n, ["parque", "ao ar livre", "ar livre", "natureza"]):
        items = filter_passeios_by_tipo_or_categoria(passeios, "parque")
        if items:
            item = items[0]
            set_last_entity(item.get("nome", ""), "passeio")
            set_active_recommendations(
                "passeio",
                names_from_items(items),
                current_name=item.get("nome", "")
            )
            return (
                f"Se a ideia for algo mais ao ar livre 🌿\n\n"
                f"Uma referência legal é o **{item.get('nome', 'Parque Ecológico Renan C. Teixeira')}**.\n\n"
                f"{item.get('perfil', item.get('observacao', 'Boa opção para passeio leve.'))}"
            )

    if has_any(text_n, ["aquario", "aquário", "acqua mundo"]):
        items = filter_passeios_by_tipo_or_categoria(passeios, "aquario")
        if items:
            item = items[0]
            set_last_entity(item.get("nome", ""), "passeio")
            set_active_recommendations(
                "passeio",
                names_from_items(items),
                current_name=item.get("nome", "")
            )
            return (
                f"Uma boa opção por aqui é o **{item.get('nome', 'Acqua Mundo - Aquário Guarujá')}** 😊\n\n"
                f"{item.get('perfil', item.get('observacao', 'Costuma funcionar muito bem para famílias e dias de chuva.'))}"
            )

    linhas = [build_passeio_line(p) for p in passeios[:6]]
    set_active_recommendations(
        "passeio",
        names_from_items(passeios[:6]),
        current_name=passeios[0].get("nome", "") if passeios else ""
    )
    return (
        "Se você quiser passeio por aqui 😊\n\n"
        "Aqui vão algumas boas opções:\n\n"
        + "\n".join(linhas)
        + "\n\nSe quiser, eu também posso filtrar isso para chuva, família, shopping, cinema, mirante ou feira."
    )


def get_mercado_reply(text):
    text_n = normalize_text(text)
    mercados = get_markets_data()

    rapido = find_item_by_type(mercados, "rapido")
    completos = [m for m in mercados if normalize_text(m.get("tipo", "")) == "completo"]

    if has_any(text_n, ["rapido", "rápido", "perto", "urgente", "mercado dia", "supermercado dia"]) or (
        phrase_in_text(text_n, "dia") and not has_any(text_n, ["bom dia"])
    ):
        item = rapido or {}
        nome = item.get("nome", "Mercado Dia")
        dist = format_distance(item.get("distancia", "ao lado"))
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="mercado", last_recommendation_name=nome)
        set_active_recommendations(
            "mercado",
            names_from_items(mercados),
            current_name=nome
        )
        set_last_entity(nome, "mercado")
        reply = f"Se a ideia for resolver algo rápido, eu iria no **{nome}**.\n\n• Distância: {dist}"
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        reply += "\n\nÉ uma escolha bem prática para água, bebida ou alguma compra mais imediata."
        return reply

    if has_any(text_n, ["mercados", "supermercados", "outro mercado", "outros mercados", "outras opcoes", "outras opções", "todos", "todas"]):
        if mercados:
            linhas = []
            for m in mercados:
                nome = m.get("nome", "")
                dist = format_distance(m.get("distancia", ""))
                tipo = normalize_text(m.get("tipo", ""))
                perfil = m.get("perfil", "")
                if tipo == "rapido":
                    linhas.append(f"• **{nome}** → {dist}" + (f" | {perfil}" if perfil else ""))
                else:
                    linhas.append(f"• **{nome}** → cerca de **{dist}**" + (f" | {perfil}" if perfil else ""))

            set_active_recommendations(
                "mercado",
                names_from_items(mercados),
                current_name=mercados[0].get("nome", "") if mercados else ""
            )
            return (
                "Claro 😊\n\n"
                "Aqui vão algumas opções de mercado por perto:\n\n"
                + "\n".join(linhas)
                + "\n\nSe quiser, eu também posso te dizer qual faz mais sentido para algo rápido ou para compra mais completa."
            )

    if has_any(text_n, ["completo", "grande", "variedade"]):
        if completos:
            linhas = []
            for m in completos:
                nome = m.get("nome", "")
                dist = format_distance(m.get("distancia", ""))
                perfil = m.get("perfil", "")
                linhas.append(f"• **{nome}** → cerca de **{dist}**" + (f" | {perfil}" if perfil else ""))

            update_session(last_recommendation_type="mercado", last_recommendation_name=completos[0].get("nome", "Pão de Açúcar - Enseada"))
            set_active_recommendations(
                "mercado",
                names_from_items(mercados),
                current_name=completos[0].get("nome", "") if completos else ""
            )
            set_last_entity(completos[0].get("nome", "Pão de Açúcar - Enseada"), "mercado")
            return (
                "Se você quiser um mercado mais completo, eu olharia para estas opções:\n\n"
                + "\n".join(linhas)
                + "\n\nElas costumam fazer mais sentido quando a ideia é comprar com mais variedade."
            )

    if mercados:
        linhas = []
        for m in mercados:
            nome = m.get("nome", "")
            dist = format_distance(m.get("distancia", ""))
            tipo = normalize_text(m.get("tipo", ""))
            perfil = m.get("perfil", "")
            if tipo == "rapido":
                linhas.append(f"• **{nome}** → {dist}" + (f" | {perfil}" if perfil else ""))
            else:
                linhas.append(f"• **{nome}** → cerca de **{dist}**" + (f" | {perfil}" if perfil else ""))

        set_active_recommendations(
            "mercado",
            names_from_items(mercados),
            current_name=mercados[0].get("nome", "") if mercados else ""
        )
        return (
            "Claro 😊\n\n"
            "Aqui vão boas opções próximas:\n\n"
            + "\n".join(linhas)
            + "\n\nSe quiser, eu também posso resumir qual seria a melhor escolha para o que você precisa agora."
        )

    return "Posso te ajudar com mercados 😊 Se quiser, me diga se você procura algo rápido, completo ou se prefere ver todos."


def get_padaria_reply():
    padaria = knowledge().get("padaria", {})
    perfil = padaria.get("perfil", "")
    obs = padaria.get("observacao", "")
    endereco = padaria.get("endereco", "")
    horario = padaria.get("horario", "")
    telefone = padaria.get("telefone", "")
    almoco = padaria.get("almoco", "")

    if padaria.get("nome"):
        set_last_entity(padaria.get("nome", ""), "padaria")

    reply = (
        "Se você quiser padaria ou café da manhã 😊\n\n"
        f"Uma referência prática é a **{padaria.get('nome', 'Padaria Pitangueiras')}**, a cerca de **{padaria.get('distancia', '300m do apartamento')}**."
    )
    if endereco:
        reply += f"\n• Endereço: {endereco}"
    if horario:
        reply += f"\n• Horário: {horario}"
    if almoco:
        reply += f"\n• Almoço: {almoco}"
    if telefone:
        reply += f"\n• Telefone: {telefone}"
    if perfil:
        reply += f"\n\n{perfil}."
    if obs:
        reply += f"\n\n{obs}"
    return reply


def get_farmacia_reply(text=""):
    text_n = normalize_text(text)
    farmacias = get_farmacias_data()

    if has_any(text_n, ["24h", "vinte e quatro", "urgente", "agora"]):
        candidatas = [f for f in farmacias if has_any(normalize_text(f.get("horario", "")), ["24h"])]
        if candidatas:
            item = candidatas[0]
            nome = item.get("nome", "Drogasil")
            set_last_entity(nome, "farmacia")
            reply = (
                f"Se você estiver precisando de farmácia 24h 😊\n\n"
                f"Uma boa referência é a **{nome}**."
            )
            if item.get("endereco"):
                reply += f"\n• Endereço: {item.get('endereco')}"
            if item.get("horario"):
                reply += f"\n• Horário: {item.get('horario')}"
            if item.get("telefone"):
                reply += f"\n• Telefone: {item.get('telefone')}"
            if item.get("observacao"):
                reply += f"\n\n{item.get('observacao')}"
            reply += "\n\nSe quiser, eu também posso te mostrar outras farmácias da região."
            return reply

    if has_any(text_n, ["entrega", "delivery"]):
        candidatas = [f for f in farmacias if has_any(normalize_text(f.get("observacao", "")), ["entrega", "delivery"])]
        if candidatas:
            linhas = []
            for f in candidatas:
                nome = f.get("nome", "")
                linhas.append(f"• **{nome}**" + (f" → {f.get('observacao')}" if f.get("observacao") else ""))
            return (
                "Claro 😊\n\n"
                "Aqui vão farmácias com indicação de entrega:\n\n"
                + "\n".join(linhas)
                + "\n\n"
                + pick_followup_soft_close("farmacia")
            )

    if has_any(text_n, ["todos", "todas", "outras", "outras farmacias", "outras farmácias", "farmacias", "farmácias"]):
        if farmacias:
            linhas = []
            for f in farmacias:
                nome = f.get("nome", "")
                horario = f.get("horario", "")
                obs = f.get("observacao", "")

                linha = f"• **{nome}**"
                if horario:
                    linha += f" → {horario}"
                if obs:
                    linha += f" | {obs}"
                linhas.append(linha)
            return (
                "Claro 😊\n\n"
                "Aqui vão algumas farmácias da região:\n\n"
                + "\n".join(linhas)
                + "\n\nSe quiser, eu também posso te indicar uma opção 24h ou com entrega."
            )

    if farmacias:
        item = farmacias[0]
        nome = item.get("nome", "Droga Raia")
        set_last_entity(nome, "farmacia")
        reply = (
            "Se você estiver precisando de farmácia 😊\n\n"
            f"Uma referência prática é a **{nome}**"
        )
        if item.get("endereco"):
            reply += f", que fica em **{item.get('endereco')}**"
        if item.get("horario"):
            reply += f" e funciona **{item.get('horario')}**"
        reply += "."
        if item.get("observacao"):
            reply += f"\n\n{item.get('observacao')}"
        reply += "\n\nSe for algo mais urgente ou delicado, eu também posso te orientar para atendimento na região."
        return reply

    return (
        "Se você estiver precisando de farmácia 😊\n\n"
        "Posso te indicar uma opção **24h**, com **entrega** ou te mostrar **todas** as farmácias da região."
    )


def get_apoio_predio_reply():
    apoio = knowledge().get("apoio_predio", {})
    pessoas = apoio.get("pessoas", [])
    fechamento = apoio.get("fechamento", "")
    intro = apoio.get("intro", "Se precisar de apoio no prédio, vocês podem contar com a equipe por aqui 😊")

    if pessoas:
        linhas = []
        for p in pessoas:
            nome = p.get("nome", "")
            funcao = p.get("funcao", "")
            obs = p.get("observacao", "")

            linha = f"• **{nome}**"
            if funcao:
                linha += f" → {funcao}"
            if obs:
                linha += f" ({obs})"
            linhas.append(linha)

        reply = f"{intro}\n\n" + "\n".join(linhas)

        if fechamento:
            reply += f"\n\n{fechamento}"

        return reply

    return (
        "Se precisar de apoio no prédio, vocês podem contar com a equipe por aqui 😊\n\n"
        "O **Paulo**, que é o zelador, pode ajudar, assim como outros funcionários do condomínio, como o **Cláudio** e o **Edson**.\n"
        "O **Edson** fica no período **noturno na portaria**.\n\n"
        "Se quiser, eu também posso te orientar sobre quando vale falar com a portaria, com a equipe do prédio ou comigo por aqui.\n\n"
        "**E, se for necessário, também posso avisar rapidamente o seu anfitrião, Bruno.**"
    )


def get_garagem_reply():
    garagem = knowledge().get("garagem", {})
    info = garagem.get("info", "")
    obs = garagem.get("observacao", "")
    if info:
        return info + (f"\n\n{obs}" if obs else "")
    return (
        "A vaga não é fixa 😊\n\n"
        "Ao chegar, um funcionário do prédio pode te orientar sobre qual utilizar.\n\n"
        "Caso você tenha interesse, por algum motivo, de estacionar em outra vaga além daquela que lhe foi indicada, havendo vagas disponíveis, é só conversar com um funcionário do prédio 👍"
    )


def get_chaves_reply():
    acesso = knowledge().get("acesso", {})
    chaves = acesso.get("chaves", "")
    obs = acesso.get("observacao", "")
    if chaves:
        return chaves + (f"\n\n{obs}" if obs else "")
    return (
        "Você tem a opção, caso queira, de deixar a chave na portaria quando for sair 😊\n\n"
        f"A portaria é {acesso.get('portaria', '24h')} e sempre terá alguém para abrir o portão.\n\n"
        "Mas caso prefiram ficar com elas, o portão social pode ser aberto utilizando a tag magnética presente no seu molho de chaves 🙂"
    )


def get_checkout_reply(guest):
    context = get_stay_context(guest)
    return build_checkout_concierge_line(guest, context)


def get_checkout_aviso_reply(guest):
    context = get_stay_context(guest)
    checkout_time = (context.get("checkout_time") or guest.get("checkout_time") or "11:00").strip() or "11:00"
    checkout_date = parse_guest_date(guest.get("checkout_date", ""))
    window = get_checkout_day_window(context, guest)

    if checkout_date:
        checkout_label = f"**{checkout_time}** do dia **{checkout_date.strftime('%d/%m')}**"
    else:
        checkout_label = f"**{checkout_time}**"

    if window == "corrido":
        intro = "Como a saída pede um pouco mais de objetividade hoje, eu deixaria estes pontos já no radar para tudo seguir leve 😊"
    elif window == "folgado":
        intro = "Como vocês ainda têm uma janela mais folgada hoje, vale só manter estes pontos no radar para o fim da estadia continuar redondo 😊"
    else:
        intro = "Para o fim da estadia seguir leve e sem correria desnecessária, eu deixaria estes pontos no radar 😊"

    return (
        f"{get_gepetto_checkout_line()}\n\n"
        f"{intro}\n\n"
        "• Por gentileza, verificar se as janelas e a porta de entrada ficaram bem travadas\n"
        "• O lixo deve ser retirado antes da saída\n"
        "• Apagar as luzes e desligar os ventiladores na saída\n"
        "• As chaves devem ser devolvidas na portaria do prédio\n"
        "• Pedimos que não deixem louça suja na cozinha; favor lavar antes do checkout\n\n"
        f"O check-out está previsto para {checkout_label}."
    )

def get_bruno_reply():
    set_bruno_pending(True)
    return get_gepetto_bruno_intro()


def notify_bruno_request(guest, raw_message=""):
    nome = guest.get("nome", "").strip() or "Hóspede sem nome definido"
    grupo = guest.get("grupo", "").strip() or "-"
    checkout = guest_checkout_label(guest)
    agora = now_iso()

    msg = (
        "📩 SOLICITAÇÃO DE CONTATO COM O BRUNO\n\n"
        f"Hóspede: {nome}\n"
        f"Grupo: {grupo}\n"
        f"Checkout: {checkout}\n"
        f"Horário: {agora}\n"
    )

    raw_message = (raw_message or "").strip()
    normalized = normalize_text(raw_message)
    if raw_message and normalized not in [
        "envie", "enviar", "manda", "mandar", "mande",
        "pode avisar", "avise", "avisar", "encaminhe", "encaminhar"
    ]:
        msg += f"\nAssunto adiantado pelo hóspede: {raw_message}"
    else:
        msg += "\nAssunto adiantado pelo hóspede: não informado"

    ok, detail = send_telegram_message(msg)
    return ok, detail


def is_airbnb_listing_info_request(text_raw):
    text_n = normalize_text(text_raw)
    return has_any(text_n, [
        "utensilios", "utensílios", "roupas de cama", "roupa de cama",
        "toalhas", "toalha", "enxoval", "enxovais", "amenidades",
        "lencol", "lençol", "lencois", "lençóis",
        "fronha", "fronhas", "edredom", "cobertor", "cobertores"
    ])


def get_airbnb_listing_info_reply():
    return (
        "Para esse tipo de informação, eu sugiro consultar o anúncio e também a sua reserva no Airbnb, onde esses detalhes costumam estar descritos de forma mais completa. O Bruno também está sempre disponível pelo chat do Airbnb e, se preferirem, eu posso avisá-lo agora para entrar em contato com vocês. Se quiser que eu te encaminhe o link do apartamento, é só responder — **'envie o anúncio'**. Se preferir, pode responder — **'falar com Bruno'** 😊"
    )


def get_airbnb_listing_link_reply():
    return (
        "Claro 😊\n\nVocê pode acessar o anúncio do apartamento por aqui:\nhttps://www.airbnb.com.br/rooms/559296170062314034"
    )


def notify_airbnb_listing_info_to_telegram(guest, raw_message):
    nome = guest.get("nome", "").strip() or "Hóspede sem nome definido"
    grupo = guest.get("grupo", "").strip() or "-"
    checkout = guest_checkout_label(guest)
    agora = now_iso()

    msg = (
        "🔎 CONSULTA SOBRE ANÚNCIO / RESERVA AIRBNB\n\n"
        f"Hóspede: {nome}\n"
        f"Grupo: {grupo}\n"
        f"Checkout: {checkout}\n"
        f"Horário: {agora}\n\n"
        f"Mensagem do hóspede: {raw_message}"
    )
    return send_telegram_message(msg)


def get_health_reply(text):
    sev = classify_health(text)
    text_n = normalize_text(text)
    saude = knowledge().get("saude", {})
    upa = saude.get("upa", {})
    hospital = saude.get("hospital", {})

    if sev == "alta":
        return (
            "Isso parece importante ⚠️\n\n"
            "Se for uma situação urgente, priorize atendimento imediato.\n\n"
            "Posso te orientar rapidamente para **UPA**, **hospital** ou **farmácia**."
        )

    if has_any(text_n, ["todos", "todas"]):
        return (
            "Claro 😊\n\n"
            "Aqui vão as opções de apoio à saúde na região:\n\n"
            f"• **farmácia** → opções práticas para medicação e itens básicos\n"
            f"• **{upa.get('nome', 'UPA Enseada')}** → atendimento de urgência mais próximo\n"
            f"• **{hospital.get('nome', 'Hospital Santo Amaro')}** → atendimento hospitalar\n\n"
            "Se quiser, eu posso te detalhar qualquer uma delas."
        )

    return (
        "Entendi 😕\n\n"
        "Se você não estiver se sentindo bem, posso te orientar para:\n"
        "• **farmácia**\n"
        "• **UPA**\n"
        "• **hospital**\n\n"
        "É só me responder com uma dessas opções e eu sigo por aqui 👍\n\n"
        "Se preferir, também pode responder **todos**."
    )


def get_problem_reply(text):
    text_n = normalize_text(text)
    sev = classify_incident(text)

    if has_any(text_n, ["porta nao abre", "porta não abre", "nao entra", "não entra"]):
        return (
            "Entendi ⚠️\n\n"
            "Isso é importante.\n\n"
            "Se vocês estiverem do lado de fora ou sem conseguir acessar, já deixei isso sinalizado por aqui com prioridade.\n\n"
            "Me ajuda só com um detalhe: isso aconteceu **agora** ou vocês **já tinham percebido antes**?"
        )

    if sev == "alta":
        return (
            "Isso parece importante ⚠️\n\n"
            "Se for seguro, se afaste do local ou desligue o equipamento, quando fizer sentido."
        )

    if sev == "media":
        return get_gepetto_incident_ack()

    return (
        "Entendi 👍 Já deixei isso encaminhado por aqui.\n\n"
        "Me conta exatamente o que aconteceu.\n\n"
        "Já estava assim antes ou aconteceu agora?"
    )


def get_eventos_reply():
    return (
        "O Guarujá costuma ter eventos e programações pontuais dependendo da época 😊\n\n"
        "Se quiser, eu posso te sugerir opções mais voltadas para:\n"
        "• passeio tranquilo\n"
        "• família\n"
        "• noite / jantar"
    )


def get_surf_reply():
    surf = knowledge().get("surf", {})
    praias = surf.get("praias", [])

    if praias:
        linhas = []
        for p in praias:
            extra = []
            if p.get("nivel"):
                extra.append(f"nível: {p.get('nivel')}")
            if p.get("observacao"):
                extra.append(p.get("observacao"))
            extra_text = f" ({'; '.join(extra)})" if extra else ""
            linhas.append(f"• **{p.get('nome', '')}** → {p.get('perfil', '')}{extra_text}")

        return (
            "Se você curte surf, posso te ajudar com uma orientação geral sobre os picos mais lembrados por aqui 🌊\n\n"
            + "\n".join(linhas)
            + "\n\nSe quiser, eu também posso te dizer qual combina mais com o seu nível."
        )

    return (
        "Se você curte surf, posso te ajudar com uma orientação geral sobre os picos mais lembrados por aqui 🌊"
    )


def get_bares_reply():
    bares = get_knowledge_list("bares")
    if bares:
        linhas = []
        for b in bares:
            if isinstance(b, str):
                linhas.append(f"• **{b}**")
            else:
                perfil = b.get("perfil", "")
                linhas.append(f"• **{b.get('nome', '')}**" + (f" → {perfil}" if perfil else ""))
    else:
        linhas = ["• **Quiosques da Orla da Enseada**"]

    return (
        "Se a ideia for sair à noite 🍻\n\n"
        "Encontrei alguns estabelecimentos próximos ao apartamento:\n\n"
        + "\n".join(linhas)
        + "\n\nSe quiser algo mais tranquilo ou mais animado, posso te direcionar melhor 😉"
    )


def get_shopping_reply():
    return get_passeios_reply("shopping")


def get_feira_reply():
    return get_passeios_reply("feira")


def get_tempo_reply():
    return get_weather_reply()


def get_roteiro_reply(guest, style_override=""):
    context = get_stay_context(guest)
    parte_do_dia = current_time_label()
    perfil = get_guest_profile(guest)
    moment = get_stay_restaurant_moment(context)
    style = normalize_text(style_override or get_roteiro_style_label(style_override, guest))
    passeios = get_passeios_data()

    aquario = best_closest_item(filter_passeios_by_tipo_or_categoria(passeios, "aquario"))
    shopping_items = filter_passeios_by_tipo_or_categoria(passeios, "shopping")
    shopping = shopping_items[0] if shopping_items else None
    cinema = best_closest_item(filter_passeios_by_tipo_or_categoria(passeios, "cinema"))
    feira = best_closest_item(filter_passeios_by_tipo_or_categoria(passeios, "feira"))
    mirante = best_closest_item(filter_passeios_by_tipo_or_categoria(passeios, "mirante"))
    burgman = find_item_by_name(get_restaurants_data(), "Boteco Burgman Enseada")

    def style_close(default_text):
        if style == "casal":
            return default_text + "\n\nSe quiser, eu também posso puxar isso para algo mais romântico ou mais especial 😊"
        if style == "familia":
            return default_text + "\n\nSe quiser, eu também posso puxar isso para algo mais confortável para família 😊"
        if style == "grupo":
            return default_text + "\n\nSe quiser, eu também posso puxar isso para algo mais social e leve para grupo 😊"
        return default_text + "\n\nSe quiser, eu também posso puxar isso para algo mais casal, família ou grupo."

    if moment == "primeira_noite":
        if style == "casal" or perfil == "casal":
            parts = [
                "Como é a primeira noite, eu tenderia a começar por algo gostoso, acolhedor e com um pouco mais de clima 😊",
                "",
                "• um jantar agradável para entrar no ritmo da estadia"
            ]
            if mirante:
                parts.append(f"• e, se quiserem algo leve depois, o **{mirante.get('nome', 'Morro do Maluf - Mirante da Campina')}** pode funcionar bem")
            return "\n".join(parts + ["", "Se quiser, eu também posso te indicar agora um lugar que combine mais com essa primeira noite."])

        if style == "familia" or perfil == "familia_com_criancas":
            parts = [
                "Para a primeira noite, eu tenderia a manter tudo mais redondo e confortável para o grupo 😊",
                "",
                "• um jantar fácil de encaixar"
            ]
            if aquario:
                parts.append(f"• e, em outro momento da estadia, o **{aquario.get('nome', 'Acqua Mundo')}** costuma funcionar bem para família")
            return "\n".join(parts + ["", "Se quiser, eu posso te apontar agora uma opção que combine melhor com esse começo de estadia."])

        if style == "grupo" or perfil in ["grupo", "amigos"]:
            parts = [
                "Como é a primeira noite, eu tenderia a começar por algo leve de encaixar, mas já com um pouco de clima social 😊",
                "",
                "• um jantar gostoso sem complicar a chegada"
            ]
            if feira:
                parts.append(f"• e a **{feira.get('nome', 'Feira da Enseada')}** pode entrar depois como passeio leve")
            return "\n".join(parts + ["", "Se quiser, eu também posso transformar isso numa sugestão mais objetiva para grupo."])

        parts = [
            "Como é a primeira noite, eu tenderia a começar por algo gostoso, simples de encaixar e sem muita fricção 😊",
            "",
            "• um jantar que funcione bem para o ritmo de chegada"
        ]
        if feira:
            parts.append(f"• e, se vocês quiserem sair um pouco depois, a **{feira.get('nome', 'Feira da Enseada')}** pode entrar como passeio leve")
        return "\n".join(parts + ["", "Se quiser, eu também posso transformar isso numa indicação mais objetiva agora."])

    if moment == "checkout_corrido":
        parts = [
            "Como hoje pede um ritmo mais corrido, eu tenderia a deixar o plano mais leve e funcional — sem perder o clima bom da estadia 😊",
            "",
            "• algo prático para comer"
        ]
        if feira:
            parts.append(f"• uma passada rápida pela **{feira.get('nome', 'Feira da Enseada')}**, se fizer sentido para vocês")
        parts.extend(["• e o restante do dia mais alinhado com a organização da saída", "", "Se quiser, eu posso te sugerir algo para comer que combine bem com esse momento."])
        return "\n".join(parts)

    if moment == "checkout_folgado":
        parts = [
            "Como vocês ainda têm uma janela boa para aproveitar o dia, dá para pensar num roteiro mais gostoso, sem sensação de pressa 😊",
            "",
            "• praia ou passeio leve em algum momento do dia"
        ]
        if mirante:
            parts.append(f"• o **{mirante.get('nome', 'Morro do Maluf - Mirante da Campina')}** pode funcionar bem para vista ou fim de tarde")
        if burgman and (style == "grupo" or perfil in ["grupo", "amigos"]):
            parts.append(f"• e até o **{burgman.get('nome', 'Boteco Burgman Enseada')}** pode entrar como opção de boliche / entretenimento mais tarde")
        elif cinema or shopping:
            parts.append(f"• um passeio coberto como **{(cinema or shopping).get('nome', 'Shopping Enseada')}** também pode funcionar bem")
        parts.extend(["• e algo bom para comer depois", "", "Se quiser, eu posso afinar isso agora para algo mais leve, mais social ou mais especial."])
        return "\n".join(parts)

    if moment == "checkout_intermediario":
        parts = [
            "Hoje ainda dá para aproveitar o dia com alguma folga, só valendo manter um plano mais redondo 😊",
            "",
            "• praia ou passeio leve, se fizer sentido"
        ]
        if cinema or shopping:
            parts.append(f"• um passeio coberto como o **{(cinema or shopping).get('nome', 'Shopping Enseada')}** também pode funcionar bem")
        parts.extend(["• e uma refeição agradável sem alongar demais a logística", "", "Se quiser, eu posso te dizer o que eu encaixaria melhor neste contexto."])
        return "\n".join(parts)

    if moment == "vespera_saida":
        parts = [
            "Como vocês já entram na reta final da hospedagem, eu tenderia a pensar em algo gostoso, mas ainda confortável de encaixar 😊",
            "",
            "• praia ou passeio leve durante o dia"
        ]
        if feira:
            parts.append(f"• a **{feira.get('nome', 'Feira da Enseada')}** pode entrar bem no fim do dia")
        if mirante:
            parts.append(f"• o **{mirante.get('nome', 'Morro do Maluf - Mirante da Campina')}** também pode funcionar como saída leve")
        parts.extend(["• e, mais tarde, um jantar agradável para fechar bem a estadia", "", "Se quiser, eu também posso afinar isso para algo mais especial, mais prático ou mais família."])
        return "\n".join(parts)

    if parte_do_dia == "manhã":
        if style == "familia" or perfil == "familia_com_criancas":
            parts = [
                "Se eu fosse montar um roteiro leve para hoje 😊",
                "",
                "☀️ **Manhã**",
                "• praia, se o tempo estiver ajudando"
            ]
            if aquario:
                parts.append(f"• ou o **{aquario.get('nome', 'Acqua Mundo')}** se a ideia for algo coberto e bom para família")
            parts.extend(["", "🍽️ **Depois**", "• um almoço confortável e fácil de encaixar", "", "Se quiser, eu transformo isso numa sugestão mais objetiva agora."])
            return "\n".join(parts)

        if style == "casal" or perfil == "casal":
            parts = [
                "Se eu fosse puxar isso para algo mais casal hoje 😊",
                "",
                "☀️ **Manhã**",
                "• praia, se o ritmo de vocês pedir algo mais leve"
            ]
            if mirante:
                parts.append(f"• ou, mais tarde, o **{mirante.get('nome', 'Morro do Maluf - Mirante da Campina')}** pode entrar bem para vista e fim de tarde")
            parts.extend(["", "🍽️ **Depois**", "• um almoço ou jantar com mais clima", "", "Se quiser, eu também posso traduzir isso numa indicação mais objetiva."])
            return "\n".join(parts)

        if style == "grupo" or perfil in ["grupo", "amigos"]:
            parts = [
                "Se eu fosse puxar isso para algo mais grupo hoje 😊",
                "",
                "☀️ **Manhã / começo do dia**",
                "• praia ou um começo mais tranquilo"
            ]
            if feira:
                parts.append(f"• e, mais tarde, a **{feira.get('nome', 'Feira da Enseada')}** pode entrar bem como passeio leve")
            parts.extend(["", "🍽️ **Depois**", "• um lugar gostoso para comer e manter o clima social", "", "Se quiser, eu também posso te direcionar nisso agora."])
            return "\n".join(parts)

        parts = [
            "Se eu fosse montar um roteiro leve para hoje 😊",
            "",
            "☀️ **Manhã**",
            "• praia ou um começo de dia mais tranquilo"
        ]
        if aquario:
            parts.append(f"• se quiser algo coberto depois, o **{aquario.get('nome', 'Acqua Mundo')}** pode entrar bem")
        parts.extend(["", "🍽️ **Depois**", "• almoço e uma programação mais leve para a tarde"])
        return style_close("\n".join(parts))

    if parte_do_dia == "tarde":
        parts = [
            "Se quiser um roteiro para o restante do dia 😄",
            "",
            "🌤️ **Agora à tarde**",
            "• praia, descanso ou uma saída leve pela região"
        ]
        if mirante:
            parts.append(f"• o **{mirante.get('nome', 'Morro do Maluf - Mirante da Campina')}** pode funcionar bem para vista e fim de tarde")
        if feira:
            parts.append(f"• a **{feira.get('nome', 'Feira da Enseada')}** entra bem como passeio leve mais para o fim do dia")
        if cinema or shopping:
            parts.append(f"• e um programa coberto como **{(cinema or shopping).get('nome', 'Shopping Enseada')}** também pode funcionar, se fizer mais sentido")
        parts.extend(["", "🍽️ **Mais tarde**", "• um jantar agradável ou algo mais social, se fizer sentido"])
        return style_close("\n".join(parts))

    options = []
    if feira:
        options.append(f"• **{feira.get('nome', 'Feira da Enseada')}** para um passeio leve")
    if cinema:
        options.append(f"• **{cinema.get('nome', 'Cinema Cine Guarujá')}** se a ideia for algo coberto")
    elif shopping:
        options.append(f"• **{shopping.get('nome', 'Shopping Enseada')}** se vocês quiserem algo coberto")
    if burgman and (style == "grupo" or perfil in ["amigos", "grupo"]):
        options.append(f"• **{burgman.get('nome', 'Boteco Burgman Enseada')}** se a ideia for algo mais animado com boliche")

    parts = ["Se quiser um plano bom para agora à noite ✨", ""]
    parts.extend(options)
    return style_close("\n".join(parts))


def get_followup_reply(text, last_topic, guest):
    text_n = normalize_text(text)
    topic = infer_contextual_followup(text, last_topic)
    session = load_session()
    last_rec_name = session.get("last_recommendation_name", "")

    if last_topic == "airbnb_info" or topic == "airbnb_info":
        if has_any(text_n, ["envie o anuncio", "envie anuncio", "anuncio", "anúncio"]):
            return get_airbnb_listing_link_reply()
        if has_any(text_n, ["falar com bruno"]):
            ok, _ = notify_bruno_request(guest, "Pedido de contato após orientação para consultar anúncio/reserva do Airbnb")
            if ok:
                return "Perfeito 😊 Já avisei o Bruno para entrar em contato com vocês o quanto antes."
            return "Entendi 😊 Tentei avisar o Bruno agora, mas não consegui enviar a solicitação de acompanhamento neste momento."

    if last_topic == "praia" or topic == "praia":
        if has_any(text_n, [
            "onde fica", "localizacao", "localização",
            "endereco", "endereço", "e o endereco", "e o endereço"
        ]):
            return get_servico_praia_localizacao_reply()

        if has_any(text_n, [
            "horario", "horário", "horarios", "horários",
            "que horas", "que horas funciona",
            "funciona que horas", "ate que horas", "até que horas"
        ]):
            servico = knowledge().get("praia", {}).get("servico_praia", {})
            return f"Claro 😊\n\nO serviço de praia funciona das **{servico.get('horario', '9h às 17h')}**."

        if has_any(text_n, [
            "como funciona", "funciona", "servico", "serviço"
        ]):
            servico = knowledge().get("praia", {}).get("servico_praia", {})
            return f"Claro 😊\n\n{servico.get('como_funciona', 'Os itens ficam montados na areia durante o horário do serviço.')}"

        temporal_reply = get_praia_temporal_followup_reply(guest, text)
        if temporal_reply:
            return temporal_reply

    if topic == "restaurantes":
        restaurantes = get_restaurants_data()
        active_current = get_current_active_recommendation("restaurantes")
        active_next = get_next_active_recommendation("restaurantes", advance=False)

        if has_any(text_n, ["todos", "todas"]):
            return get_restaurantes_reply("todos")

        if has_any(text_n, ["outro restaurante", "outros restaurantes", "restaurantes"]):
            return get_restaurantes_reply("restaurantes")

        if has_any(text_n, ["esse", "essa", "esse ai", "esse aí", "essa ai", "essa aí", "vou nesse", "vou nessa", "manda esse", "manda essa"]):
            chosen = active_current or last_rec_name
            if chosen:
                set_last_entity(chosen, "restaurantes")
                set_current_active_recommendation_by_name(chosen, "restaurantes")
                update_session(last_recommendation_type="restaurantes", last_recommendation_name=chosen)
                return (
                    f"{pick_confirmation_intro()}\n\n"
                    f"{pick_recommendation_intro('restaurantes')}\n\n"
                    f"Eu iria de **{chosen}**."
                )

        if has_any(text_n, ["o outro", "a outra", "outro", "outra", "tem outro", "tem outra"]):
            alt_name = active_next
            alt = find_item_by_name(restaurantes, alt_name) if alt_name else None

            if alt:
                nome = alt.get("nome", "")
                set_last_entity(nome, "restaurantes")
                set_current_active_recommendation_by_name(nome, "restaurantes")
                update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)

                reply = (
                    f"{pick_alternative_intro()}\n\n"
                    f"Se você quiser variar um pouco, uma outra boa opção é o **{nome}**."
                )
                if alt.get("perfil"):
                    reply += f"\n\n{alt.get('perfil')}."
                if alt.get("observacao"):
                    reply += f"\n\n{alt.get('observacao')}"
                return reply

        if has_any(text_n, ["qual deles", "qual delas", "entre eles", "entre elas"]):
            active = get_active_recommendations()
            if normalize_text(active.get("type", "")) == "restaurantes" and active.get("options"):
                nomes = active["options"][:3]
                linhas = []

                for nome in nomes:
                    item = find_item_by_name(restaurantes, nome)
                    if item:
                        perfil = item.get("perfil", "")
                        if perfil:
                            linhas.append(f"• **{nome}** → {perfil}")
                        else:
                            linhas.append(f"• **{nome}**")
                    else:
                        linhas.append(f"• **{nome}**")

                return (
                    f"{pick_comparison_intro()}\n\n"
                    "Entre essas opções, eu resumiria assim:\n\n"
                    + "\n".join(linhas)
                    + f"\n\n{pick_followup_soft_close('restaurantes')}"
                )

        if has_any(text_n, ["mais perto", "perto"]):
            item = best_closest_item(restaurantes)
            if item:
                nome = item.get("nome", "")
                set_last_entity(nome, "restaurantes")
                set_current_active_recommendation_by_name(nome, "restaurantes")
                update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
                reply = f"Se a prioridade for proximidade, eu iria no **{nome}**"
                if item.get("distancia"):
                    reply += f", que fica a cerca de **{format_distance(item.get('distancia', ''))}**"
                reply += "."
                if item.get("perfil"):
                    reply += f"\n\n{item.get('perfil')}."
                reply += "\n\nSe quiser, eu também posso te dizer qual eu escolheria pelo custo-benefício."
                return reply

        if has_any(text_n, ["mais barato", "barato", "economico", "econômico", "leve", "em conta", "mais em conta", "rapido", "rápido"]):
            return get_restaurantes_reply("rapido")

        if has_any(text_n, ["mais especial", "especial", "romantico", "romântico", "sofisticado"]):
            return get_restaurantes_reply("especial")

        if has_any(text_n, ["frutos do mar"]):
            return get_restaurantes_reply("frutos do mar")

        if has_any(text_n, ["tradicional", "classico", "clássico"]):
            return get_restaurantes_reply("tradicional")

        if has_any(text_n, ["pizza", "pizzaria"]):
            return get_restaurantes_reply("pizza")

        if has_any(text_n, ["japones", "japonês", "sushi"]):
            return get_restaurantes_reply("japones")

        if has_any(text_n, ["doce", "sobremesa", "chocolate"]):
            return get_restaurantes_reply("doce")

        if has_any(text_n, ["vista", "mirante", "lugar bonito"]):
            return get_restaurantes_reply("vista")

        if has_any(text_n, [
            "happy hour", "drinks", "rooftop",
            "lugar animado", "animado", "ambiente animado",
            "lugar para conversar", "conversar", "mesa para conversar"
        ]):
            return get_restaurantes_reply("happy hour")

        if has_any(text_n, ["crianca", "criança", "criancas", "crianças", "kids", "area kids", "área kids", "espaco kids", "espaço kids", "familia", "família"]):
            return get_restaurantes_reply("kids")

        if has_any(text_n, ["hamburguer", "hambúrguer", "burger", "lanche"]):
            return get_restaurantes_reply("hamburguer")

        if has_any(text_n, ["qual melhor", "qual voce indica", "qual você indica", "qual vc indica", "qual voce recomenda", "qual você recomenda", "qual vc recomenda", "compensa", "vale a pena"]):
            active = get_active_recommendations()
            if normalize_text(active.get("type", "")) == "restaurantes" and active.get("options"):
                current = get_current_active_recommendation("restaurantes")
                if current:
                    return (
                        f"{pick_recommendation_intro('restaurantes')}\n\n"
                        f"Eu começaria pelo **{current}**."
                    )

            return (
                "Se eu tivesse que te direcionar sem erro 😊\n\n"
                "• **Thai Lounge Bar** → se você quiser algo mais especial\n"
                "• **Alcide’s** → se quiser algo clássico e tradicional\n"
                "• **Sushi Katoshi 23** → se estiver com vontade de japonês 🍣\n"
                "• **Alcides Pizzaria** → se a ideia for pizza 🍕\n"
                "• **McDonald's Enseada** → se a ideia for praticidade"
            )

        if text_n in ["esse", "essa", "pode ser", "quero esse", "quero essa"] and last_rec_name:
            set_last_entity(last_rec_name, "restaurantes")
            set_current_active_recommendation_by_name(last_rec_name, "restaurantes")
            return (
                f"{pick_confirmation_intro()}\n\n"
                f"{pick_recommendation_intro('restaurantes')}\n\n"
                f"Eu iria de **{last_rec_name}**."
            )

    if topic == "mercado":
        mercados = get_markets_data()
        active_current = get_current_active_recommendation("mercado")
        active_next = get_next_active_recommendation("mercado", advance=False)

        if has_any(text_n, ["todos", "todas"]):
            return get_mercado_reply("todos")

        if has_any(text_n, ["outro mercado", "outros mercados", "outras opcoes", "outras opções", "supermercados", "mercados"]):
            return get_mercado_reply("mercados")

        if has_any(text_n, ["esse", "essa", "esse ai", "esse aí", "essa ai", "essa aí", "vou nesse", "vou nessa", "manda esse", "manda essa"]):
            chosen = active_current or last_rec_name
            if chosen:
                set_last_entity(chosen, "mercado")
                set_current_active_recommendation_by_name(chosen, "mercado")
                update_session(last_recommendation_type="mercado", last_recommendation_name=chosen)
                return (
                    f"{pick_confirmation_intro()}\n\n"
                    f"{pick_recommendation_intro('mercado')}\n\n"
                    f"Eu iria no **{chosen}**."
                )

        if has_any(text_n, ["o outro", "a outra", "outro", "outra", "tem outro", "tem outra"]):
            alt_name = active_next
            alt = find_item_by_name(mercados, alt_name) if alt_name else None

            if alt:
                nome = alt.get("nome", "")
                set_last_entity(nome, "mercado")
                set_current_active_recommendation_by_name(nome, "mercado")
                update_session(last_recommendation_type="mercado", last_recommendation_name=nome)

                reply = (
                    f"{pick_alternative_intro()}\n\n"
                    f"Se você quiser outra alternativa, uma boa opção é o **{nome}**."
                )
                if alt.get("distancia"):
                    reply += f"\n• Distância: {format_distance(alt.get('distancia', ''))}"
                if alt.get("perfil"):
                    reply += f"\n\n{alt.get('perfil')}."
                if alt.get("observacao"):
                    reply += f"\n\n{alt.get('observacao')}"
                return reply

        if has_any(text_n, ["qual deles", "qual delas", "entre eles", "entre elas"]):
            active = get_active_recommendations()
            if normalize_text(active.get("type", "")) == "mercado" and active.get("options"):
                nomes = active["options"][:3]
                linhas = []

                for nome in nomes:
                    item = find_item_by_name(mercados, nome)
                    if item:
                        perfil = item.get("perfil", "")
                        dist = format_distance(item.get("distancia", ""))
                        linha = f"• **{nome}**"
                        if dist:
                            linha += f" → {dist}"
                        if perfil:
                            linha += f" | {perfil}"
                        linhas.append(linha)
                    else:
                        linhas.append(f"• **{nome}**")

                return (
                    "Depende do tipo de compra que você quer fazer 😊\n\n"
                    "Eu resumiria assim:\n\n"
                    + "\n".join(linhas)
                    + f"\n\n{pick_followup_soft_close('mercado')}"
                )

        if has_any(text_n, ["mais completo", "completo", "grande", "variedade"]):
            return get_mercado_reply("completo")

        if has_any(text_n, ["mais perto", "perto", "rapido", "rápido"]):
            item = best_closest_item(mercados)
            if item:
                nome = item.get("nome", "")
                set_last_entity(nome, "mercado")
                set_current_active_recommendation_by_name(nome, "mercado")
                update_session(last_recommendation_type="mercado", last_recommendation_name=nome)
                reply = f"Se a prioridade for praticidade, eu iria no **{nome}**"
                if item.get("distancia"):
                    reply += f", que fica **{format_distance(item.get('distancia', ''))}**"
                reply += "."
                if item.get("perfil"):
                    reply += f"\n\n{item.get('perfil')}."
                reply += "\n\nÉ a melhor opção para resolver algo rápido."
                return reply
            return get_mercado_reply("rapido")

        if has_any(text_n, ["qual melhor", "qual voce recomenda", "qual você recomenda", "qual vc recomenda", "compensa"]):
            current = get_current_active_recommendation("mercado")
            if current:
                return (
                    f"{pick_recommendation_intro('mercado')}\n\n"
                    f"Eu começaria pelo **{current}**."
                )
            return (
                "Depende do que você precisa 😊\n\n"
                "• **Mercado Dia** → se quiser algo rápido\n"
                "• **Pão de Açúcar - Enseada** → se quiser algo mais organizado e confortável\n"
                "• **Extra / Carrefour** → se a ideia for compra mais completa"
            )

    if topic == "saude":
        if has_any(text_n, ["todos", "todas"]):
            return get_health_reply("todos")
        if has_any(text_n, ["farmacia", "farmácia", "farmacias", "farmácias"]):
            return get_farmacia_reply("farmacia")
        if has_any(text_n, ["entrega", "delivery", "24h", "vinte e quatro", "urgente", "agora"]):
            return get_farmacia_reply(text)
        if has_any(text_n, ["upa"]):
            return get_localizacao_reply("upa")
        if has_any(text_n, ["hospital"]):
            return get_localizacao_reply("hospital")
        return get_health_reply(text)

    if topic == "farmacia":
        if has_any(text_n, ["todos", "todas", "outras", "outras farmacias", "outras farmácias"]):
            return get_farmacia_reply("todos")
        if has_any(text_n, ["24h", "vinte e quatro", "urgente", "agora"]):
            return get_farmacia_reply("24h")
        if has_any(text_n, ["entrega", "delivery"]):
            return get_farmacia_reply("entrega")
        return get_farmacia_reply(text)

    if topic == "incidente":
        return get_problem_reply(text)

    if topic == "roteiro" or last_topic == "roteiro":
        if has_any(text_n, ["casal", "familia", "família", "grupo", "amigos", "galera", "pessoal"]):
            style = get_roteiro_style_label(text, guest)
            if style:
                return get_roteiro_reply(guest, style_override=style)

    if topic == "passeio":
        passeios = get_passeios_data()
        active_current = get_current_active_recommendation("passeio")
        active_next = get_next_active_recommendation("passeio", advance=False)

        if has_any(text_n, ["chuva", "chovendo", "dia de chuva"]):
            return get_passeios_reply("chuva")

        if has_any(text_n, ["familia", "família", "crianca", "criança", "criancas", "crianças"]):
            return get_passeios_reply("familia")

        if has_any(text_n, ["shopping"]):
            return get_passeios_reply("shopping")

        if has_any(text_n, ["cinema"]):
            return get_passeios_reply("cinema")

        if has_any(text_n, ["mirante", "vista", "por do sol", "pôr do sol"]):
            return get_passeios_reply("mirante")

        if has_any(text_n, ["feira", "feirinha"]):
            return get_passeios_reply("feira")

        if has_any(text_n, ["esse", "essa", "esse ai", "esse aí", "essa ai", "essa aí", "vou nesse", "vou nessa", "manda esse", "manda essa"]):
            chosen = active_current or session.get("last_entity_name", "")
            if chosen:
                set_last_entity(chosen, "passeio")
                set_current_active_recommendation_by_name(chosen, "passeio")
                return (
                    f"{pick_confirmation_intro()}\n\n"
                    f"{pick_recommendation_intro('passeio')}\n\n"
                    f"**{chosen}** pode ser uma ótima escolha."
                )

        if has_any(text_n, ["o outro", "a outra", "outro", "outra", "tem outro", "tem outra"]):
            alt_name = active_next
            alt = find_item_by_name(passeios, alt_name) if alt_name else None

            if alt:
                nome = alt.get("nome", "")
                set_last_entity(nome, "passeio")
                set_current_active_recommendation_by_name(nome, "passeio")
                return (
                    f"{pick_alternative_intro()}\n\n"
                    f"Se você quiser variar o passeio, uma outra boa opção é **{nome}**.\n\n"
                    f"{alt.get('perfil', alt.get('observacao', 'Pode ser uma boa alternativa por aqui.'))}"
                )

        if has_any(text_n, ["qual deles", "qual delas", "entre eles", "entre elas"]):
            active = get_active_recommendations()
            if normalize_text(active.get("type", "")) == "passeio" and active.get("options"):
                nomes = active["options"][:3]
                linhas = []

                for nome in nomes:
                    item = find_item_by_name(passeios, nome)
                    if item:
                        perfil = item.get("perfil", item.get("observacao", ""))
                        if perfil:
                            linhas.append(f"• **{nome}** → {perfil}")
                        else:
                            linhas.append(f"• **{nome}**")
                    else:
                        linhas.append(f"• **{nome}**")

                return (
                    "Depende bastante do clima e do tipo de passeio que você quer 😊\n\n"
                    "Eu resumiria assim:\n\n"
                    + "\n".join(linhas)
                    + f"\n\n{pick_followup_soft_close('passeio')}"
                )

    if topic == "tempo":
        if has_any(text_n, ["e pra praia", "compensa", "vale a pena", "e hoje"]):
            return f"{get_weather_reply()}\n\nSe quiser, eu também posso te sugerir se hoje faz mais sentido praia, passeio ou algo coberto 😉"

    if topic == "apoio_predio":
        if has_any(text_n, [
            "quem contactar no predio", "quem contactar no prédio",
            "com quem falar no predio", "com quem falar no prédio",
            "contato no predio", "contato no prédio",
            "ajuda no condominio", "ajuda no condomínio",
            "ajuda no predio", "ajuda no prédio"
        ]):
            return get_apoio_predio_reply()

    if topic == "bruno":
        if has_any(text_n, [
            "envie", "enviar", "manda", "mandar", "mande",
            "pode mandar", "pode avisar", "avise", "avisar",
            "encaminhe", "encaminhar"
        ]):
            ok, _ = notify_bruno_request(guest, "")
            set_bruno_pending(False)
            if ok:
                return "Perfeito 😊 Já enviei uma solicitação de acompanhamento ao Bruno. Ele entrará em contato com você o quanto antes."
            return "Entendi 😊 Tentei avisar o Bruno agora, mas não consegui enviar a solicitação de acompanhamento neste momento."

        incident_like = is_incident_like_message(text)
        if incident_like:
            sev = classify_incident(text)
            incident_ok, _ = maybe_notify("incidente", text, guest, sev)
            bruno_ok, _ = notify_bruno_request(guest, text)
            set_bruno_pending(False)

            if incident_ok and bruno_ok:
                return "Entendi 😊 Já deixei isso sinalizado por aqui e também enviei uma solicitação de acompanhamento ao Bruno. Ele entrará em contato com você o quanto antes."
            if incident_ok and not bruno_ok:
                return "Entendi 😊 Já deixei isso sinalizado por aqui, mas não consegui enviar a solicitação de acompanhamento ao Bruno neste momento."
            if bruno_ok and not incident_ok:
                return "Entendi 😊 Já avisei o Bruno com esse assunto. Ele entrará em contato com você o quanto antes."
            return "Entendi 😊 Registrei o assunto por aqui, mas não consegui enviar a solicitação ao Bruno neste momento."

        ok, _ = notify_bruno_request(guest, text)
        set_bruno_pending(False)
        if ok:
            return "Perfeito 😊 Já avisei o Bruno e adiantei esse assunto para ele. Ele entrará em contato com você o quanto antes."
        return "Entendi 😊 Tentei avisar o Bruno agora, mas não consegui enviar a solicitação de acompanhamento neste momento."

    return ""


def get_guided_reply(intent):
    if intent == "restaurantes":
        guest = load_guest()
        profile = get_guest_profile(guest)
        restaurantes = get_restaurants_data()

        intro = build_profile_opening_line(profile)

        if restaurantes:
            ordered = sort_restaurants_for_profile(restaurantes, profile)
            ordered = [r for r in ordered if isinstance(r, dict) and r.get("nome")]

            if ordered:
                current_name = ordered[0].get("nome", "")
                set_active_recommendations(
                    "restaurantes",
                    names_from_items(ordered),
                    current_name=current_name
                )

                if current_name:
                    set_last_entity(current_name, "restaurantes")
                    update_session(
                        last_recommendation_type="restaurantes",
                        last_recommendation_name=current_name
                    )

                top_1 = ordered[0]
                top_2 = ordered[1] if len(ordered) > 1 else None

                sugestoes = []

                line_1 = f"• **{top_1.get('nome', '')}**"
                if top_1.get("perfil"):
                    line_1 += f" → {top_1.get('perfil')}"
                sugestoes.append(line_1)

                if top_2:
                    line_2 = f"• **{top_2.get('nome', '')}**"
                    if top_2.get("perfil"):
                        line_2 += f" → {top_2.get('perfil')}"
                    sugestoes.append(line_2)

                return (
                    "Claro 😊\n\n"
                    f"{intro}\n\n"
                    "Se eu fosse começar sem complicar, eu olharia primeiro para estas opções:\n\n"
                    + "\n".join(sugestoes)
                    + "\n\n"
                    "Se quiser, eu também posso refinar agora por estilo:\n"
                    "• mais **rápido**\n"
                    "• mais **especial**\n"
                    "• mais **tradicional**\n"
                    "• **frutos do mar**\n"
                    "• **japonês**\n"
                    "• **pizza**\n"
                    "• **hambúrguer**\n"
                    "• **happy hour**\n"
                    "• lugar bom para **criança**\n"
                    "• **doce**\n"
                    "• ou **todos**"
                )

        return (
            "Claro 😊\n\n"
            f"{intro}\n\n"
            "Pra eu te direcionar melhor, me diga o estilo que faria mais sentido agora:\n"
            "• mais **rápido**\n"
            "• mais **especial**\n"
            "• mais **tradicional**\n"
            "• **frutos do mar**\n"
            "• **japonês**\n"
            "• **pizza**\n"
            "• **hambúrguer**\n"
            "• **happy hour**\n"
            "• lugar bom para **criança**\n"
            "• **doce**\n"
            "• ou **todos**"
        )

    if intent == "mercado":
        return (
            "Claro 😊\n\n"
            "Me diga só o que faria mais sentido agora:\n"
            "• algo **rápido**\n"
            "• um mercado mais **completo**\n"
            "• ou **todos**"
        )

    if intent == "saude":
        return (
            "Entendi 😕\n\n"
            "Posso te orientar agora para:\n"
            "• **farmácia**\n"
            "• **UPA**\n"
            "• **hospital**\n"
            "• ou **todos**"
        )

    if intent == "farmacia":
        return (
            "Claro 😊\n\n"
            "Você quer que eu te mostre:\n"
            "• uma opção **24h**\n"
            "• com **entrega**\n"
            "• ou **todas**"
        )

    if intent == "praia":
        return get_gepetto_praia_line() + "\n\n" + (
            "Se quiser, eu te digo:\n"
            "• a **localização**\n"
            "• o **horário**\n"
            "• ou como funciona o **serviço de praia**"
        )

    if intent == "bares":
        return (
            "Boa 😊\n\n"
            "Você quer algo mais:\n"
            "• **animado**\n"
            "• ou mais **tranquilo**?"
        )

    return ""
    

def get_fallback_reply(guest):
    last_msgs = get_recent_messages(5)
    fallback_count = sum(1 for m in last_msgs if m.get("topic") == "fallback")

    if fallback_count >= 2:
        if guest_language(guest) == "en":
            return (
                "Sorry 😅\n\n"
                "I am still in beta tests and I didn't fully understand your question.\n\n"
                "If you can rephrase it, I will try to help you better 🙏"
            )
        return (
            "Peço desculpas 😅\n\n"
            "Ainda estou em fase de testes beta e não entendi muito bem sua pergunta.\n\n"
            "Se puder escrever de outra forma, eu tento te ajudar melhor 🙏"
        )

    if guest_language(guest) == "en":
        return "If you give me a little more context, I can help better 😊"

    nome = guest.get("nome", "").strip()
    reply = get_gepetto_fallback_line()
    if nome:
        return f"{nome}, {reply}"
    return reply


# =========================
# DASHBOARD TELEGRAM
# =========================

def compose_dashboard_text():
    usage = read_json(USAGE_FILE, {
        "total_messages": 0,
        "guest_messages": 0,
        "assistant_messages": 0,
        "fallback_count": 0,
        "successful_followups": 0,
        "por_dia": {}
    })
    intents = read_json(INTENT_FILE, {})
    insights = read_json(INSIGHT_FILE, {})
    incidents = read_json(INCIDENTS_FILE, [])

    hoje = today_local_str()
    hoje_stats = usage.get("por_dia", {}).get(hoje, {})

    total_messages = usage.get("total_messages", 0)
    guest_messages = usage.get("guest_messages", 0)
    assistant_messages = usage.get("assistant_messages", 0)
    messages_today = hoje_stats.get("total_messages", 0)
    guest_today = hoje_stats.get("guest_messages", 0)
    assistant_today = hoje_stats.get("assistant_messages", 0)
    first_activity = hoje_stats.get("first_activity", "-")
    last_activity = hoje_stats.get("last_activity", "-")
    fallback_today = hoje_stats.get("fallback_count", 0)
    followups_today = hoje_stats.get("successful_followups", 0)

    sorted_intents = sorted(intents.items(), key=lambda x: x[1], reverse=True)
    top_intents = sorted_intents[:3]
    intents_text = "\n".join([f"• {k}: {v}" for k, v in top_intents]) if top_intents else "• sem dados"

    sorted_insights = sorted(insights.items(), key=lambda x: x[1], reverse=True)
    top_insights = sorted_insights[:4]
    insights_text = "\n".join([f"• {k}: {v}" for k, v in top_insights]) if top_insights else "• sem dados"

    recent_incidents = incidents[-3:] if incidents else []
    if recent_incidents:
        incidents_text = "\n".join([
            f"• {i.get('tipo', 'incidente')} | {i.get('gravidade', '-')} | {i.get('timestamp', '-')}"
            for i in recent_incidents
        ])
    else:
        incidents_text = "• nenhum recente"

    days = usage.get("por_dia", {})
    media_por_dia = 0
    if days:
        total_daily = sum(day.get("total_messages", 0) for day in days.values())
        media_por_dia = round(total_daily / max(len(days), 1), 1)

    return (
        "📊 DASHBOARD GEPETTO — Apto 14B\n\n"
        f"**Mensagens totais:** {total_messages}\n"
        f"**Hóspede:** {guest_messages}\n"
        f"**Gepetto:** {assistant_messages}\n"
        f"**Média por dia:** {media_por_dia}\n\n"
        f"**Hoje:** {messages_today}\n"
        f"• Hóspede: {guest_today}\n"
        f"• Gepetto: {assistant_today}\n"
        f"• Primeiro uso: {first_activity}\n"
        f"• Última atividade: {last_activity}\n"
        f"• Fallbacks hoje: {fallback_today}\n"
        f"• Follow-ups bem sucedidos hoje: {followups_today}\n\n"
        f"**Top intents:**\n{intents_text}\n\n"
        f"**Top interesses detectados:**\n{insights_text}\n\n"
        f"**Incidentes recentes:**\n{incidents_text}"
    )


# =========================
# ADMIN
# =========================

def handle_admin_command(message):
    global ADMIN_UNLOCKED

    parts = message.strip().split(" ", 2)
    cmd = parts[0].lower()

    if cmd == "/admin":
        if len(parts) < 2:
            return "Use: /admin SEU_PIN"
        pin = parts[1].strip()
        if pin == ADMIN_PIN:
            ADMIN_UNLOCKED = True
            return (
                "Modo admin ativado ✅\n\n"
                "Agora você pode usar:\n"
                "/set nome Fernanda\n"
                "/set grupo familia\n"
                "/set perfil_hospede casal\n"
                "/set checkin_date 26/03/2026\n"
                "/set checkout_date 29/03/2026\n"
                "/set checkout_time 11h\n"
                "/set idioma pt\n"
                "/set observacoes aniversário hoje\n"
                "/show\n"
                "/dashboard\n"
                "/reset\n"
                "/lock"
            )
        return "PIN incorreto ❌"

    if cmd == "/lock":
        ADMIN_UNLOCKED = False
        return "Modo admin desativado 🔒"

    if cmd == "/show":
        guest = load_guest()
        return (
        "Hóspede atual 👇\n\n"
        f"nome: {guest.get('nome','')}\n"
        f"grupo: {guest.get('grupo','')}\n"
        f"perfil_hospede: {guest.get('perfil_hospede','neutro')}\n"
        f"checkin_date: {guest.get('checkin_date','')}\n"
        f"checkout_date: {guest.get('checkout_date','')}\n"
        f"checkout_time: {guest.get('checkout_time','')}\n"
        f"idioma: {guest.get('idioma','')}\n"
        f"observacoes: {guest.get('observacoes','')}"
    )

    if cmd == "/dashboard":
        if not ADMIN_UNLOCKED:
            return "Ative primeiro com /admin SEU_PIN 🔒"

        text = compose_dashboard_text()
        ok, msg = send_telegram_message(text)
        if ok:
            return f"{text}\n\n📨 Dashboard enviado ao Telegram ✅"
        return f"{text}\n\n⚠️ Não consegui enviar ao Telegram agora: {msg}"

    if cmd == "/reset":
        save_guest(default_guest())
        reset_memory()
        reset_session()
        return "Dados do hóspede e sessão resetados ♻️"

    if cmd == "/set":
        if not ADMIN_UNLOCKED:
            return "Ative primeiro com /admin SEU_PIN 🔒"

        if len(parts) < 3:
            return "Use: /set campo valor"

        field = parts[1].strip().lower()
        value = parts[2].strip()

        valid_fields = [
            "nome",
            "grupo",
            "checkin_date",
            "checkout_date",
            "checkout_time",
            "idioma",
            "observacoes",
            "perfil_hospede"
        ]
        if field not in valid_fields:
            return f"Campo inválido. Use um destes: {', '.join(valid_fields)}"

        guest = load_guest()

        if field == "grupo":
            value = normalize_group_value(value)

        if field == "perfil_hospede":
             value = normalize_profile_value(value)
        
        if field == "idioma":
            value = normalize_text(value)
            if value not in ["pt", "en"]:
                value = "pt"

        if field in ["checkin_date", "checkout_date"]:
            normalized_date = normalize_admin_date_input(value)
            if not normalized_date:
                return "Data inválida. Use, por exemplo: 26/03/2026 ou 26/03"
            value = normalized_date

        if field == "checkout_time":
            normalized_time = normalize_checkout_time_input(value)
            if not normalized_time:
                return "Horário inválido. Use, por exemplo: 11:00, 11h ou 13:30"
            value = normalized_time

        guest[field] = value
        save_guest(guest)

        return f"{field} atualizado para: {value} ✅"

    return None


# =========================
# CORE
# =========================

def finalize_and_log(
    guest,
    text_raw,
    topic,
    reply,
    remembered=False,
    used_followup=False,
    intent_for_session=""
):
    append_memory("user", text_raw, topic, {
        "remembered_guest": remembered,
        "used_followup": used_followup
    })
    append_memory("assistant", reply, topic)
    update_session(
        last_topic=topic,
        last_intent=intent_for_session or topic
    )
    log_conversation(guest, text_raw, topic, reply)
    update_intent_stats(topic)
    update_guest_insights(text_raw)
    update_usage_stats(text_raw, reply, topic, used_followup=used_followup)
    update_guest_preferences(text_raw)

    if topic in ["incidente", "saude", "bruno", "fallback", "checkout"]:
        notify_conversation_to_telegram(guest, text_raw, topic, reply)

    return reply


def gepetto_responde(msg):
    guest_before = load_guest()
    text_raw = msg or ""
    text = normalize_text(text_raw)
    last_topic = get_last_topic()

    guest_after, remembered = remember_guest_details(text_raw)
    guest = guest_after if remembered else guest_before

    if text_raw.startswith("/"):
        admin_reply = handle_admin_command(text_raw)
        if admin_reply is not None:
            append_memory("user", text_raw, "admin")
            append_memory("assistant", admin_reply, "admin")
            return admin_reply

    if is_social_checkin(text_raw):
        reply = get_social_reply()
        return finalize_and_log(guest, text_raw, "saudacao", reply, remembered, intent_for_session="saudacao_social")

    if has_any(text, ["oi", "ola", "olá", "cheguei", "chegamos", "boa tarde", "bom dia", "boa noite", "hello", "hi", "hey"]):
        especial = observacao_especial(guest)
        if guest_language(guest) == "en":
            reply = (
                f"{saudacao_personalizada(guest)}\n\n"
                f"{especial}"
                "Glad you arrived well!\n\n"
                f"{proactive_prompt(guest)}"
            )
        else:
            reply = (
                f"{saudacao_personalizada(guest)}\n\n"
                f"{especial}"
                "Que bom que você chegou!\n\n"
                f"{proactive_prompt(guest)}"
            )
        return finalize_and_log(guest, text_raw, "saudacao", reply, remembered, intent_for_session="saudacao")

    sess = load_session()

    if sess.get("pending_incident_context"):
        incident_context_reply = handle_incident_context_followup(guest, text_raw)
        if incident_context_reply:
            return finalize_and_log(
                guest,
                text_raw,
                "incidente",
                incident_context_reply,
                remembered,
                used_followup=True,
                intent_for_session="incidente_contexto"
            )

    if sess.get("pending_bruno_contact"):
        followup = get_followup_reply(text_raw, "bruno", guest)
        if followup:
            return finalize_and_log(
                guest,
                text_raw,
                "bruno",
                followup,
                remembered,
                used_followup=True,
                intent_for_session="bruno"
            )

        text_n = normalize_text(text_raw)
        if text_n and not has_any(text_n, ["oi", "ola", "olá", "bom dia", "boa tarde", "boa noite"]):
            ok, _ = notify_bruno_request(guest, text_raw)
            set_bruno_pending(False)

            if ok:
                reply = "Perfeito 😊 Já avisei o Bruno e adiantei esse assunto para ele. Ele entrará em contato com você o quanto antes."
            else:
                reply = "Entendi 😊 Tentei avisar o Bruno agora, mas não consegui enviar a solicitação de acompanhamento neste momento."

            return finalize_and_log(
                guest,
                text_raw,
                "bruno",
                reply,
                remembered,
                used_followup=True,
                intent_for_session="bruno"
            )

    if is_airbnb_listing_info_request(text_raw):
        notify_airbnb_listing_info_to_telegram(guest, text_raw)
        reply = get_airbnb_listing_info_reply()
        return finalize_and_log(
            guest,
            text_raw,
            "airbnb_info",
            reply,
            remembered,
            intent_for_session="airbnb_info"
        )

    inferred_intent_preview = infer_primary_intent(text_raw, last_topic)

    if last_topic == "saude" and has_any(text, [
        "farmacia", "farmácia", "farmacias", "farmácias",
        "upa", "hospital", "todos", "todas"
    ]):
        inferred_intent_preview = "saude"

    preferred_category = contextual_entity_category(last_topic, inferred_intent_preview)

    comparison_entities = resolve_entities_from_text(
        text_raw,
        allow_generic_aliases=False,
        preferred_category=preferred_category
    )
    if (
        len(comparison_entities) >= 2
        and all(e.get("category", "") == "restaurantes" for e in comparison_entities[:2])
        and has_any(normalize_text(text_raw), [" ou ", " vs ", "versus", "melhor", "diferenca", "diferença", "comparar", "qual compensa", "qual vale mais a pena"])
    ):
        comparison_reply = get_entity_comparison_reply(comparison_entities[0], comparison_entities[1])
        if comparison_reply:
            return finalize_and_log(
                guest,
                text_raw,
                "restaurantes",
                comparison_reply,
                remembered,
                used_followup=True,
                intent_for_session="comparacao_restaurantes"
            )

    if should_use_entity_detail_mode(text_raw, inferred_intent_preview, last_topic):
        entity = resolve_entity_from_text(
            text_raw,
            allow_generic_aliases=True,
            preferred_category=preferred_category
        )
        if not entity:
            entity = resolve_last_entity_from_session()

        field = get_requested_detail_field(text_raw)
        detail_reply = get_entity_detail_reply(entity, field) if entity else ""

        if detail_reply:
            return finalize_and_log(
                guest,
                text_raw,
                entity.get("category", "detalhe_local"),
                detail_reply,
                remembered,
                used_followup=True,
                intent_for_session="detalhe_local"
            )

    explicit_entity = resolve_entity_from_text(
        text_raw,
        allow_generic_aliases=False,
        preferred_category=preferred_category
    )
    if explicit_entity and not looks_like_detail_question(text_raw):
        summary_reply = get_entity_summary_reply(explicit_entity)
        if summary_reply:
            return finalize_and_log(
                guest,
                text_raw,
                explicit_entity.get("category", "local"),
                summary_reply,
                remembered,
                used_followup=True,
                intent_for_session="entidade_explicita"
            )

    inferred_intent = inferred_intent_preview

    if is_followup_candidate(text_raw, last_topic, inferred_intent):
        followup = get_followup_reply(text_raw, last_topic, guest)
        if followup:
            return finalize_and_log(
                guest,
                text_raw,
                last_topic or "followup",
                followup,
                remembered,
                used_followup=True,
                intent_for_session=last_topic or "followup"
            )

    if should_ask_for_followup_reference(text_raw, last_topic, inferred_intent):
        clarify_reply = get_followup_reference_clarifier(text_raw, last_topic)
        return finalize_and_log(
            guest,
            text_raw,
            last_topic or "clarificacao_contexto",
            clarify_reply,
            remembered,
            used_followup=True,
            intent_for_session="clarificacao_contexto"
        )

    if not last_topic and is_ambiguous_reference_message(text_raw):
        clarify_reply = get_followup_reference_clarifier(text_raw, "")
        return finalize_and_log(
            guest,
            text_raw,
            "clarificacao_contexto",
            clarify_reply,
            remembered,
            used_followup=True,
            intent_for_session="clarificacao_contexto"
        )

    intent = inferred_intent

    if intent == "identidade":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "identidade", get_identidade_reply(text_raw), remembered, intent_for_session="identidade")

    if intent == "localizacao":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "localizacao", get_localizacao_reply(text_raw), remembered, intent_for_session="localizacao")

    if intent == "saude":
        clear_active_recommendations()
        if has_any(text, ["hospital", "upa"]) and not has_any(text, ["doente", "mal", "passando mal", "dor", "febre", "enjoo", "vomito", "vômito"]):
            if has_any(text, ["hospital"]):
                reply = get_localizacao_reply("hospital")
            else:
                reply = get_localizacao_reply("upa")
            return finalize_and_log(guest, text_raw, "saude", reply, remembered, intent_for_session="saude")

        if len(text.split()) <= 4 and has_any(text, ["doente", "mal", "passando mal", "saude", "saúde"]):
            reply = get_guided_reply("saude")
        else:
            reply = get_health_reply(text_raw)

        ok, _ = maybe_notify("saude", text_raw, guest, classify_health(text_raw))
        if ok:
            reply = reply + "\n\nJá deixei isso sinalizado por aqui e enviei uma solicitação de acompanhamento ao Bruno. Ele entrará em contato com você o quanto antes 😊"
        else:
            reply = reply + "\n\nJá deixei isso sinalizado por aqui, mas não consegui enviar a solicitação de acompanhamento ao Bruno neste momento."
        return finalize_and_log(guest, text_raw, "saude", reply, remembered, intent_for_session="saude")

    if intent == "incidente":
        clear_active_recommendations()
        severity = classify_incident(text_raw)
        base_reply = get_problem_reply(text_raw)
        ok, _ = maybe_notify("incidente", text_raw, guest, severity)

        if has_any(text, ["porta nao abre", "porta não abre", "nao entra", "não entra"]):
            set_incident_pending(True)
        elif severity in ["media", "baixa"]:
            set_incident_pending(True)
        else:
            set_incident_pending(False)

        if ok:
            reply = base_reply + "\n\nJá deixei isso sinalizado por aqui e enviei uma solicitação de acompanhamento ao Bruno. Ele entrará em contato com você o quanto antes 😊"
        else:
            reply = base_reply + "\n\nJá deixei isso sinalizado por aqui, mas não consegui enviar a solicitação de acompanhamento neste momento."

        return finalize_and_log(guest, text_raw, "incidente", reply, remembered, intent_for_session="incidente")

    if intent == "wifi":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "wifi", get_wifi_reply(), remembered, intent_for_session="wifi")

    if intent == "regras":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "regras", get_regras_reply(text_raw), remembered, intent_for_session="regras")

    if intent == "praia_local":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "praia", get_servico_praia_localizacao_reply(), remembered, intent_for_session="praia_local")

    if intent == "praia":
        clear_active_recommendations()
        reply = (
            get_guided_reply("praia")
            if len(text.split()) <= 2 and has_any(text, ["praia", "praias"])
            else get_praia_reply(guest, text_raw)
        )
        return finalize_and_log(
            guest,
            text_raw,
            "praia",
            reply,
            remembered,
            intent_for_session="praia"
        )

    if intent == "roteiro":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "roteiro", get_roteiro_reply(guest), remembered, intent_for_session="roteiro")

    if intent == "restaurantes":
        if len(text.split()) <= 3 and has_any(text, ["comer", "jantar", "restaurante", "restaurantes", "fome"]):
            reply = get_guided_reply("restaurantes")
        else:
            reply = get_restaurantes_reply(text_raw)
        return finalize_and_log(guest, text_raw, "restaurantes", reply, remembered, intent_for_session="restaurantes")

    if intent == "mercado":
        if len(text.split()) <= 3 and has_any(text, ["mercado", "mercados", "compras", "supermercado", "supermercados", "mercado dia", "supermercado dia"]):
            reply = get_guided_reply("mercado")
        else:
            reply = get_mercado_reply(text_raw)
        return finalize_and_log(guest, text_raw, "mercado", reply, remembered, intent_for_session="mercado")

    if intent == "padaria":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "padaria", get_padaria_reply(), remembered, intent_for_session="padaria")

    if intent == "farmacia":
        clear_active_recommendations()
        if len(text.split()) <= 3 and has_any(text, ["farmacia", "farmácia", "farmacias", "farmácias"]):
            reply = get_guided_reply("farmacia")
        else:
            reply = get_farmacia_reply(text_raw)
        return finalize_and_log(guest, text_raw, "farmacia", reply, remembered, intent_for_session="farmacia")

    if intent == "apoio_predio":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "apoio_predio", get_apoio_predio_reply(), remembered, intent_for_session="apoio_predio")

    if intent == "garagem":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "garagem", get_garagem_reply(), remembered, intent_for_session="garagem")

    if intent == "chaves":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "chaves", get_chaves_reply(), remembered, intent_for_session="chaves")

    if intent == "checkout":
        clear_active_recommendations()
        if has_any(text, [
            "antes do checkout", "antes do check-out",
            "ir embora", "antes de sair", "o que fazer antes de sair",
            "avisos antes do checkout", "preciso fazer algo antes de sair"
        ]):
            reply = get_checkout_aviso_reply(guest)
        else:
            reply = get_checkout_reply(guest)
        return finalize_and_log(guest, text_raw, "checkout", reply, remembered, intent_for_session="checkout")

    if intent == "bruno":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "bruno", get_bruno_reply(), remembered, intent_for_session="bruno")

    if intent == "bares":
        clear_active_recommendations()
        if len(text.split()) <= 3 and has_any(text, ["bar", "bares", "pub", "noite", "drink", "drinks"]):
            reply = get_guided_reply("bares")
        else:
            reply = get_bares_reply()
        return finalize_and_log(guest, text_raw, "bares", reply, remembered, intent_for_session="bares")

    if intent == "shopping":
        reply = get_shopping_reply()
        return finalize_and_log(guest, text_raw, "shopping", reply, remembered, intent_for_session="shopping")

    if intent == "feira":
        reply = get_feira_reply()
        return finalize_and_log(guest, text_raw, "feira", reply, remembered, intent_for_session="feira")

    if intent == "tempo":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "tempo", get_tempo_reply(), remembered, intent_for_session="tempo")

    if intent == "passeio":
        reply = get_passeios_reply(text_raw)
        return finalize_and_log(guest, text_raw, "passeio", reply, remembered, intent_for_session="passeio")

    if intent == "eventos":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "eventos", get_eventos_reply(), remembered, intent_for_session="eventos")

    if intent == "surf":
        clear_active_recommendations()
        return finalize_and_log(guest, text_raw, "surf", get_surf_reply(), remembered, intent_for_session="surf")

    clear_active_recommendations()
    reply = get_fallback_reply(guest)
    return finalize_and_log(guest, text_raw, "fallback", reply, remembered, intent_for_session="fallback")


# =========================
# ROTAS
# =========================

@app.route("/")
def home():
    return send_from_directory("static", "index.html")


@app.route("/chat", methods=["POST"])
def chat():
    try:
        data = request.get_json() or {}
        msg = data.get("message", "")
        resposta = gepetto_responde(msg)
        return json_response({"reply": resposta})
    except Exception as e:
        print("ERRO NO CHAT:", e)
        return json_response({
            "reply": "Peço desculpas 😅\n\nAinda estou em fase de testes beta e tive uma falha aqui.\n\nPode repetir sua mensagem?"
        }, status=500)


@app.route("/welcome", methods=["GET"])
def welcome():
    try:
        return json_response({"message": mensagem_boas_vindas()})
    except Exception as e:
        print("ERRO NO WELCOME:", e)
        return json_response({"message": "Olá 😊"})


@app.route("/admin", methods=["GET"])
@admin_required
def admin_home():
    db_status = "conectado" if has_database() else "não configurado"
    token = get_admin_token_from_request(request)

    operational_items = [
        ("Dashboard", f"/admin/dashboard?token={token}"),
        ("Conversations", f"/admin/conversations?token={token}"),
        ("Sessions", f"/admin/sessions?token={token}"),
        ("Incidents", f"/admin/incidents?token={token}"),
        ("Guests", f"/admin/guests?token={token}")
    ]

    intelligence_items = [
        ("Intents", f"/admin/intents?token={token}"),
        ("Insights", f"/admin/insights?token={token}"),
        ("Usage", f"/admin/usage?token={token}")
    ]

    def build_nav_links(items):
        return "".join(
            f'<a href="{href}" style="display:block;padding:10px 12px;margin:8px 0;'
            f'background:#f6f6f6;border:1px solid #e2e2e2;border-radius:10px;'
            f'text-decoration:none;color:#111;">{label}</a>'
            for label, href in items
        )

    operational_links = build_nav_links(operational_items)
    intelligence_links = build_nav_links(intelligence_items)

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:980px;margin:0 auto;padding:24px;">
            <div style="background:white;border-radius:16px;padding:24px;border:1px solid #e6e6e6;">
                <div style="margin-bottom:24px;">
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin V1
                    </div>
                    <h1 style="margin:8px 0 10px 0;font-size:32px;line-height:1.1;">
                        Painel administrativo
                    </h1>
                    <p style="margin:0;color:#555;font-size:16px;line-height:1.5;">
                        Área protegida para acompanhamento operacional e leitura de dados persistidos antes da 3.2.
                    </p>
                </div>

                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-bottom:24px;">
                    <div style="background:#fafafa;border:1px solid #e6e6e6;border-radius:14px;padding:16px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;">Status</div>
                        <div style="margin-top:8px;font-size:18px;font-weight:bold;">Acesso autorizado</div>
                    </div>

                    <div style="background:#fafafa;border:1px solid #e6e6e6;border-radius:14px;padding:16px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;">Horário local</div>
                        <div style="margin-top:8px;font-size:18px;font-weight:bold;">{now_iso()}</div>
                    </div>

                    <div style="background:#fafafa;border:1px solid #e6e6e6;border-radius:14px;padding:16px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;">Banco</div>
                        <div style="margin-top:8px;font-size:18px;font-weight:bold;">{db_status}</div>
                    </div>
                </div>

                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px;margin-bottom:24px;">
                    <div style="background:white;border:1px solid #e6e6e6;border-radius:14px;padding:18px;">
                        <h2 style="margin:0 0 12px 0;font-size:20px;">Operação</h2>
                        <p style="margin:0 0 14px 0;color:#555;">
                            Leitura prática da hospedagem, conversa, sessão e incidentes.
                        </p>
                        {operational_links}
                    </div>

                    <div style="background:white;border:1px solid #e6e6e6;border-radius:14px;padding:18px;">
                        <h2 style="margin:0 0 12px 0;font-size:20px;">Leitura e inteligência</h2>
                        <p style="margin:0 0 14px 0;color:#555;">
                            Sinais, intents, insights e uso recente do Gepetto.
                        </p>
                        {intelligence_links}
                    </div>
                </div>

                <div style="background:#fcfcfc;border:1px dashed #d8d8d8;border-radius:14px;padding:16px;">
                    <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;">Etapa atual</div>
                    <div style="margin-top:8px;font-size:16px;line-height:1.5;">
                        V1 administrativa já estruturada com navegação funcional. Próxima etapa recomendada: melhorar leitura temporal e navegação das conversas.
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


@app.route("/admin/dashboard", methods=["GET"])
@admin_required
def admin_dashboard():
    token = get_admin_token_from_request(request)

    counts = {
        "guests": 0,
        "sessions": 0,
        "conversation_logs": 0,
        "conversation_messages": 0,
        "incidents": 0,
        "intent_events": 0,
        "guest_insight_events": 0,
        "usage_events": 0
    }

    latest_guest = None
    latest_session = None
    recent_incidents = []
    recent_intents = []

    db_error = ""

    if has_database():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) AS total FROM guests;")
                    counts["guests"] = cur.fetchone()["total"]

                    cur.execute("SELECT COUNT(*) AS total FROM session_states;")
                    counts["sessions"] = cur.fetchone()["total"]

                    cur.execute("SELECT COUNT(*) AS total FROM conversation_logs;")
                    counts["conversation_logs"] = cur.fetchone()["total"]

                    cur.execute("SELECT COUNT(*) AS total FROM conversation_messages;")
                    counts["conversation_messages"] = cur.fetchone()["total"]

                    cur.execute("SELECT COUNT(*) AS total FROM incidents;")
                    counts["incidents"] = cur.fetchone()["total"]

                    cur.execute("SELECT COUNT(*) AS total FROM intent_events;")
                    counts["intent_events"] = cur.fetchone()["total"]

                    cur.execute("SELECT COUNT(*) AS total FROM guest_insight_events;")
                    counts["guest_insight_events"] = cur.fetchone()["total"]

                    cur.execute("SELECT COUNT(*) AS total FROM usage_events;")
                    counts["usage_events"] = cur.fetchone()["total"]

                    cur.execute("""
                        SELECT nome, grupo, perfil_hospede, idioma, checkin_date, checkout_date, checkout_time, updated_at
                        FROM guests
                        ORDER BY updated_at DESC
                        LIMIT 1
                    """)
                    latest_guest = cur.fetchone()

                    cur.execute("""
                        SELECT last_topic, last_intent, last_recommendation_type, last_recommendation_name,
                               pending_bruno_contact, pending_incident_context, updated_at
                        FROM session_states
                        ORDER BY updated_at DESC
                        LIMIT 1
                    """)
                    latest_session = cur.fetchone()

                    cur.execute("""
                        SELECT tipo, gravidade, mensagem, status, timestamp
                        FROM incidents
                        ORDER BY timestamp DESC
                        LIMIT 5
                    """)
                    recent_incidents = cur.fetchall() or []

                    cur.execute("""
                        SELECT intent, topic, timestamp
                        FROM intent_events
                        ORDER BY timestamp DESC
                        LIMIT 5
                    """)
                    recent_intents = cur.fetchall() or []

        except Exception as e:
            db_error = str(e)

    def fmt_dt(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(value)

    def fmt_date(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y")
        except Exception:
            return str(value)

    def fmt_time(value):
        if not value:
            return "-"
        try:
            return value.strftime("%H:%M")
        except Exception:
            return str(value)

    def build_count_card(label, value):
        return f"""
        <div style="background:#fafafa;border:1px solid #e6e6e6;border-radius:14px;padding:16px;">
            <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;">{label}</div>
            <div style="margin-top:8px;font-size:26px;font-weight:bold;">{value}</div>
        </div>
        """

    def build_rows(items, empty_message, row_builder):
        if not items:
            return f'<div style="color:#666;">{empty_message}</div>'
        return "".join(row_builder(item) for item in items)

    guest_html = """
        <div style="color:#666;">Nenhum guest encontrado.</div>
    """
    if latest_guest:
        guest_html = f"""
        <div style="line-height:1.7;">
            <div><strong>Nome:</strong> {latest_guest.get("nome") or "-"}</div>
            <div><strong>Grupo:</strong> {latest_guest.get("grupo") or "-"}</div>
            <div><strong>Perfil:</strong> {latest_guest.get("perfil_hospede") or "-"}</div>
            <div><strong>Idioma:</strong> {latest_guest.get("idioma") or "-"}</div>
            <div><strong>Check-in:</strong> {fmt_date(latest_guest.get("checkin_date"))}</div>
            <div><strong>Check-out:</strong> {fmt_date(latest_guest.get("checkout_date"))}</div>
            <div><strong>Horário de saída:</strong> {fmt_time(latest_guest.get("checkout_time"))}</div>
            <div><strong>Atualizado em:</strong> {fmt_dt(latest_guest.get("updated_at"))}</div>
        </div>
        """

    session_html = """
        <div style="color:#666;">Nenhuma session encontrada.</div>
    """
    if latest_session:
        session_html = f"""
        <div style="line-height:1.7;">
            <div><strong>Último tópico:</strong> {latest_session.get("last_topic") or "-"}</div>
            <div><strong>Última intent:</strong> {latest_session.get("last_intent") or "-"}</div>
            <div><strong>Último tipo de recomendação:</strong> {latest_session.get("last_recommendation_type") or "-"}</div>
            <div><strong>Última recomendação:</strong> {latest_session.get("last_recommendation_name") or "-"}</div>
            <div><strong>Bruno pendente:</strong> {"sim" if latest_session.get("pending_bruno_contact") else "não"}</div>
            <div><strong>Incidente pendente:</strong> {"sim" if latest_session.get("pending_incident_context") else "não"}</div>
            <div><strong>Atualizado em:</strong> {fmt_dt(latest_session.get("updated_at"))}</div>
        </div>
        """

    incidents_html = build_rows(
        recent_incidents,
        "Nenhum incidente recente.",
        lambda item: f"""
        <div style="padding:12px 0;border-top:1px solid #efefef;">
            <div><strong>{item.get("tipo") or "-"}</strong> • gravidade: {item.get("gravidade") or "-"}</div>
            <div style="margin-top:4px;color:#444;">{item.get("mensagem") or "-"}</div>
            <div style="margin-top:4px;font-size:13px;color:#666;">
                status: {item.get("status") or "-"} • {fmt_dt(item.get("timestamp"))}
            </div>
        </div>
        """
    )

    intents_html = build_rows(
        recent_intents,
        "Nenhuma intent recente.",
        lambda item: f"""
        <div style="padding:12px 0;border-top:1px solid #efefef;">
            <div><strong>{item.get("intent") or "-"}</strong></div>
            <div style="margin-top:4px;color:#444;">topic: {item.get("topic") or "-"}</div>
            <div style="margin-top:4px;font-size:13px;color:#666;">{fmt_dt(item.get("timestamp"))}</div>
        </div>
        """
    )

    db_status = "conectado" if has_database() and not db_error else ("erro" if db_error else "não configurado")

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin Dashboard</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:1200px;margin:0 auto;padding:24px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px;">
                <div>
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin Dashboard V1
                    </div>
                    <h1 style="margin:8px 0 0 0;font-size:32px;line-height:1.1;">Resumo operacional</h1>
                </div>
                <div>
                    <a href="/admin?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        ← Voltar ao admin
                    </a>
                </div>
            </div>

            <div style="background:white;border-radius:16px;padding:18px 20px;border:1px solid #e6e6e6;margin-bottom:18px;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">
                    <div><strong>Horário local:</strong> {now_iso()}</div>
                    <div><strong>Banco:</strong> {db_status}</div>
                    <div><strong>Stage:</strong> admin_v1_dashboard_live</div>
                </div>
                {f'<div style="margin-top:12px;color:#a33;"><strong>Erro DB:</strong> {db_error}</div>' if db_error else ''}
            </div>

            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:18px;">
                {build_count_card("Guests", counts["guests"])}
                {build_count_card("Sessions", counts["sessions"])}
                {build_count_card("Logs", counts["conversation_logs"])}
                {build_count_card("Messages", counts["conversation_messages"])}
                {build_count_card("Incidents", counts["incidents"])}
                {build_count_card("Intents", counts["intent_events"])}
                {build_count_card("Insights", counts["guest_insight_events"])}
                {build_count_card("Usage", counts["usage_events"])}
            </div>

            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:16px;">
                <div style="background:white;border-radius:16px;padding:20px;border:1px solid #e6e6e6;">
                    <h2 style="margin:0 0 14px 0;font-size:20px;">Último hóspede</h2>
                    {guest_html}
                </div>

                <div style="background:white;border-radius:16px;padding:20px;border:1px solid #e6e6e6;">
                    <h2 style="margin:0 0 14px 0;font-size:20px;">Última sessão</h2>
                    {session_html}
                </div>

                <div style="background:white;border-radius:16px;padding:20px;border:1px solid #e6e6e6;">
                    <h2 style="margin:0 0 14px 0;font-size:20px;">Incidentes recentes</h2>
                    {incidents_html}
                </div>

                <div style="background:white;border-radius:16px;padding:20px;border:1px solid #e6e6e6;">
                    <h2 style="margin:0 0 14px 0;font-size:20px;">Intents recentes</h2>
                    {intents_html}
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")

@app.route("/admin/conversations", methods=["GET"])
@admin_required
def admin_conversations():
    token = get_admin_token_from_request(request)
    rows = []
    db_error = ""

    if has_database():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            thread_id,
                            role,
                            text,
                            topic,
                            meta_json,
                            timestamp
                        FROM (
                            SELECT
                                thread_id,
                                role,
                                text,
                                topic,
                                meta_json,
                                timestamp
                            FROM conversation_messages
                            ORDER BY timestamp DESC
                            LIMIT 120
                        ) recent_messages
                        ORDER BY timestamp ASC
                    """)
                    rows = cur.fetchall() or []
        except Exception as e:
            db_error = str(e)

    def fmt_dt(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(value)

    grouped_threads = []
    thread_map = {}

    for item in rows:
        thread_id = item.get("thread_id") or "sem-thread"

        if thread_id not in thread_map:
            thread_map[thread_id] = {
                "thread_id": thread_id,
                "messages": [],
                "started_at": item.get("timestamp"),
                "ended_at": item.get("timestamp")
            }
            grouped_threads.append(thread_map[thread_id])

        thread_data = thread_map[thread_id]
        thread_data["messages"].append(item)

        current_ts = item.get("timestamp")
        if current_ts:
            if not thread_data["started_at"] or current_ts < thread_data["started_at"]:
                thread_data["started_at"] = current_ts
            if not thread_data["ended_at"] or current_ts > thread_data["ended_at"]:
                thread_data["ended_at"] = current_ts

    grouped_threads.sort(
        key=lambda t: t["ended_at"] if t["ended_at"] else "",
        reverse=True
    )

    if grouped_threads:
        thread_blocks = []

        for thread in grouped_threads:
            thread_id = thread["thread_id"]
            started_at = fmt_dt(thread["started_at"])
            ended_at = fmt_dt(thread["ended_at"])
            message_count = len(thread["messages"])

            message_blocks = []

            for item in thread["messages"]:
                role = item.get("role") or "-"
                topic = item.get("topic") or "-"
                text = (item.get("text") or "").replace("<", "&lt;").replace(">", "&gt;")
                timestamp = fmt_dt(item.get("timestamp"))
                meta = item.get("meta_json") or {}

                meta_html = ""
                if meta:
                    meta_html = f"""
                    <div style="margin-top:10px;padding:10px 12px;background:#fafafa;border:1px solid #ececec;border-radius:10px;font-size:13px;color:#555;">
                        <strong>Meta:</strong> {json.dumps(meta, ensure_ascii=False)}
                    </div>
                    """

                role_label = "Hóspede" if role == "user" else ("Gepetto" if role == "assistant" else role)

                message_blocks.append(f"""
                <div style="background:#fff;border:1px solid #ececec;border-radius:14px;padding:16px;margin-bottom:12px;">
                    <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
                        <div style="font-size:15px;font-weight:bold;">{role_label}</div>
                        <div style="font-size:13px;color:#666;">{timestamp}</div>
                    </div>
                    <div style="font-size:13px;color:#666;margin-bottom:10px;">
                        <strong>role:</strong> {role} &nbsp;•&nbsp; <strong>topic:</strong> {topic}
                    </div>
                    <div style="font-size:15px;line-height:1.6;white-space:pre-wrap;">{text}</div>
                    {meta_html}
                </div>
                """)

            thread_blocks.append(f"""
            <div style="background:white;border:1px solid #dddddd;border-radius:18px;padding:20px;margin-bottom:20px;">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:16px;padding-bottom:14px;border-bottom:1px solid #ececec;">
                    <div>
                        <div style="font-size:13px;letter-spacing:0.06em;color:#666;text-transform:uppercase;margin-bottom:6px;">
                            Início da thread
                        </div>
                        <div style="font-size:24px;font-weight:700;line-height:1.2;">
                            {started_at}
                        </div>
                        <div style="margin-top:8px;font-size:13px;color:#666;">
                            <strong>thread_id:</strong> {thread_id}
                        </div>
                        <div style="margin-top:4px;font-size:13px;color:#666;">
                            <strong>última atividade:</strong> {ended_at}
                        </div>
                    </div>

                    <div style="background:#f7f7f7;border:1px solid #e4e4e4;border-radius:12px;padding:10px 12px;min-width:120px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;">Mensagens</div>
                        <div style="margin-top:6px;font-size:22px;font-weight:bold;">{message_count}</div>
                    </div>
                </div>

                {''.join(message_blocks)}
            </div>
            """)

        conversations_html = "".join(thread_blocks)
    else:
        conversations_html = """
        <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:20px;color:#666;">
            Nenhuma mensagem encontrada.
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin Conversations</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:1100px;margin:0 auto;padding:24px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px;flex-wrap:wrap;">
                <div>
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin Conversations
                    </div>
                    <h1 style="margin:8px 0 0 0;font-size:32px;line-height:1.1;">Conversas por thread</h1>
                    <p style="margin:10px 0 0 0;color:#555;">
                        Últimas 120 mensagens agrupadas por <code>thread_id</code>, com mensagens em ordem cronológica dentro de cada thread.
                    </p>
                </div>
                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    <a href="/admin?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        ← Admin
                    </a>
                    <a href="/admin/dashboard?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        Dashboard
                    </a>
                </div>
            </div>

            <div style="background:white;border-radius:16px;padding:18px 20px;border:1px solid #e6e6e6;margin-bottom:18px;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">
                    <div><strong>Horário local:</strong> {now_iso()}</div>
                    <div><strong>Banco:</strong> {"conectado" if has_database() and not db_error else ("erro" if db_error else "não configurado")}</div>
                    <div><strong>Recorte:</strong> 120 mensagens</div>
                </div>
                {f'<div style="margin-top:12px;color:#a33;"><strong>Erro DB:</strong> {db_error}</div>' if db_error else ''}
            </div>

            {conversations_html}
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


@app.route("/admin/sessions", methods=["GET"])
@admin_required
def admin_sessions():
    token = get_admin_token_from_request(request)
    sessions = []
    db_error = ""

    if has_database():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            guest_id,
                            last_topic,
                            last_intent,
                            last_followup_hint,
                            last_recommendation_type,
                            last_recommendation_name,
                            last_entity_name,
                            last_entity_category,
                            pending_bruno_contact,
                            pending_incident_context,
                            last_incident_context,
                            active_recommendation_type,
                            active_recommendation_options_json,
                            active_recommendation_index,
                            active_recommendation_updated_at,
                            updated_at
                        FROM session_states
                        ORDER BY updated_at DESC
                        LIMIT 30
                    """)
                    sessions = cur.fetchall() or []
        except Exception as e:
            db_error = str(e)

    def fmt_dt(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(value)

    if sessions:
        session_blocks = []

        for item in sessions:
            active_options = item.get("active_recommendation_options_json")
            if not isinstance(active_options, list):
                active_options = []

            active_count = len(active_options)
            active_index = item.get("active_recommendation_index", 0)
            current_active_name = "-"

            if active_options and isinstance(active_index, int) and 0 <= active_index < len(active_options):
                current_active_name = str(active_options[active_index])

            options_html = (
                "<br>".join(f"• {str(opt)}" for opt in active_options)
                if active_options else
                "<span style='color:#666;'>nenhuma</span>"
            )

            pending_bruno = item.get("pending_bruno_contact")
            pending_incident = item.get("pending_incident_context")

            pending_bruno_html = (
                '<span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#fff3cd;border:1px solid #f1d58a;">Bruno pendente</span>'
                if pending_bruno else
                '<span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#f3f3f3;border:1px solid #e2e2e2;color:#666;">Bruno ok</span>'
            )

            pending_incident_html = (
                '<span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#ffe3e3;border:1px solid #f0b2b2;">Incidente pendente</span>'
                if pending_incident else
                '<span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#f3f3f3;border:1px solid #e2e2e2;color:#666;">Incidente ok</span>'
            )

            session_blocks.append(f"""
            <div style="background:white;border:1px solid #dddddd;border-radius:18px;padding:20px;margin-bottom:20px;">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:16px;padding-bottom:14px;border-bottom:1px solid #ececec;">
                    <div>
                        <div style="font-size:12px;letter-spacing:0.06em;color:#666;text-transform:uppercase;margin-bottom:6px;">
                            Session
                        </div>
                        <div style="font-size:22px;font-weight:700;line-height:1.2;">
                            guest_id: {item.get("guest_id") or "-"}
                        </div>
                        <div style="margin-top:8px;font-size:13px;color:#666;">
                            Atualizado em: {fmt_dt(item.get("updated_at"))}
                        </div>
                    </div>

                    <div style="display:flex;gap:10px;flex-wrap:wrap;">
                        {pending_bruno_html}
                        {pending_incident_html}
                    </div>
                </div>

                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px;">
                    <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:14px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">Contexto</div>
                        <div><strong>last_topic:</strong> {item.get("last_topic") or "-"}</div>
                        <div><strong>last_intent:</strong> {item.get("last_intent") or "-"}</div>
                        <div><strong>last_followup_hint:</strong> {item.get("last_followup_hint") or "-"}</div>
                        <div><strong>last_entity_name:</strong> {item.get("last_entity_name") or "-"}</div>
                        <div><strong>last_entity_category:</strong> {item.get("last_entity_category") or "-"}</div>
                    </div>

                    <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:14px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">Recomendação</div>
                        <div><strong>last_recommendation_type:</strong> {item.get("last_recommendation_type") or "-"}</div>
                        <div><strong>last_recommendation_name:</strong> {item.get("last_recommendation_name") or "-"}</div>
                        <div><strong>active_recommendation_type:</strong> {item.get("active_recommendation_type") or "-"}</div>
                        <div><strong>current_active_name:</strong> {current_active_name}</div>
                    </div>

                    <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:14px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">Pendências</div>
                        <div><strong>pending_bruno_contact:</strong> {"sim" if pending_bruno else "não"}</div>
                        <div><strong>pending_incident_context:</strong> {"sim" if pending_incident else "não"}</div>
                        <div><strong>last_incident_context:</strong> {item.get("last_incident_context") or "-"}</div>
                    </div>

                    <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:14px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">Estado ativo</div>
                        <div><strong>active_recommendation_index:</strong> {active_index if active_index is not None else "-"}</div>
                        <div><strong>active_options_count:</strong> {active_count}</div>
                        <div><strong>active_recommendation_updated_at:</strong> {fmt_dt(item.get("active_recommendation_updated_at"))}</div>
                    </div>
                </div>

                <div style="margin-top:14px;padding:12px;background:#fcfcfc;border:1px solid #ececec;border-radius:12px;">
                    <div style="font-size:13px;color:#666;margin-bottom:6px;"><strong>active_recommendation_options_json</strong></div>
                    <div style="font-size:14px;line-height:1.6;">{options_html}</div>
                </div>
            </div>
            """)

        sessions_html = "".join(session_blocks)
    else:
        sessions_html = """
        <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:20px;color:#666;">
            Nenhuma session encontrada.
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin Sessions</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:1150px;margin:0 auto;padding:24px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px;flex-wrap:wrap;">
                <div>
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin Sessions
                    </div>
                    <h1 style="margin:8px 0 0 0;font-size:32px;line-height:1.1;">Sessions persistidas</h1>
                    <p style="margin:10px 0 0 0;color:#555;">
                        Últimos 30 registros de <code>session_states</code>.
                    </p>
                </div>
                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    <a href="/admin?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        ← Admin
                    </a>
                    <a href="/admin/dashboard?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        Dashboard
                    </a>
                </div>
            </div>

            <div style="background:white;border-radius:16px;padding:18px 20px;border:1px solid #e6e6e6;margin-bottom:18px;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">
                    <div><strong>Horário local:</strong> {now_iso()}</div>
                    <div><strong>Banco:</strong> {"conectado" if has_database() and not db_error else ("erro" if db_error else "não configurado")}</div>
                    <div><strong>Limite:</strong> 30 sessions</div>
                </div>
                {f'<div style="margin-top:12px;color:#a33;"><strong>Erro DB:</strong> {db_error}</div>' if db_error else ''}
            </div>

            {sessions_html}
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")

@app.route("/admin/incidents", methods=["GET"])
@admin_required
def admin_incidents():
    token = get_admin_token_from_request(request)
    incidents = []
    db_error = ""

    if has_database():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            tipo,
                            gravidade,
                            mensagem,
                            detalhe,
                            status,
                            grupo,
                            checkout_label,
                            timestamp
                        FROM incidents
                        ORDER BY timestamp DESC
                        LIMIT 50
                    """)
                    incidents = cur.fetchall() or []
        except Exception as e:
            db_error = str(e)

    def fmt_dt(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(value)

    if incidents:
        incident_blocks = []

        for item in incidents:
            tipo = item.get("tipo") or "-"
            gravidade = item.get("gravidade") or "-"
            mensagem = (item.get("mensagem") or "-").replace("<", "&lt;").replace(">", "&gt;")
            detalhe = (item.get("detalhe") or "").replace("<", "&lt;").replace(">", "&gt;")
            status = item.get("status") or "-"
            grupo = item.get("grupo") or "-"
            checkout_label = item.get("checkout_label") or "-"
            timestamp = fmt_dt(item.get("timestamp"))

            incident_blocks.append(f"""
            <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:18px;margin-bottom:14px;">
                <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
                    <div style="font-size:16px;font-weight:bold;">{tipo}</div>
                    <div style="font-size:13px;color:#666;">{timestamp}</div>
                </div>

                <div style="font-size:13px;color:#666;margin-bottom:12px;">
                    <strong>gravidade:</strong> {gravidade}
                    &nbsp;•&nbsp;
                    <strong>status:</strong> {status}
                    &nbsp;•&nbsp;
                    <strong>grupo:</strong> {grupo}
                    &nbsp;•&nbsp;
                    <strong>checkout:</strong> {checkout_label}
                </div>

                <div style="font-size:15px;line-height:1.6;white-space:pre-wrap;">
                    <strong>Mensagem:</strong><br>{mensagem}
                </div>

                <div style="margin-top:12px;padding:12px;background:#fafafa;border:1px solid #ececec;border-radius:10px;">
                    <div style="font-size:13px;color:#666;margin-bottom:6px;"><strong>Detalhe</strong></div>
                    <div style="font-size:14px;line-height:1.6;white-space:pre-wrap;">{detalhe or '<span style="color:#666;">sem detalhe adicional</span>'}</div>
                </div>
            </div>
            """)

        incidents_html = "".join(incident_blocks)
    else:
        incidents_html = """
        <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:20px;color:#666;">
            Nenhum incidente encontrado.
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin Incidents</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:1150px;margin:0 auto;padding:24px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px;flex-wrap:wrap;">
                <div>
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin Incidents
                    </div>
                    <h1 style="margin:8px 0 0 0;font-size:32px;line-height:1.1;">Incidentes recentes</h1>
                    <p style="margin:10px 0 0 0;color:#555;">
                        Últimos 50 registros de <code>incidents</code>.
                    </p>
                </div>
                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    <a href="/admin?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        ← Admin
                    </a>
                    <a href="/admin/dashboard?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        Dashboard
                    </a>
                </div>
            </div>

            <div style="background:white;border-radius:16px;padding:18px 20px;border:1px solid #e6e6e6;margin-bottom:18px;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">
                    <div><strong>Horário local:</strong> {now_iso()}</div>
                    <div><strong>Banco:</strong> {"conectado" if has_database() and not db_error else ("erro" if db_error else "não configurado")}</div>
                    <div><strong>Limite:</strong> 50 incidents</div>
                </div>
                {f'<div style="margin-top:12px;color:#a33;"><strong>Erro DB:</strong> {db_error}</div>' if db_error else ''}
            </div>

            {incidents_html}
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


@app.route("/admin/guests", methods=["GET"])
@admin_required
def admin_guests():
    token = get_admin_token_from_request(request)
    guests = []
    db_error = ""

    if has_database():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            nome,
                            grupo,
                            checkin_date,
                            checkout_date,
                            checkout_time,
                            idioma,
                            observacoes,
                            perfil_hospede,
                            preferencias_json,
                            updated_at
                        FROM guests
                        ORDER BY updated_at DESC
                        LIMIT 30
                    """)
                    guests = cur.fetchall() or []
        except Exception as e:
            db_error = str(e)

    def fmt_dt(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(value)

    def fmt_date(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y")
        except Exception:
            return str(value)

    def fmt_time(value):
        if not value:
            return "-"
        try:
            return value.strftime("%H:%M")
        except Exception:
            return str(value)

    if guests:
        guest_blocks = []

        for item in guests:
            nome = item.get("nome") or "-"
            grupo = item.get("grupo") or "-"
            checkin_date = fmt_date(item.get("checkin_date"))
            checkout_date = fmt_date(item.get("checkout_date"))
            checkout_time = fmt_time(item.get("checkout_time"))
            idioma = item.get("idioma") or "-"
            observacoes = (item.get("observacoes") or "").replace("<", "&lt;").replace(">", "&gt;")
            perfil_hospede = item.get("perfil_hospede") or "-"
            preferencias = item.get("preferencias_json") or {}
            updated_at = fmt_dt(item.get("updated_at"))

            preferencias_html = "<span style='color:#666;'>nenhuma</span>"
            preferencias_count = 0

            if isinstance(preferencias, dict) and preferencias:
                preferencias_count = len(preferencias)
                preferencias_html = "<br>".join(
                    f"• {str(k)}: {str(v)}"
                    for k, v in preferencias.items()
                )

            guest_blocks.append(f"""
            <div style="background:white;border:1px solid #dddddd;border-radius:18px;padding:20px;margin-bottom:20px;">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:16px;padding-bottom:14px;border-bottom:1px solid #ececec;">
                    <div>
                        <div style="font-size:12px;letter-spacing:0.06em;color:#666;text-transform:uppercase;margin-bottom:6px;">
                            Guest
                        </div>
                        <div style="font-size:24px;font-weight:700;line-height:1.2;">
                            {nome}
                        </div>
                        <div style="margin-top:8px;font-size:13px;color:#666;">
                            Atualizado em: {updated_at}
                        </div>
                    </div>

                    <div style="background:#f7f7f7;border:1px solid #e4e4e4;border-radius:12px;padding:10px 12px;min-width:130px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;">Preferências</div>
                        <div style="margin-top:6px;font-size:22px;font-weight:bold;">{preferencias_count}</div>
                    </div>
                </div>

                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px;">
                    <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:14px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">Identidade</div>
                        <div><strong>Nome:</strong> {nome}</div>
                        <div><strong>Grupo:</strong> {grupo}</div>
                        <div><strong>Perfil:</strong> {perfil_hospede}</div>
                        <div><strong>Idioma:</strong> {idioma}</div>
                    </div>

                    <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:14px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">Estadia</div>
                        <div><strong>Check-in:</strong> {checkin_date}</div>
                        <div><strong>Check-out:</strong> {checkout_date}</div>
                        <div><strong>Horário de saída:</strong> {checkout_time}</div>
                    </div>

                    <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:14px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">Contexto</div>
                        <div style="font-size:14px;line-height:1.6;white-space:pre-wrap;">{observacoes or '<span style="color:#666;">sem observações</span>'}</div>
                    </div>

                    <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:14px;">
                        <div style="font-size:12px;color:#666;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px;">Preferências</div>
                        <div style="font-size:14px;line-height:1.6;">{preferencias_html}</div>
                    </div>
                </div>
            </div>
            """)

        guests_html = "".join(guest_blocks)
    else:
        guests_html = """
        <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:20px;color:#666;">
            Nenhum guest encontrado.
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin Guests</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:1150px;margin:0 auto;padding:24px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px;flex-wrap:wrap;">
                <div>
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin Guests
                    </div>
                    <h1 style="margin:8px 0 0 0;font-size:32px;line-height:1.1;">Guests persistidos</h1>
                    <p style="margin:10px 0 0 0;color:#555;">
                        Últimos 30 registros de <code>guests</code>.
                    </p>
                </div>
                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    <a href="/admin?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        ← Admin
                    </a>
                    <a href="/admin/dashboard?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        Dashboard
                    </a>
                </div>
            </div>

            <div style="background:white;border-radius:16px;padding:18px 20px;border:1px solid #e6e6e6;margin-bottom:18px;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">
                    <div><strong>Horário local:</strong> {now_iso()}</div>
                    <div><strong>Banco:</strong> {"conectado" if has_database() and not db_error else ("erro" if db_error else "não configurado")}</div>
                    <div><strong>Limite:</strong> 30 guests</div>
                </div>
                {f'<div style="margin-top:12px;color:#a33;"><strong>Erro DB:</strong> {db_error}</div>' if db_error else ''}
            </div>

            {guests_html}
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


@app.route("/admin/intents", methods=["GET"])
@admin_required
def admin_intents():
    token = get_admin_token_from_request(request)
    intents = []
    db_error = ""

    if has_database():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            intent,
                            topic,
                            timestamp
                        FROM intent_events
                        ORDER BY timestamp DESC
                        LIMIT 80
                    """)
                    intents = cur.fetchall() or []
        except Exception as e:
            db_error = str(e)

    def fmt_dt(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(value)

    if intents:
        intent_blocks = []

        for item in intents:
            intent_value = item.get("intent") or "-"
            topic = item.get("topic") or "-"
            timestamp = fmt_dt(item.get("timestamp"))

            intent_blocks.append(f"""
            <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:18px;margin-bottom:14px;">
                <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
                    <div style="font-size:16px;font-weight:bold;">{intent_value}</div>
                    <div style="font-size:13px;color:#666;">{timestamp}</div>
                </div>

                <div style="font-size:14px;line-height:1.6;">
                    <strong>topic:</strong> {topic}
                </div>
            </div>
            """)

        intents_html = "".join(intent_blocks)
    else:
        intents_html = """
        <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:20px;color:#666;">
            Nenhuma intent encontrada.
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin Intents</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:1050px;margin:0 auto;padding:24px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px;flex-wrap:wrap;">
                <div>
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin Intents
                    </div>
                    <h1 style="margin:8px 0 0 0;font-size:32px;line-height:1.1;">Intents recentes</h1>
                    <p style="margin:10px 0 0 0;color:#555;">
                        Últimos 80 registros de <code>intent_events</code>.
                    </p>
                </div>
                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    <a href="/admin?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        ← Admin
                    </a>
                    <a href="/admin/dashboard?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        Dashboard
                    </a>
                </div>
            </div>

            <div style="background:white;border-radius:16px;padding:18px 20px;border:1px solid #e6e6e6;margin-bottom:18px;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">
                    <div><strong>Horário local:</strong> {now_iso()}</div>
                    <div><strong>Banco:</strong> {"conectado" if has_database() and not db_error else ("erro" if db_error else "não configurado")}</div>
                    <div><strong>Limite:</strong> 80 intents</div>
                </div>
                {f'<div style="margin-top:12px;color:#a33;"><strong>Erro DB:</strong> {db_error}</div>' if db_error else ''}
            </div>

            {intents_html}
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


@app.route("/admin/insights", methods=["GET"])
@admin_required
def admin_insights():
    token = get_admin_token_from_request(request)
    insights = []
    db_error = ""

    if has_database():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            insight_key,
                            source_message,
                            timestamp
                        FROM guest_insight_events
                        ORDER BY timestamp DESC
                        LIMIT 80
                    """)
                    insights = cur.fetchall() or []
        except Exception as e:
            db_error = str(e)

    def fmt_dt(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(value)

    if insights:
        insight_blocks = []

        for item in insights:
            insight_key = item.get("insight_key") or "-"
            source_message = (item.get("source_message") or "").replace("<", "&lt;").replace(">", "&gt;")
            timestamp = fmt_dt(item.get("timestamp"))

            insight_blocks.append(f"""
            <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:18px;margin-bottom:14px;">
                <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
                    <div style="font-size:16px;font-weight:bold;">{insight_key}</div>
                    <div style="font-size:13px;color:#666;">{timestamp}</div>
                </div>

                <div style="margin-top:8px;padding:12px;background:#fafafa;border:1px solid #ececec;border-radius:10px;">
                    <div style="font-size:13px;color:#666;margin-bottom:6px;"><strong>source_message</strong></div>
                    <div style="font-size:14px;line-height:1.6;white-space:pre-wrap;">{source_message or '<span style="color:#666;">sem mensagem de origem</span>'}</div>
                </div>
            </div>
            """)

        insights_html = "".join(insight_blocks)
    else:
        insights_html = """
        <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:20px;color:#666;">
            Nenhum insight encontrado.
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin Insights</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:1050px;margin:0 auto;padding:24px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px;flex-wrap:wrap;">
                <div>
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin Insights
                    </div>
                    <h1 style="margin:8px 0 0 0;font-size:32px;line-height:1.1;">Insights recentes</h1>
                    <p style="margin:10px 0 0 0;color:#555;">
                        Últimos 80 registros de <code>guest_insight_events</code>.
                    </p>
                </div>
                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    <a href="/admin?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        ← Admin
                    </a>
                    <a href="/admin/dashboard?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        Dashboard
                    </a>
                </div>
            </div>

            <div style="background:white;border-radius:16px;padding:18px 20px;border:1px solid #e6e6e6;margin-bottom:18px;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">
                    <div><strong>Horário local:</strong> {now_iso()}</div>
                    <div><strong>Banco:</strong> {"conectado" if has_database() and not db_error else ("erro" if db_error else "não configurado")}</div>
                    <div><strong>Limite:</strong> 80 insights</div>
                </div>
                {f'<div style="margin-top:12px;color:#a33;"><strong>Erro DB:</strong> {db_error}</div>' if db_error else ''}
            </div>

            {insights_html}
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


@app.route("/admin/usage", methods=["GET"])
@admin_required
def admin_usage():
    token = get_admin_token_from_request(request)
    usage_events = []
    db_error = ""

    if has_database():
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            topic,
                            used_followup,
                            user_text,
                            assistant_text,
                            timestamp
                        FROM usage_events
                        ORDER BY timestamp DESC
                        LIMIT 80
                    """)
                    usage_events = cur.fetchall() or []
        except Exception as e:
            db_error = str(e)

    def fmt_dt(value):
        if not value:
            return "-"
        try:
            return value.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(value)

    if usage_events:
        usage_blocks = []

        for item in usage_events:
            topic = item.get("topic") or "-"
            used_followup = "sim" if item.get("used_followup") else "não"
            user_text = (item.get("user_text") or "").replace("<", "&lt;").replace(">", "&gt;")
            assistant_text = (item.get("assistant_text") or "").replace("<", "&lt;").replace(">", "&gt;")
            timestamp = fmt_dt(item.get("timestamp"))

            usage_blocks.append(f"""
            <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:18px;margin-bottom:14px;">
                <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
                    <div style="font-size:16px;font-weight:bold;">{topic}</div>
                    <div style="font-size:13px;color:#666;">{timestamp}</div>
                </div>

                <div style="font-size:14px;line-height:1.6;margin-bottom:12px;">
                    <strong>used_followup:</strong> {used_followup}
                </div>

                <div style="margin-top:8px;padding:12px;background:#fafafa;border:1px solid #ececec;border-radius:10px;">
                    <div style="font-size:13px;color:#666;margin-bottom:6px;"><strong>user_text</strong></div>
                    <div style="font-size:14px;line-height:1.6;white-space:pre-wrap;">{user_text or '<span style="color:#666;">sem texto do usuário</span>'}</div>
                </div>

                <div style="margin-top:12px;padding:12px;background:#fafafa;border:1px solid #ececec;border-radius:10px;">
                    <div style="font-size:13px;color:#666;margin-bottom:6px;"><strong>assistant_text</strong></div>
                    <div style="font-size:14px;line-height:1.6;white-space:pre-wrap;">{assistant_text or '<span style="color:#666;">sem texto do assistente</span>'}</div>
                </div>
            </div>
            """)

        usage_html = "".join(usage_blocks)
    else:
        usage_html = """
        <div style="background:white;border:1px solid #e6e6e6;border-radius:16px;padding:20px;color:#666;">
            Nenhum usage event encontrado.
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Gepetto Admin Usage</title>
    </head>
    <body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;color:#111;">
        <div style="max-width:1100px;margin:0 auto;padding:24px;">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:18px;flex-wrap:wrap;">
                <div>
                    <div style="font-size:12px;letter-spacing:0.08em;color:#666;text-transform:uppercase;">
                        Gepetto • Admin Usage
                    </div>
                    <h1 style="margin:8px 0 0 0;font-size:32px;line-height:1.1;">Usage recente</h1>
                    <p style="margin:10px 0 0 0;color:#555;">
                        Últimos 80 registros de <code>usage_events</code>.
                    </p>
                </div>
                <div style="display:flex;gap:10px;flex-wrap:wrap;">
                    <a href="/admin?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        ← Admin
                    </a>
                    <a href="/admin/dashboard?token={token}" style="text-decoration:none;color:#111;background:#fff;border:1px solid #ddd;padding:10px 14px;border-radius:10px;">
                        Dashboard
                    </a>
                </div>
            </div>

            <div style="background:white;border-radius:16px;padding:18px 20px;border:1px solid #e6e6e6;margin-bottom:18px;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">
                    <div><strong>Horário local:</strong> {now_iso()}</div>
                    <div><strong>Banco:</strong> {"conectado" if has_database() and not db_error else ("erro" if db_error else "não configurado")}</div>
                    <div><strong>Limite:</strong> 80 usage events</div>
                </div>
                {f'<div style="margin-top:12px;color:#a33;"><strong>Erro DB:</strong> {db_error}</div>' if db_error else ''}
            </div>

            {usage_html}
        </div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")
        


@app.route("/db-init", methods=["GET"])
def db_init():
    try:
        if not has_database():
            return json_response({"ok": False, "message": "DATABASE_URL não configurado"}, status=500)

        ddl_statements = [
            """
            CREATE EXTENSION IF NOT EXISTS "pgcrypto";
            """,
            """
            CREATE TABLE IF NOT EXISTS guests (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                nome TEXT NOT NULL DEFAULT '',
                grupo TEXT NOT NULL DEFAULT '',
                checkin_date DATE,
                checkout_date DATE,
                checkout_time TIME,
                idioma TEXT NOT NULL DEFAULT 'pt',
                observacoes TEXT NOT NULL DEFAULT '',
                perfil_hospede TEXT NOT NULL DEFAULT 'neutro',
                preferencias_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS session_states (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                guest_id UUID NOT NULL REFERENCES guests(id) ON DELETE CASCADE,
                last_topic TEXT NOT NULL DEFAULT '',
                last_intent TEXT NOT NULL DEFAULT '',
                last_followup_hint TEXT NOT NULL DEFAULT '',
                last_recommendation_type TEXT NOT NULL DEFAULT '',
                last_recommendation_name TEXT NOT NULL DEFAULT '',
                last_entity_name TEXT NOT NULL DEFAULT '',
                last_entity_category TEXT NOT NULL DEFAULT '',
                pending_bruno_contact BOOLEAN NOT NULL DEFAULT FALSE,
                pending_incident_context BOOLEAN NOT NULL DEFAULT FALSE,
                last_incident_context TEXT NOT NULL DEFAULT '',
                active_recommendation_type TEXT NOT NULL DEFAULT '',
                active_recommendation_options_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                active_recommendation_index INTEGER NOT NULL DEFAULT 0,
                active_recommendation_updated_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (guest_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS conversation_threads (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                guest_id UUID NOT NULL REFERENCES guests(id) ON DELETE CASCADE,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS conversation_messages (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                thread_id UUID REFERENCES conversation_threads(id) ON DELETE CASCADE,
                guest_id UUID REFERENCES guests(id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                text TEXT NOT NULL,
                topic TEXT NOT NULL DEFAULT '',
                meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS conversation_logs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                guest_id UUID REFERENCES guests(id) ON DELETE SET NULL,
                thread_id UUID REFERENCES conversation_threads(id) ON DELETE SET NULL,
                guest_nome TEXT NOT NULL DEFAULT '',
                message TEXT NOT NULL,
                intent TEXT NOT NULL DEFAULT '',
                response TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS incidents (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                guest_id UUID REFERENCES guests(id) ON DELETE SET NULL,
                thread_id UUID REFERENCES conversation_threads(id) ON DELETE SET NULL,
                tipo TEXT NOT NULL,
                gravidade TEXT NOT NULL DEFAULT '',
                mensagem TEXT NOT NULL,
                detalhe TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'aberto',
                grupo TEXT NOT NULL DEFAULT '',
                checkout_label TEXT NOT NULL DEFAULT '',
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS intent_events (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                guest_id UUID REFERENCES guests(id) ON DELETE SET NULL,
                thread_id UUID REFERENCES conversation_threads(id) ON DELETE SET NULL,
                intent TEXT NOT NULL,
                topic TEXT NOT NULL DEFAULT '',
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS guest_insight_events (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                guest_id UUID REFERENCES guests(id) ON DELETE SET NULL,
                thread_id UUID REFERENCES conversation_threads(id) ON DELETE SET NULL,
                insight_key TEXT NOT NULL,
                source_message TEXT NOT NULL DEFAULT '',
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS usage_events (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                guest_id UUID REFERENCES guests(id) ON DELETE SET NULL,
                thread_id UUID REFERENCES conversation_threads(id) ON DELETE SET NULL,
                topic TEXT NOT NULL DEFAULT '',
                used_followup BOOLEAN NOT NULL DEFAULT FALSE,
                user_text TEXT NOT NULL DEFAULT '',
                assistant_text TEXT NOT NULL DEFAULT '',
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        ]

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for stmt in ddl_statements:
                    cur.execute(stmt)
            conn.commit()

        return json_response({"ok": True, "message": "db init ok"})
    except Exception as e:
        return json_response({"ok": False, "message": str(e)}, status=500)
        

@app.route("/db-stats", methods=["GET"])
def db_stats():
    try:
        if not has_database():
            return json_response({"ok": False, "message": "DATABASE_URL não configurado"}, status=500)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS total FROM conversation_logs;")
                conversation_logs = cur.fetchone()["total"]

                cur.execute("SELECT COUNT(*) AS total FROM intent_events;")
                intent_events = cur.fetchone()["total"]

                cur.execute("SELECT COUNT(*) AS total FROM guest_insight_events;")
                guest_insight_events = cur.fetchone()["total"]

                cur.execute("SELECT COUNT(*) AS total FROM usage_events;")
                usage_events = cur.fetchone()["total"]

                cur.execute("SELECT COUNT(*) AS total FROM incidents;")
                incidents = cur.fetchone()["total"]

        return json_response({
            "ok": True,
            "conversation_logs": conversation_logs,
            "intent_events": intent_events,
            "guest_insight_events": guest_insight_events,
            "usage_events": usage_events,
            "incidents": incidents
        })
    except Exception as e:
        return json_response({"ok": False, "message": str(e)}, status=500)


@app.route("/db-check", methods=["GET"])
def db_check():
    try:
        if not has_database():
            return json_response({"ok": False, "message": "DATABASE_URL não configurado"}, status=500)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok;")
                row = cur.fetchone()

        return json_response({"ok": True, "db": row["ok"]})
    except Exception as e:
        return json_response({"ok": False, "message": str(e)}, status=500)
    

@app.route("/db-guest", methods=["GET"])
def db_guest():
    try:
        data = db_get_latest_guest()
        if not data:
            return json_response({"ok": False, "message": "nenhum guest encontrado"}, status=404)

        return json_response({"ok": True, "guest": data})
    except Exception as e:
        return json_response({"ok": False, "message": str(e)}, status=500)

@app.route("/db-session", methods=["GET"])
def db_session():
    try:
        data = db_get_latest_session_state()
        if not data:
            return json_response({"ok": False, "message": "nenhuma session encontrada"}, status=404)

        return json_response({"ok": True, "session": data})
    except Exception as e:
        return json_response({"ok": False, "message": str(e)}, status=500)
    
@app.route("/db-memory", methods=["GET"])
def db_memory():
    try:
        data = db_get_recent_conversation_messages()
        if not data:
            return json_response({"ok": False, "message": "nenhuma memory encontrada"}, status=404)

        return json_response({
            "ok": True,
            "total_messages": len(data.get("messages", [])),
            "messages": data.get("messages", [])[-10:]
        })
    except Exception as e:
        return json_response({"ok": False, "message": str(e)}, status=500)    


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
    