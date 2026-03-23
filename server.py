from flask import Flask, request, send_from_directory, Response
import os
import json
import random
import re
import unicodedata
from pathlib import Path
from datetime import datetime

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
        "checkout": "",
        "idioma": "pt",
        "observacoes": "",
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

    return data


def save_guest(data):
    write_json(GUEST_FILE, data)


def default_memory():
    return {"messages": []}


def load_memory():
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
        "timestamp": datetime.now().isoformat(timespec="seconds")
    })
    memory["messages"] = memory["messages"][-120:]
    save_memory(memory)


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
    data = read_json(SESSION_FILE, None)
    if data is None:
        data = default_session()
        save_session(data)

    for k, v in default_session().items():
        data.setdefault(k, v)

    return data


def save_session(data):
    write_json(SESSION_FILE, data)


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
    sess["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_session(sess)


def set_bruno_pending(value: bool):
    sess = load_session()
    sess["pending_bruno_contact"] = value
    sess["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_session(sess)


def set_incident_pending(value: bool, context: str = ""):
    sess = load_session()
    sess["pending_incident_context"] = value
    if context:
        sess["last_incident_context"] = context
    elif not value:
        sess["last_incident_context"] = ""
    sess["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_session(sess)


def set_last_entity(name: str, category: str = ""):
    sess = load_session()
    sess["last_entity_name"] = name or ""
    sess["last_entity_category"] = category or ""
    sess["updated_at"] = datetime.now().isoformat(timespec="seconds")
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
    sess["active_recommendation_updated_at"] = datetime.now().isoformat(timespec="seconds")
    sess["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_session(sess)


def clear_active_recommendations():
    sess = load_session()
    sess["active_recommendation_type"] = ""
    sess["active_recommendation_options"] = []
    sess["active_recommendation_index"] = 0
    sess["active_recommendation_updated_at"] = ""
    sess["updated_at"] = datetime.now().isoformat(timespec="seconds")
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
        sess["updated_at"] = datetime.now().isoformat(timespec="seconds")
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
            sess["updated_at"] = datetime.now().isoformat(timespec="seconds")
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


# =========================
# LOGS / STATS
# =========================

def log_conversation(guest, message, intent, response):
    logs = read_json(LOG_FILE, [])
    logs.append({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "guest": guest.get("nome", ""),
        "message": message,
        "intent": intent,
        "response": response[:800]
    })
    logs = logs[-3000:]
    write_json(LOG_FILE, logs)


def update_intent_stats(intent):
    stats = read_json(INTENT_FILE, {})
    key = intent or "fallback"
    stats[key] = stats.get(key, 0) + 1
    write_json(INTENT_FILE, stats)


def update_guest_insights(message):
    insights = read_json(INSIGHT_FILE, {})
    msg = normalize_text(message)

    def inc(key):
        insights[key] = insights.get(key, 0) + 1

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
    if has_any(msg, ["restaurante", "restaurantes", "comer", "jantar", "almoco", "almoço", "pizza"]):
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

    hoje = datetime.now().strftime("%Y-%m-%d")
    agora = datetime.now().strftime("%H:%M:%S")

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
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "manhã"
    if 12 <= hour < 18:
        return "tarde"
    return "noite"


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
    if has_any(text_n, ["bar", "bares", "drink", "drinks", "cerveja", "noite"]):
        inc("noite")
    if has_any(text_n, ["restaurante", "restaurantes", "comer", "jantar", "almoco", "almoço", "pizza"]):
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
    checkout = guest.get("checkout", "").strip() or "-"
    agora = datetime.now().isoformat(timespec="seconds")

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


def filter_passeios_by_momento(items, target):
    target_n = normalize_text(target)
    result = []

    for p in items:
        momentos = p.get("melhor_momento", [])
        momentos_n = [normalize_text(i) for i in momentos if isinstance(i, str)]
        if target_n in momentos_n:
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
        "Burguer King": ["burger king", "bk"],
        "Madero & Jeronimo Burger Guarujá": ["madero", "jeronimo", "jerônimo", "jeronimo burger", "jeronimo track"],
        "Alcide’s": ["alcides", "alcide's", "alcides restaurante"],
        "Alcides Pizzaria": ["alcides pizzaria", "pizzaria alcides"],
        "Thai Lounge Bar": ["thai lounge"],
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
        "Parque Ecológico Renan C. Teixeira": ["parque ecológico", "parque ecologico", "parque renan", "parque"]
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


def should_use_entity_detail_mode(text_raw, inferred_intent="", last_topic=""):
    text_n = normalize_text(text_raw)
    field = get_requested_detail_field(text_raw)

    if not field:
        return False

    if last_topic == "praia" and has_any(text_n, [
        "horario", "horário", "horarios", "horários",
        "que horas", "que horas funciona",
        "funciona que horas", "ate que horas", "até que horas",
        "onde fica", "localizacao", "localização",
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
            "servico de praia", "serviço de praia",
            "horario", "horário", "horarios", "horários",
            "que horas", "que horas funciona",
            "funciona que horas", "ate que horas", "até que horas",
            "como funciona", "funciona",
            "e o horario", "e o horário",
            "e o endereco", "e o endereço",
            "endereco", "endereço"
        ]):
            return "praia"

    if last_topic == "saude":
        if has_any(text_n, [
            "farmacia", "farmácia", "farmacias", "farmácias",
            "upa", "hospital", "todos", "todas",
            "24h", "vinte e quatro", "entrega", "delivery"
        ]):
            return "saude"

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
        "shopping", "cinema", "mirante", "feira", "chuva", "familia", "família",
        "tradicional", "classico", "clássico",
        "e o endereco", "e o endereço", "e o horario", "e o horário",
        "e entrega", "e delivery"
    ]):
        return last_topic

    very_short_contextual = [
        "qual", "melhor", "barato", "perto", "especial",
        "completo", "tranquilo", "animado", "leve",
        "rapido", "rápido", "em conta",
        "esse", "essa", "entao", "então", "vc indica",
        "localizacao", "localização", "horario", "horário", "horarios", "horários",
        "servico", "serviço", "envie", "manda", "pode mandar",
        "farmacia", "farmácia", "upa", "hospital", "todos", "todas",
        "pizza", "japones", "japonês", "doce", "vista",
        "que horas", "como funciona", "shopping", "cinema", "mirante", "feira",
        "o outro", "a outra", "outro", "outra",
        "esse lugar", "essa opcao", "essa opção",
        "esse ai", "esse aí", "essa ai", "essa aí",
        "qual deles", "qual delas", "tem outro", "tem outra",
        "endereco", "endereço",
        "tradicional", "familia", "família", "chuva"
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
        "o outro", "a outra", "outro", "outra",
        "esse lugar", "essa opcao", "essa opção",
        "esse ai", "esse aí", "essa ai", "essa aí",
        "qual deles", "qual delas",
        "endereco", "endereço", "horario", "horário", "horarios", "horários", "delivery"
    ]
    if text_n in exact_short:
        return True

    return False


def is_ambiguous_reference_message(text_raw):
    text_n = normalize_text(text_raw)

    exacts = {
        "esse", "essa", "esse ai", "esse aí", "essa ai", "essa aí",
        "o outro", "a outra", "outro", "outra",
        "qual", "qual deles", "qual delas",
        "endereco", "endereço",
        "horario", "horário", "horarios", "horários",
        "entrega", "delivery",
        "compensa", "vale a pena"
    }

    if text_n in exacts:
        return True

    return False


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
        "restaurantes": ["restaurante", "restaurantes", "jantar", "comer", "pizza", "japones", "japonês", "sushi"],
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
        "compensa", "vale a pena"
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

    if text_n in ["qual", "qual deles", "qual delas", "compensa", "vale a pena"]:
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
        "caixa de som", "ruido", "ruído", "areia", "lixo", "fumar", "festa", "festas",
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
        "pizza", "japones", "japonês", "sushi", "doce", "sobremesa",
        "kopenhagen", "cacau show", "mcdonald", "burger", "burguer king",
        "alcides", "thai lounge", "atlantico signature", "atlântico signature", "dati",
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
        "checkout": guest.get("checkout", ""),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
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
        f"Horário: {datetime.now().isoformat(timespec='seconds')}"
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
        "checkout": guest.get("checkout", ""),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "status": "complemento"
    }
    append_incident(payload)
    return payload


def notify_incident_context_to_telegram(guest, raw_message, detail):
    nome = guest.get("nome", "").strip() or "Hóspede sem nome definido"
    agora = datetime.now().isoformat(timespec="seconds")

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
        return f"Não. {fumar}"

    if has_any(text_n, ["festa", "festas", "evento", "eventos", "pode fazer festa", "posso fazer festa"]):
        return f"Não. {festas}"

    if has_any(text_n, ["reciclagem", "reciclavel", "reciclável", "lixo", "onde joga o lixo", "coleta", "onde descarta o lixo"]):
        return f"Sobre o lixo: {lixo}"

    if has_any(text_n, [
        "silencio", "silêncio", "barulho", "som alto", "musica alta", "música alta",
        "caixa de som", "ruido", "ruído", "perturbar", "incomodar vizinhos",
        "horario de silencio", "horário de silêncio", "pode musica", "pode música",
        "posso por musica", "posso pôr música", "posso colocar musica", "posso colocar música",
        "pode som alto", "posso som alto"
    ]):
        return f"O horário de silêncio é {silencio}."

    if has_any(text_n, ["areia", "lava pes", "lava-pes", "lava pés", "e a areia"]):
        return f"Sobre a areia: {areia}"

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


def get_praia_reply():
    k = knowledge()
    praia = k.get("praia", {})
    servico = praia.get("servico_praia", {})
    melhor_horario = praia.get("melhor_horario", "")
    dica = praia.get("dica", "")
    serv_obs = servico.get("observacao", "")

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
        f"{gepetto_line('praia_5')}\n\n"
        f"A praia fica a {praia.get('distancia', '280 metros (4 a 5 minutos a pé)')}.\n"
        f"O serviço de praia funciona das {servico.get('horario', '9h às 17h')}.\n"
        f"Ele fica {servico.get('localizacao', 'ao lado do Thai Lounge, em frente ao Casa Grande Hotel')}.\n\n"
        f"{servico.get('como_funciona', 'Os itens ficam montados na areia durante o horário do serviço.')}"
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

    rapido = find_item_by_type(restaurantes, "rapido")
    especial = find_item_by_type(restaurantes, "especial")
    tradicional = find_item_by_type(restaurantes, "tradicional")
    japones = find_item_by_type(restaurantes, "japones")
    doce = find_item_by_type(restaurantes, "doce")
    pizza = find_item_by_type(restaurantes, "pizza")
    vista = find_item_by_type(restaurantes, "vista")

    if has_any(text_n, ["barato", "economico", "econômico", "simples", "rapido", "rápido", "leve", "em conta", "mais em conta"]):
        item = rapido or {}
        nome = item.get("nome", "McDonald's Enseada")
        dist = format_distance(item.get("distancia", "5 minutos"))
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items([item, tradicional, especial, japones, pizza, doce, vista]),
            current_name=nome
        )
        set_last_entity(nome, "restaurantes")
        reply = f"Se a ideia for algo mais prático, eu iria no **{nome}**, que fica a cerca de **{dist}**."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        reply += "\n\nSe quiser, eu também posso te levar para algo mais tradicional, pizza, japonês ou uma opção mais especial."
        return reply

    if has_any(text_n, ["especial", "romantico", "romântico", "sofisticado", "premium"]):
        item = especial or {}
        nome = item.get("nome", "Thai Lounge Bar")
        alt = (tradicional or {}).get("nome", "Alcide’s")
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items([item, tradicional, japones, pizza, vista]),
            current_name=nome
        )
        set_last_entity(nome, "restaurantes")
        reply = f"Se você quiser algo mais especial, o **{nome}** costuma ser uma escolha muito boa ✨"
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        reply += f"\n\nSe preferir uma linha mais clássica e tradicional, o **{alt}** também costuma funcionar muito bem."
        return reply

    if has_any(text_n, ["tradicional", "classico", "clássico", "frutos do mar"]):
        item = tradicional or {}
        nome = item.get("nome", "Alcide’s")
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items([item, especial, japones, pizza, vista]),
            current_name=nome
        )
        set_last_entity(nome, "restaurantes")
        reply = f"Se a ideia for algo mais tradicional, uma boa referência é o **{nome}**."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        return reply

    if has_any(text_n, ["japones", "japonês", "japonesa", "sushi"]):
        item = japones or {}
        nome = item.get("nome", "Sushi Katoshi 23")
        dist = format_distance(item.get("distancia", "4 minutos"))
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items([item, especial, tradicional, pizza, doce]),
            current_name=nome
        )
        set_last_entity(nome, "restaurantes")
        reply = f"Se a vontade for japonês 🍣\n\nUma boa referência é o **{nome}**, que fica a cerca de **{dist}**."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        return reply

    if has_any(text_n, ["pizza", "pizzaria"]):
        item = pizza or {}
        nome = item.get("nome", "Alcides Pizzaria")
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items([item, tradicional, especial, japones]),
            current_name=nome
        )
        set_last_entity(nome, "restaurantes")
        reply = f"Se a ideia for pizza 🍕\n\nUma boa pedida é a **{nome}**."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        return reply

    if has_any(text_n, ["hamburguer", "hambúrguer", "lanche", "mcdonald", "mcdonald's", "burger", "burguer king", "burger king", "madero", "jeronimo", "jerônimo"]):
        item = rapido or {}
        nome = item.get("nome", "McDonald's Enseada")
        dist = format_distance(item.get("distancia", "5 minutos"))
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items([item, tradicional, especial, japones, pizza, doce, vista]),
            current_name=nome
        )
        set_last_entity(nome, "restaurantes")
        return f"Se você quiser algo mais rápido 🍔\n\nO **{nome}** fica a cerca de **{dist}**."

    if has_any(text_n, ["doce", "sobremesa", "chocolate", "kopenhagen", "cacau show"]):
        item = doce or {}
        nome = item.get("nome", "Kopenhagen Enseada")
        dist = format_distance(item.get("distancia", "4 minutos"))
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items([item, especial, japones, pizza]),
            current_name=nome
        )
        set_last_entity(nome, "restaurantes")
        reply = f"Se a ideia for um doce ou uma parada gostosa 🍫\n\nA **{nome}** fica a cerca de **{dist}**."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        return reply

    if has_any(text_n, ["vista", "mirante", "lugar bonito", "lugar com vista"]):
        item = vista or {}
        nome = item.get("nome", "Restaurante Mirante Bela Vista")
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        set_active_recommendations(
            "restaurantes",
            names_from_items([item, especial, tradicional, japones, pizza]),
            current_name=nome
        )
        set_last_entity(nome, "restaurantes")
        reply = f"Se a ideia for um lugar com vista ✨\n\nO **{nome}** pode funcionar muito bem."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        return reply

    if has_any(text_n, ["restaurantes", "outro restaurante", "outros restaurantes", "opcoes de restaurante", "opções de restaurante", "todos", "todas"]):
        if restaurantes:
            linhas = []
            for r in restaurantes:
                nome = r.get("nome", "")
                tipo = normalize_text(r.get("tipo", ""))
                perfil = r.get("perfil", "")
                dist = format_distance(r.get("distancia", ""))
                if tipo == "tradicional":
                    linhas.append(f"• **{nome}** → {perfil or 'clássico e tradicional'}")
                elif tipo == "especial":
                    linhas.append(f"• **{nome}** → {perfil or 'experiência mais especial'} ✨")
                elif tipo == "japones":
                    linhas.append(f"• **{nome}** → {perfil or 'comida japonesa'}, a cerca de **{dist or '4 minutos'}** 🍣")
                elif tipo == "rapido":
                    linhas.append(f"• **{nome}** → {perfil or 'opção prática'}, a cerca de **{dist or '5 minutos'}** 🍔")
                elif tipo == "pizza":
                    linhas.append(f"• **{nome}** → {perfil or 'boa pedida para pizza'} 🍕")
                elif tipo == "doce":
                    linhas.append(f"• **{nome}** → {perfil or 'chocolateria ou sobremesa'} 🍫")
                elif tipo == "vista":
                    linhas.append(f"• **{nome}** → {perfil or 'experiência com vista'} 🌅")
                else:
                    linhas.append(f"• **{nome}**")

            set_active_recommendations(
                "restaurantes",
                names_from_items(restaurantes),
                current_name=restaurantes[0].get("nome", "") if restaurantes else ""
            )
            return (
                "Claro 😊\n\n"
                "Aqui vão algumas boas referências de restaurantes por perto:\n\n"
                + "\n".join(linhas)
                + "\n\nSe quiser, eu também posso filtrar isso para algo mais **rápido**, mais **especial**, mais **tradicional**, **pizza**, **japonês** ou com **vista**."
            )

    if restaurantes:
        linhas = []
        for r in restaurantes[:6]:
            nome = r.get("nome", "")
            tipo = normalize_text(r.get("tipo", ""))
            perfil = r.get("perfil", "")
            if tipo == "especial":
                linhas.append(f"• **{nome}** → {perfil} ✨")
            else:
                linhas.append(f"• **{nome}** → {perfil or 'boa opção por perto'}")

        set_active_recommendations(
            "restaurantes",
            names_from_items(restaurantes[:6]),
            current_name=restaurantes[0].get("nome", "") if restaurantes else ""
        )
        return (
            "Claro 😊\n\n"
            "Aqui vão algumas boas referências por perto:\n\n"
            + "\n".join(linhas)
            + "\n\nSe quiser, eu posso afinar isso por estilo e te deixar só as opções que mais fazem sentido."
        )

    return "Posso te ajudar com restaurantes 😊 Se quiser, me diga se você procura algo rápido, especial, tradicional, japonês, pizza ou todos."


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

    if has_any(text_n, ["shopping", "shoppings"]):
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


def get_zelador_reply():
    return get_apoio_predio_reply()


def get_checkout_reply(guest):
    checkout = guest.get("checkout") or knowledge().get("apartamento", {}).get("checkout", "11h")
    return f"O check-out está configurado para: **{checkout}** 😊"


def get_checkout_aviso_reply(guest):
    checkout = guest.get("checkout") or knowledge().get("apartamento", {}).get("checkout", "11h")
    return (
        f"{get_gepetto_checkout_line()}\n\n"
        "• Verifique se janelas e porta de entrada ficarão travadas\n"
        "• Favor retirar o lixo\n"
        "• Apague as luzes e desligue os ventiladores\n"
        "• Devolva as chaves na portaria do prédio\n"
        "• Não deixem louça suja\n\n"
        f"O check-out está configurado para **{checkout}**."
    )


def get_bruno_reply():
    set_bruno_pending(True)
    return get_gepetto_bruno_intro()


def notify_bruno_request(guest, raw_message=""):
    nome = guest.get("nome", "").strip() or "Hóspede sem nome definido"
    grupo = guest.get("grupo", "").strip() or "-"
    checkout = guest.get("checkout", "").strip() or "-"
    agora = datetime.now().isoformat(timespec="seconds")

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


def get_acqua_mundo_reply():
    return get_passeios_reply("acqua mundo")


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


def get_roteiro_reply(guest):
    parte_do_dia = current_time_label()
    grupo = guest_group_label(guest)
    last_pref = top_guest_preference(guest)

    if parte_do_dia == "manhã":
        if grupo == "família":
            return (
                "Se eu fosse montar um roteiro leve para hoje 😊\n\n"
                "☀️ **Manhã**\n"
                "• aproveitar a praia e o serviço montado\n\n"
                "🍽️ **Almoço**\n"
                "• algo tradicional e confortável\n\n"
                "🌤️ **Tarde**\n"
                "• descanso ou passeio leve\n\n"
                "🌙 **Noite**\n"
                "• jantar tranquilo"
            )
        if last_pref == "surf":
            return (
                "Se eu fosse montar um plano pro seu estilo hoje 🌊\n\n"
                "☀️ **Manhã**\n"
                "• checar o mar e aproveitar a praia cedo\n\n"
                "🍽️ **Almoço**\n"
                "• algo prático e sem pressa\n\n"
                "🌙 **Noite**\n"
                "• jantar gostoso e descanso"
            )
        return (
            "Se eu fosse montar um roteiro leve para hoje, faria assim 😊\n\n"
            "☀️ **Manhã**\n"
            "• aproveitar a praia e o serviço montado\n\n"
            "🍽️ **Almoço**\n"
            "• algo tradicional\n\n"
            "🌤️ **Tarde**\n"
            "• descanso, caminhada leve ou mercado rápido se precisar de algo\n\n"
            "🌙 **Noite**\n"
            "• jantar mais especial"
        )

    if parte_do_dia == "tarde":
        return (
            "Se quiser um roteiro para o resto do dia 😄\n\n"
            "🌤️ **Agora à tarde**\n"
            "• praia, descanso ou alguma saída leve pela região\n\n"
            "🍽️ **Fim de tarde / noite**\n"
            "• jantar em algo mais tradicional ou mais especial\n\n"
            "Se quiser, eu adapto isso para algo mais romântico, mais família ou mais prático 😉"
        )

    return (
        "Se quiser um plano bom para agora à noite ✨\n\n"
        "• jantar em um lugar gostoso\n"
        "• passeio leve pela região\n"
        "• ou algo mais tranquilo para descansar e começar bem amanhã\n\n"
        "Se quiser, eu monto um roteiro mais no seu estilo 😉"
    )


def get_followup_reply(text, last_topic, guest):
    text_n = normalize_text(text)
    topic = infer_contextual_followup(text, last_topic)
    session = load_session()
    last_rec_name = session.get("last_recommendation_name", "")

    if last_topic == "praia" or topic == "praia":
        if has_any(text_n, [
            "onde fica", "localizacao", "localização",
            "servico de praia", "serviço de praia",
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

        if has_any(text_n, ["mais tarde", "ainda hoje"]):
            return "Se for ainda hoje, eu aproveitaria enquanto o serviço está funcionando e já deixaria o fim do dia mais leve 😉"

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

        if has_any(text_n, ["tradicional", "classico", "clássico", "frutos do mar"]):
            return get_restaurantes_reply("tradicional")

        if has_any(text_n, ["pizza", "pizzaria"]):
            return get_restaurantes_reply("pizza")

        if has_any(text_n, ["japones", "japonês", "sushi"]):
            return get_restaurantes_reply("japones")

        if has_any(text_n, ["doce", "sobremesa", "chocolate"]):
            return get_restaurantes_reply("doce")

        if has_any(text_n, ["vista", "mirante", "lugar bonito"]):
            return get_restaurantes_reply("vista")

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
        return (
            "Claro 😊\n\n"
            "Pra eu te direcionar melhor, me diga o estilo que faria mais sentido agora:\n"
            "• mais **rápido**\n"
            "• mais **especial**\n"
            "• mais **tradicional**\n"
            "• **japonês**\n"
            "• **pizza**\n"
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

    hoje = datetime.now().strftime("%Y-%m-%d")
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
                "/set checkout 11h\n"
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
            f"checkout: {guest.get('checkout','')}\n"
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

        valid_fields = ["nome", "grupo", "checkout", "idioma", "observacoes"]
        if field not in valid_fields:
            return f"Campo inválido. Use um destes: {', '.join(valid_fields)}"

        guest = load_guest()

        if field == "grupo":
            value = normalize_group_value(value)
        if field == "idioma":
            value = normalize_text(value)
            if value not in ["pt", "en"]:
                value = "pt"

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

    inferred_intent_preview = infer_primary_intent(text_raw, last_topic)

    if last_topic == "saude" and has_any(text, [
        "farmacia", "farmácia", "farmacias", "farmácias",
        "upa", "hospital", "todos", "todas"
    ]):
        inferred_intent_preview = "saude"

    if should_use_entity_detail_mode(text_raw, inferred_intent_preview, last_topic):
        preferred_category = contextual_entity_category(last_topic, inferred_intent_preview)
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

    preferred_category = contextual_entity_category(last_topic, inferred_intent_preview)
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
        reply = get_guided_reply("praia") if len(text.split()) <= 2 and has_any(text, ["praia", "praias"]) else get_praia_reply()
        return finalize_and_log(guest, text_raw, "praia", reply, remembered, intent_for_session="praia")

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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
