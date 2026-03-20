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
    memory["messages"] = memory["messages"][-80:]
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
        "pending_bruno_contact": False,
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


def load_incidents():
    return read_json(INCIDENTS_FILE, [])


def save_incidents(data):
    write_json(INCIDENTS_FILE, data)


def append_incident(payload):
    data = load_incidents()
    data.append(payload)
    data = data[-300:]
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
        "response": response[:500]
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
    if has_any(msg, ["mercado", "supermercado", "compras", "mercado dia", "supermercado dia", "dia"]):
        inc("mercado")
    if has_any(msg, ["praia", "guarda-sol", "cadeira de praia"]):
        inc("praia")
    if has_any(msg, ["bar", "bares", "cerveja", "drink", "drinks", "noite"]):
        inc("noite")
    if has_any(msg, ["doce", "sobremesa", "chocolate", "kopenhagen"]):
        inc("doce")
    if has_any(msg, ["surf", "ondas", "surfar"]):
        inc("surf")
    if has_any(msg, ["restaurante", "comer", "jantar", "almoco", "almoço"]):
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
    if has_any(text_n, ["doce", "sobremesa", "chocolate", "kopenhagen"]):
        inc("doce")
    if has_any(text_n, ["praia", "guarda-sol", "servico de praia", "serviço de praia"]):
        inc("praia")
    if has_any(text_n, ["mercado", "supermercado", "compras", "mercado dia", "supermercado dia", "dia"]):
        inc("mercado")
    if has_any(text_n, ["surf", "ondas", "surfar"]):
        inc("surf")
    if has_any(text_n, ["bar", "bares", "drink", "drinks", "cerveja", "noite"]):
        inc("noite")
    if has_any(text_n, ["restaurante", "comer", "jantar", "almoco", "almoço"]):
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
        "🌴 Bem-vindo à praia da Enseada!\n\n"
        "É uma honra ter você hospedado aqui 😊 Espero que tenha feito uma ótima viagem!\n\n"
        "Eu sou o **Gepetto**, seu concierge pessoal durante a estadia.\n\n"
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


def format_distance(dist):
    if not dist:
        return ""
    dist_n = normalize_text(str(dist))
    if has_any(dist_n, ["a pe", "a pé", "metros", "ao lado", "menos de", "andando", "km"]):
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


# =========================
# INTENÇÃO / CONTEXTO
# =========================

def infer_contextual_followup(text_raw, last_topic):
    text_n = normalize_text(text_raw)

    if not last_topic:
        return ""

    if has_any(text_n, [
        "mais perto", "perto", "mais barato", "barato",
        "mais especial", "especial", "mais completo", "completo",
        "mais rapido", "mais rápido", "algo leve", "algo melhor",
        "qual melhor", "qual voce indica", "qual você indica",
        "qual vc indica", "qual voce recomenda", "qual você recomenda",
        "qual vc recomenda", "mais tranquilo", "mais animado",
        "vale a pena", "compensa", "e esse", "e essa"
    ]):
        return last_topic

    very_short_contextual = [
        "qual", "melhor", "barato", "perto", "especial",
        "completo", "tranquilo", "animado", "leve", "esse", "essa",
        "entao", "então", "vc indica",
        "localizacao", "localização", "horario", "horário",
        "servico", "serviço", "envie", "manda", "pode mandar"
    ]
    if text_n in very_short_contextual:
        return last_topic

    return ""


def is_followup_candidate(text_raw, last_topic, inferred_intent):
    if not last_topic:
        return False

    text_n = normalize_text(text_raw)

    strong_new_intents = ["wifi", "regras", "localizacao", "tempo", "identidade", "saude", "incidente", "chaves", "garagem", "checkout"]
    if inferred_intent in strong_new_intents and inferred_intent != last_topic:
        return False

    if infer_contextual_followup(text_raw, last_topic):
        return True

    exact_short = [
        "sim", "isso", "esse", "essa", "pode ser", "manda", "quero esse",
        "quero essa", "qual", "melhor", "barato", "perto", "especial",
        "vc indica", "vcs indicam", "envie", "enviar", "mandar", "mande",
        "pode avisar", "avise", "encaminhe", "encaminhar"
    ]
    if text_n in exact_short:
        return True

    return False


def score_intents(text_raw, last_topic=""):
    text_n = normalize_text(text_raw)
    scores = {}

    def add(intent, points):
        scores[intent] = scores.get(intent, 0) + points

    if has_any(text_n, [
        "gepetto", "gepeto", "qual seu nome", "como voce chama", "como você chama",
        "quem e voce", "quem é você", "quem te fez", "quem te criou", "qm e voce", "qm é você"
    ]):
        add("identidade", 12)

    if has_any(text_n, [
        "onde estamos", "qual o endereco", "qual o endereço", "me passa o endereco",
        "me passa o endereço", "endereco daqui", "endereço daqui", "onde fica aqui"
    ]):
        add("localizacao", 11)
    if has_any(text_n, ["upa", "hospital"]) and has_any(text_n, ["onde fica", "endereco", "endereço"]):
        add("localizacao", 9)

    if has_any(text_n, [
        "desmaiou", "desmaio", "nao consegue respirar", "não consegue respirar",
        "falta de ar", "dor no peito", "muita dor", "dor forte", "sangrando",
        "dor", "doente", "febre", "passando mal", "mal estar", "mal-estar",
        "vomito", "vômito", "enjoo", "to mal", "tô mal"
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

    if has_any(text_n, ["praia", "servico de praia", "serviço de praia", "guarda-sol", "guarda sol", "cadeira de praia"]):
        add("praia", 9)

    if has_any(text_n, ["roteiro", "o que fazer hoje", "plano pro dia", "sugestao de roteiro", "sugestão de roteiro", "o que fazer agora"]):
        add("roteiro", 9)

    if has_any(text_n, [
        "restaurante", "jantar", "almoco", "almoço", "comer", "comida", "fome",
        "sushi", "japones", "japonês", "japonesa", "lanche",
        "hamburguer", "hambúrguer", "chocolate", "sobremesa", "doce",
        "kopenhagen", "mcdonald", "mcdonald's", "burger"
    ]):
        add("restaurantes", 9)

    if has_any(text_n, [
        "mercado", "supermercado", "compras", "pao de acucar", "pão de açúcar",
        "carrefour", "extra", "agua", "água", "mercado dia", "supermercado dia"
    ]):
        add("mercado", 9)

    if phrase_in_text(text_n, "dia") and last_topic == "mercado":
        add("mercado", 7)

    if has_any(text_n, ["padaria", "cafe da manha", "café da manhã", "cafe", "café"]):
        add("padaria", 8)

    if has_any(text_n, ["farmacia", "farmácia", "remedio", "remédio", "dor de cabeca", "dor de cabeça", "droga raia"]):
        add("farmacia", 8)

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

    if has_any(text_n, ["shopping", "la plage"]):
        add("shopping", 8)

    if has_any(text_n, ["feira", "artesanato", "feirinha"]):
        add("feira", 7)

    if has_any(text_n, [
        "tempo", "clima", "previsao", "previsão", "meteorologia",
        "vai chover", "vai fazer sol", "como esta o tempo", "como está o tempo"
    ]):
        add("tempo", 10)

    if has_any(text_n, ["crianca", "criança", "chuva", "passeio", "passeios", "aquario", "aquário", "acqua mundo"]):
        add("passeio", 7)

    if has_any(text_n, ["evento", "eventos", "show", "shows", "festa na cidade"]):
        add("eventos", 7)

    if has_any(text_n, ["surf", "ondas", "mar", "pico de surf", "surfar"]):
        add("surf", 8)

    if has_any(text_n, ["zelador", "paulo", "funcionario", "funcionário", "alguem no predio", "alguém no prédio"]):
        add("zelador", 8)

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
        "garagem", "checkout", "roteiro", "surf", "bares", "shopping", "feira",
        "passeio", "eventos", "zelador", "bruno", "identidade"
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
        "vazamento", "sem energia", "porta nao abre", "nao entra",
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
        "enjoo", "vomito", "vômito", "cansaco", "cansaço"
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
        return (
            f"🌦️ **Clima agora na {referencia}**\n\n"
            f"• Condição: {cond}\n"
            f"• Temperatura: **{temp}°C**\n"
            f"• Sensação térmica: **{apparent}°C**\n"
            f"• Vento: **{wind} km/h**"
            f"{chuva_hint}"
        )
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

    if has_any(text_n, ["quem te fez", "quem te criou", "qm te criou"]):
        return f"O **{anfitriao}** me criou para proporcionar a melhor experiência possível para vocês ✨"

    if has_any(text_n, ["qual seu nome", "como voce chama", "como você chama", "quem e voce", "quem é você", "gepetto", "gepeto", "qm e voce", "qm é você"]):
        return f"Eu sou o **{concierge_nome}**, seu concierge particular 😊"

    return f"Oi 😊 Eu sou o **{concierge_nome}**. Em que posso te ajudar?"


def get_localizacao_reply(text):
    text_n = normalize_text(text)
    k = knowledge()
    apt = k.get("apartamento", {})
    endereco = apt.get("endereco", {})
    proximidades = k.get("proximidades", {})
    upa = proximidades.get("upa_enseada", {})
    hospital = proximidades.get("hospital_santo_amaro", {})

    if has_any(text_n, ["upa"]):
        perfil = upa.get("perfil", "")
        suffix = f"\n\n{perfil}" if perfil else ""
        return (
            "Claro 😊\n\n"
            f"**{upa.get('nome', 'UPA Enseada')}** → cerca de {upa.get('tempo_carro', '4 a 6 minutos de carro')}"
            f"{suffix}\n\n"
            "Se quiser, eu também posso te orientar para hospital ou farmácia."
        )

    if has_any(text_n, ["hospital"]):
        perfil = hospital.get("perfil", "")
        suffix = f"\n\n{perfil}" if perfil else ""
        return (
            "Claro 😊\n\n"
            f"**{hospital.get('nome', 'Hospital Santo Amaro')}** → cerca de {hospital.get('tempo_carro', '10 a 15 minutos de carro')}"
            f"{suffix}\n\n"
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
        "Boa escolha 😄\n\n"
        f"A praia fica a {praia.get('distancia', '280 metros (4 a 5 minutos a pé)')}.\n"
        f"O serviço de praia funciona das {servico.get('horario', '9h às 17h')}.\n"
        f"Ele fica {servico.get('localizacao', 'ao lado do Thai Lounge, em frente ao Casa Grande Hotel')}.\n\n"
        f"{servico.get('como_funciona', 'Os itens ficam montados na areia durante o horário do serviço.')}"
        f"{extra_text}\n\n"
        "Se quiser, eu também posso te explicar rapidinho como aproveitar melhor esse primeiro dia de praia 😉"
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

    if has_any(text_n, ["barato", "economico", "econômico", "simples", "rapido", "rápido", "leve"]):
        item = rapido or {}
        nome = item.get("nome", "McDonald's")
        dist = format_distance(item.get("distancia", "5 minutos"))
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        reply = f"Boa 😄\n\nUma opção prática nesse estilo é o **{nome}**, que fica a cerca de **{dist}**."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        reply += "\n\nSe quiser, também posso te sugerir algo mais tradicional ou mais especial 😉"
        return reply

    if has_any(text_n, ["especial", "romantico", "romântico", "sofisticado", "premium"]):
        item = especial or {}
        nome = item.get("nome", "Thai Lounge")
        alt = (tradicional or {}).get("nome", "Restaurante Alcides")
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        reply = f"Boa 😄\n\nSe quiser algo mais especial, o **{nome}** costuma ser uma ótima pedida ✨"
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        reply += f"\n\nSe preferir algo mais clássico e tradicional, o **{alt}** também é uma excelente escolha."
        return reply

    if has_any(text_n, ["japones", "japonês", "japonesa", "sushi"]):
        item = japones or {}
        nome = item.get("nome", "Sushi Katoshi")
        dist = format_distance(item.get("distancia", "4 minutos"))
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        reply = f"Se a vontade for comida japonesa 🍣\n\nUma boa referência é o **{nome}**, que fica a cerca de **{dist}**."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        return reply

    if has_any(text_n, ["hamburguer", "hambúrguer", "lanche", "mcdonald", "mcdonald's", "burger"]):
        item = rapido or {}
        nome = item.get("nome", "McDonald's")
        dist = format_distance(item.get("distancia", "5 minutos"))
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        return f"Se quiser algo mais rápido 🍔\n\nO **{nome}** fica a cerca de **{dist}**."

    if has_any(text_n, ["doce", "sobremesa", "chocolate", "kopenhagen"]):
        item = doce or {}
        nome = item.get("nome", "Kopenhagen")
        dist = format_distance(item.get("distancia", "4 minutos"))
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="restaurantes", last_recommendation_name=nome)
        reply = f"Se a ideia for um doce ou uma lembrança gostosa 🍫\n\nA **{nome}** fica a cerca de **{dist}**."
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        return reply

    if restaurantes:
        linhas = []
        for r in restaurantes:
            nome = r.get("nome", "")
            tipo = normalize_text(r.get("tipo", ""))
            dist = format_distance(r.get("distancia", ""))
            perfil = r.get("perfil", "")
            if tipo == "tradicional":
                linhas.append(f"• **{nome}** → {perfil or 'clássico, tradicional e muito lembrado no Guarujá'}")
            elif tipo == "especial":
                linhas.append(f"• **{nome}** → {perfil or 'vibe mais especial e experiência mais sofisticada'} ✨")
            elif tipo == "japones":
                linhas.append(f"• **{nome}** → {perfil or 'comida japonesa'}, a cerca de **{dist or '4 minutos de carro'}** 🍣")
            elif tipo == "rapido":
                linhas.append(f"• **{nome}** → {perfil or 'opção prática'}, a cerca de **{dist or '5 minutos de carro'}** 🍔")
            elif tipo == "doce":
                linhas.append(f"• **{nome}** → {perfil or 'chocolateria'}, a cerca de **{dist or '4 minutos de carro'}** 🍫")
            else:
                linhas.append(f"• **{nome}**")

        return (
            "Boa 😄\n\n"
            "Aqui vão algumas boas referências por perto:\n\n"
            + "\n".join(linhas)
            + "\n\nSe quiser, também posso te sugerir uma opção mais **rápida**, mais **especial** ou mais **tradicional** 😉"
        )

    return (
        "Boa 😄\n\n"
        "Aqui vão algumas boas referências por perto:\n\n"
        "• **Restaurante Alcides** → clássico, tradicional e muito lembrado no Guarujá 🦐\n"
        "• **Thai Lounge** → vibe mais especial e experiência mais sofisticada ✨\n"
        "• **Sushi Katoshi** → comida japonesa, a cerca de **4 minutos de carro** 🍣\n"
        "• **McDonald's** → opção prática, a cerca de **5 minutos de carro** 🍔\n"
        "• **Kopenhagen** → chocolateria, a cerca de **4 minutos de carro** 🍫\n\n"
        "Se quiser, também posso te sugerir uma opção mais **rápida**, mais **especial** ou mais **tradicional** 😉"
    )


def get_mercado_reply(text):
    text_n = normalize_text(text)
    mercados = get_markets_data()

    rapido = find_item_by_type(mercados, "rapido")
    completos = [m for m in mercados if normalize_text(m.get("tipo", "")) == "completo"]

    if has_any(text_n, ["rapido", "rápido", "perto", "urgente", "mercado dia", "supermercado dia"]) or (phrase_in_text(text_n, "dia") and not has_any(text_n, ["bom dia"])):
        item = rapido or {}
        nome = item.get("nome", "Mercado Dia")
        dist = format_distance(item.get("distancia", "ao lado"))
        perfil = item.get("perfil", "")
        obs = item.get("observacao", "")
        update_session(last_recommendation_type="mercado", last_recommendation_name=nome)
        reply = f"Pra algo rápido 🛒\n\n• **{nome}** → {dist}"
        if perfil:
            reply += f"\n\n{perfil}."
        if obs:
            reply += f"\n\n{obs}"
        reply += "\n\nPerfeito pra água, bebida ou emergência 👍"
        return reply

    if has_any(text_n, ["completo", "grande", "variedade"]):
        if completos:
            linhas = []
            for m in completos:
                nome = m.get("nome", "")
                dist = format_distance(m.get("distancia", ""))
                perfil = m.get("perfil", "")
                linhas.append(f"• **{nome}** → cerca de **{dist}**" + (f" | {perfil}" if perfil else ""))

            update_session(last_recommendation_type="mercado", last_recommendation_name=completos[0].get("nome", "Pão de Açúcar"))
            return (
                "Se quiser um mercado mais completo:\n\n"
                + "\n".join(linhas)
                + "\n\nEssas opções fazem mais sentido quando você quer mais variedade 🚗"
            )

        update_session(last_recommendation_type="mercado", last_recommendation_name="Pão de Açúcar")
        return (
            "Se quiser um mercado mais completo:\n\n"
            "• **Pão de Açúcar** → cerca de **3 minutos de carro**\n"
            "• **Carrefour** → cerca de **5 minutos de carro**\n"
            "• **Extra** → cerca de **5 minutos de carro**\n\n"
            "Essas opções fazem mais sentido quando você quer mais variedade 🚗"
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

        return (
            "Aqui vão boas opções próximas 😊\n\n"
            + "\n".join(linhas)
            + "\n\nSe quiser, te indico o melhor dependendo do que você precisa 😉"
        )

    return (
        "Aqui vão boas opções próximas 😊\n\n"
        "• **Mercado Dia** → ao lado (ultra prático)\n"
        "• **Pão de Açúcar** → cerca de **3 minutos de carro**\n"
        "• **Extra** → cerca de **5 minutos de carro**\n"
        "• **Carrefour** → cerca de **5 minutos de carro**\n\n"
        "Se quiser, te indico o melhor dependendo do que você precisa 😉"
    )


def get_padaria_reply():
    padaria = knowledge().get("padaria", {})
    perfil = padaria.get("perfil", "")
    obs = padaria.get("observacao", "")
    reply = (
        "Se você quiser padaria ou café da manhã 😊\n\n"
        f"Uma referência prática é a **{padaria.get('nome', 'Padaria Pitangueiras')}**, a cerca de **{padaria.get('distancia', '300m do apartamento')}**."
    )
    if perfil:
        reply += f"\n\n{perfil}."
    if obs:
        reply += f"\n\n{obs}"
    return reply


def get_farmacia_reply():
    farmacia = knowledge().get("farmacia", {})
    nome = farmacia.get("nome", "Droga Raia")
    distancia = farmacia.get("distancia", "1,2km do apartamento")
    horario = farmacia.get("horario", "24h")
    perfil = farmacia.get("perfil", "")
    obs = farmacia.get("observacao", "")

    reply = (
        "Se você estiver precisando de farmácia 😊\n\n"
        f"Uma referência prática é a **{nome}**, que fica a cerca de **{distancia}** e funciona **{horario}**."
    )
    if perfil:
        reply += f"\n\n{perfil}."
    if obs:
        reply += f"\n\n{obs}"
    reply += "\n\nSe for algo urgente ou mais delicado, também posso te orientar para atendimento na região 👍"
    return reply


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
    cond = knowledge().get("condominio", {})
    nome = cond.get("zelador", "Paulo")
    msg = cond.get("mensagem", "Caso vocês precisem de algum auxílio, podem contar com ele.")
    obs = cond.get("observacao", "")
    return f"Se precisar de auxílio no prédio, você pode contar com o zelador, o {nome} 😊\n\n{msg}" + (f"\n\n{obs}" if obs else "")


def get_checkout_reply(guest):
    checkout = guest.get("checkout") or knowledge().get("apartamento", {}).get("checkout", "11h")
    return f"O check-out está configurado para: **{checkout}** 😊"


def get_checkout_aviso_reply(guest):
    checkout = guest.get("checkout") or knowledge().get("apartamento", {}).get("checkout", "11h")
    return (
        f"Antes do check-out, peço por gentileza que verifiquem estes pontos 😊\n\n"
        f"• Verifique se janelas e porta de entrada ficarão travadas\n"
        f"• Favor retirar o lixo\n"
        f"• Apague as luzes e desligue os ventiladores\n"
        f"• Devolva as chaves na portaria do prédio\n"
        f"• Não deixem louça suja\n\n"
        f"O check-out está configurado para **{checkout}**."
    )


def get_bruno_reply():
    anfitriao = knowledge().get("extras", {}).get("anfitriao", "Bruno")
    set_bruno_pending(True)
    return (
        f"Claro 😊 Posso avisar o {anfitriao} agora.\n\n"
        "Tem algum assunto que você queira que eu adiante na notificação?\n\n"
        "Se preferir, também pode só me responder:\n"
        "• **envie**\n"
        "• **enviar**\n"
        "• **manda**\n"
        "• **mandar**\n"
        "• **mande**\n"
        "• **encaminhe**"
    )


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
    if sev == "alta":
        return (
            "Isso parece importante ⚠️\n\n"
            "Se for uma situação urgente, priorize atendimento imediato.\n\n"
            "Posso te orientar rapidamente para **UPA Enseada** ou **Hospital Santo Amaro**."
        )
    return (
        "Entendi 😕\n\n"
        "Se você não estiver se sentindo bem, posso te orientar para farmácia ou atendimento na região.\n\n"
        "Se quiser, já te digo qual caminho faz mais sentido."
    )


def get_problem_reply(text):
    sev = classify_incident(text)

    if sev == "alta":
        return (
            "Isso parece importante ⚠️\n\n"
            "Se for seguro, se afaste do local ou desligue o equipamento, quando fizer sentido."
        )

    if sev == "media":
        return (
            "Entendi 👍\n\n"
            "Vou te ajudar com isso.\n\n"
            "Me conta só se aconteceu do nada ou se já estava assim antes."
        )

    return (
        "Entendi 👍\n\n"
        "Me conta exatamente o que aconteceu.\n\n"
        "Já estava assim antes ou aconteceu agora?\n\n"
        "Enquanto isso, eu já deixo isso encaminhado por aqui."
    )


def get_acqua_mundo_reply():
    acqua = knowledge().get("proximidades", {}).get("acqua_mundo", {})
    ideal = acqua.get("ideal_para", [])
    ideal_text = ""
    if ideal:
        ideal_text = "\n\nIdeal para: " + ", ".join(ideal)

    return (
        f"Uma boa opção por aqui é o **{acqua.get('nome', 'Acqua Mundo')}** 😊\n\n"
        f"{acqua.get('observacao', 'Costuma funcionar muito bem para famílias e também em dias de chuva.')}"
        f"{ideal_text}\n\n"
        "Se quiser, posso te sugerir esse tipo de passeio quando o tempo não estiver tão bom."
    )


def get_eventos_reply():
    return (
        "O Guarujá costuma ter eventos, festas e programações pontuais dependendo da época 😊\n\n"
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
        "Se você curte surf, posso te ajudar com uma orientação geral sobre os picos mais lembrados por aqui 🌊\n\n"
        "Entre os mais conhecidos estão:\n"
        "• **Tombo** → mais tradicional e forte\n"
        "• **Enseada** → mais acessível em alguns dias\n"
        "• **Pitangueiras** → opção urbana\n"
        "• **Pernambuco / Mar Casado** → pode render bem dependendo do mar\n\n"
        "Se quiser, posso sugerir qual parece fazer mais sentido para o seu perfil."
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
        linhas = [
            "• **Dona Eva Bar & Chopperia**",
            "• **Bali Hai**",
            "• **Quiosques da Orla da Enseada**",
            "• **Boteco da Orla**"
        ]

    return (
        "Se a ideia for sair à noite 🍻\n\n"
        "Encontrei alguns estabelecimentos próximos ao apartamento:\n\n"
        + "\n".join(linhas)
        + "\n\nSão opções mais descontraídas para curtir a noite 😊\n\n"
        "Se quiser algo mais tranquilo ou mais animado, posso te direcionar melhor 😉"
    )


def get_shopping_reply():
    return (
        "Se quiser shopping, uma referência útil é o **Shopping La Plage** 🛍️\n\n"
        "Ele fica em **Pitangueiras** e costuma ser uma boa opção para passeio, lojas e alimentação."
    )


def get_feira_reply():
    return (
        "Se quiser algo mais local, também vale procurar a **Feira da Enseada** 😊\n\n"
        "Ela costuma ser uma boa opção para artesanato, lembranças e um passeio mais leve no fim do dia."
    )


def get_tempo_reply():
    return get_weather_reply()


def get_roteiro_reply(guest):
    parte_do_dia = current_time_label()
    grupo = guest_group_label(guest)
    last_pref = top_guest_preference(guest)

    if parte_do_dia == "manhã":
        if grupo == "família":
            return (
                "Se eu fosse montar um roteiro leve para hoje, faria assim 😊\n\n"
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
            "• algo tradicional como o **Restaurante Alcides**\n\n"
            "🌤️ **Tarde**\n"
            "• descanso, caminhada leve ou mercado rápido se precisar de algo\n\n"
            "🌙 **Noite**\n"
            "• jantar mais especial, como o **Thai Lounge**"
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

    if topic == "praia":
        if has_any(text_n, [
            "onde fica", "localizacao", "localização",
            "servico de praia", "serviço de praia"
        ]):
            return get_servico_praia_localizacao_reply()

        if has_any(text_n, ["horario", "horário", "que horas", "funciona que horas"]):
            servico = knowledge().get("praia", {}).get("servico_praia", {})
            return f"Claro 😊\n\nO serviço de praia funciona das **{servico.get('horario', '9h às 17h')}**."

        if has_any(text_n, ["como funciona", "funciona", "servico", "serviço"]):
            servico = knowledge().get("praia", {}).get("servico_praia", {})
            return f"Claro 😊\n\n{servico.get('como_funciona', 'Os itens ficam montados na areia durante o horário do serviço.')}"

        if has_any(text_n, ["mais tarde", "ainda hoje"]):
            return "Se for ainda hoje, eu aproveitaria enquanto o serviço está funcionando e já deixaria o fim do dia mais leve 😉"

    if topic == "restaurantes":
        restaurantes = get_restaurants_data()
        if has_any(text_n, ["mais perto", "perto"]):
            item = best_closest_item(restaurantes)
            if item:
                reply = f"Se a prioridade for proximidade, eu iria no **{item.get('nome', '')}**, que fica a cerca de **{format_distance(item.get('distancia', ''))}**."
                if item.get("perfil"):
                    reply += f"\n\n{item.get('perfil')}."
                reply += "\n\nSe quiser, eu também posso te dizer qual eu escolheria pelo custo-benefício."
                return reply

        if has_any(text_n, ["mais barato", "barato", "economico", "econômico", "leve"]):
            return get_restaurantes_reply("barato")

        if has_any(text_n, ["mais especial", "especial", "romantico", "romântico", "sofisticado"]):
            return get_restaurantes_reply("especial")

        if has_any(text_n, ["qual melhor", "qual voce indica", "qual você indica", "qual vc indica", "qual voce recomenda", "qual você recomenda", "qual vc recomenda", "compensa", "vale a pena"]):
            return (
                "Se eu tivesse que te direcionar sem erro 😊\n\n"
                "• **Thai Lounge** → se você quiser algo mais especial\n"
                "• **Restaurante Alcides** → se quiser algo clássico e tradicional\n"
                "• **McDonald's** → se a ideia for praticidade\n"
                "• **Sushi Katoshi** → se estiver com vontade de japonês 🍣"
            )

        if text_n in ["esse", "essa", "pode ser", "quero esse", "quero essa"] and last_rec_name:
            return f"Boa escolha 😄\n\nSe eu fosse por esse caminho, iria de **{last_rec_name}**."

    if topic == "mercado":
        mercados = get_markets_data()

        if has_any(text_n, ["mais completo", "completo", "grande", "variedade"]):
            return get_mercado_reply("completo")

        if has_any(text_n, ["mais perto", "perto", "rapido", "rápido"]):
            item = best_closest_item(mercados)
            if item:
                reply = f"Se a prioridade for praticidade, eu iria no **{item.get('nome', '')}**, que fica **{format_distance(item.get('distancia', ''))}**."
                if item.get("perfil"):
                    reply += f"\n\n{item.get('perfil')}."
                reply += "\n\nÉ a melhor opção para resolver algo rápido."
                return reply
            return get_mercado_reply("rapido")

        if has_any(text_n, ["qual melhor", "qual voce recomenda", "qual você recomenda", "qual vc recomenda", "compensa"]):
            return (
                "Depende do que você precisa 😊\n\n"
                "• **Mercado Dia** → se quiser algo rápido\n"
                "• **Pão de Açúcar** → se quiser algo mais organizado e confortável\n"
                "• **Extra / Carrefour** → se a ideia for compra mais completa"
            )

    if topic == "saude":
        return get_health_reply(text)

    if topic == "incidente":
        return get_problem_reply(text)

    if topic == "bares":
        if has_any(text_n, ["mais tranquilo", "tranquilo"]):
            bares = get_knowledge_list("bares")
            calm = next((b for b in bares if isinstance(b, dict) and has_any(b.get("perfil", ""), ["leve", "casual", "descontraido", "descontraído"])), None)
            if calm:
                return f"Se a ideia for algo mais tranquilo, eu começaria por **{calm.get('nome', '')}** → {calm.get('perfil', '')} 😊"
            return "Se a ideia for algo mais tranquilo, eu priorizaria uma saída leve, sem muita agitação, só pra curtir a noite com calma 😊"

        if has_any(text_n, ["mais animado", "animado"]):
            return "Se vocês quiserem algo mais animado, eu buscaria uma opção mais voltada para noite e movimento na orla 🍻"

    if topic == "tempo":
        if has_any(text_n, ["e pra praia", "compensa", "vale a pena", "e hoje"]):
            return f"{get_weather_reply()}\n\nSe quiser, eu também posso te sugerir se hoje faz mais sentido praia, passeio ou algo coberto 😉"

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
            "Claro 😄\n\n"
            "Pra eu te direcionar melhor, você quer algo:\n"
            "• mais **rápido**\n"
            "• mais **especial**\n"
            "• ou mais **em conta**?"
        )

    if intent == "mercado":
        return (
            "Posso te ajudar nisso 👍\n\n"
            "Você quer algo:\n"
            "• **rápido**\n"
            "• ou um mercado mais **completo**?"
        )

    if intent == "bares":
        return (
            "Boa 😄\n\n"
            "Você quer algo mais:\n"
            "• **animado**\n"
            "• ou mais **tranquilo**?"
        )

    if intent == "praia":
        return (
            "Boa 😊\n\n"
            "Você quer saber sobre:\n"
            "• **localização**\n"
            "• **horário**\n"
            "• ou como funciona o **serviço de praia**?"
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
    base = [
        "Me diz melhor o que você precisa que eu te ajudo 😊",
        "Posso te ajudar com **praia**, **comida**, **mercado**, **clima** ou qualquer dúvida do apartamento 👍",
        "Se puder me dar um pouco mais de contexto, eu te ajudo melhor 😉"
    ]
    reply = random.choice(base)
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

    inferred_intent = infer_primary_intent(text_raw, last_topic)

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

    intent = inferred_intent

    if intent == "identidade":
        return finalize_and_log(guest, text_raw, "identidade", get_identidade_reply(text_raw), remembered, intent_for_session="identidade")

    if intent == "localizacao":
        return finalize_and_log(guest, text_raw, "localizacao", get_localizacao_reply(text_raw), remembered, intent_for_session="localizacao")

    if intent == "saude":
        base_reply = get_health_reply(text_raw)
        ok, _ = maybe_notify("saude", text_raw, guest, classify_health(text_raw))
        if ok:
            reply = base_reply + "\n\nJá deixei isso sinalizado por aqui e enviei uma solicitação de acompanhamento ao Bruno. Ele entrará em contato com você o quanto antes 😊"
        else:
            reply = base_reply + "\n\nJá deixei isso sinalizado por aqui, mas não consegui enviar a solicitação de acompanhamento ao Bruno neste momento."
        return finalize_and_log(guest, text_raw, "saude", reply, remembered, intent_for_session="saude")

    if intent == "incidente":
        base_reply = get_problem_reply(text_raw)
        ok, _ = maybe_notify("incidente", text_raw, guest, classify_incident(text_raw))
        if ok:
            reply = base_reply + "\n\nJá deixei isso sinalizado por aqui e enviei uma solicitação de acompanhamento ao Bruno. Ele entrará em contato com você o quanto antes 😊"
        else:
            reply = base_reply + "\n\nJá deixei isso sinalizado por aqui, mas não consegui enviar a solicitação de acompanhamento ao Bruno neste momento."
        return finalize_and_log(guest, text_raw, "incidente", reply, remembered, intent_for_session="incidente")

    if intent == "wifi":
        return finalize_and_log(guest, text_raw, "wifi", get_wifi_reply(), remembered, intent_for_session="wifi")

    if intent == "regras":
        return finalize_and_log(guest, text_raw, "regras", get_regras_reply(text_raw), remembered, intent_for_session="regras")

    if intent == "praia_local":
        return finalize_and_log(guest, text_raw, "praia", get_servico_praia_localizacao_reply(), remembered, intent_for_session="praia_local")

    if intent == "praia":
        reply = get_guided_reply("praia") if len(text.split()) <= 2 and has_any(text, ["praia"]) else get_praia_reply()
        return finalize_and_log(guest, text_raw, "praia", reply, remembered, intent_for_session="praia")

    if intent == "roteiro":
        return finalize_and_log(guest, text_raw, "roteiro", get_roteiro_reply(guest), remembered, intent_for_session="roteiro")

    if intent == "restaurantes":
        if len(text.split()) <= 3 and has_any(text, ["comer", "jantar", "restaurante", "fome"]):
            reply = get_guided_reply("restaurantes")
        else:
            reply = get_restaurantes_reply(text_raw)
        return finalize_and_log(guest, text_raw, "restaurantes", reply, remembered, intent_for_session="restaurantes")

    if intent == "mercado":
        if len(text.split()) <= 3 and has_any(text, ["mercado", "compras", "supermercado", "mercado dia", "supermercado dia"]):
            reply = get_guided_reply("mercado")
        else:
            reply = get_mercado_reply(text_raw)
        return finalize_and_log(guest, text_raw, "mercado", reply, remembered, intent_for_session="mercado")

    if intent == "padaria":
        return finalize_and_log(guest, text_raw, "padaria", get_padaria_reply(), remembered, intent_for_session="padaria")

    if intent == "farmacia":
        return finalize_and_log(guest, text_raw, "farmacia", get_farmacia_reply(), remembered, intent_for_session="farmacia")

    if intent == "garagem":
        return finalize_and_log(guest, text_raw, "garagem", get_garagem_reply(), remembered, intent_for_session="garagem")

    if intent == "chaves":
        return finalize_and_log(guest, text_raw, "chaves", get_chaves_reply(), remembered, intent_for_session="chaves")

    if intent == "checkout":
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
        return finalize_and_log(guest, text_raw, "bruno", get_bruno_reply(), remembered, intent_for_session="bruno")

    if intent == "bares":
        if len(text.split()) <= 3 and has_any(text, ["bar", "bares", "pub", "noite", "drink", "drinks"]):
            reply = get_guided_reply("bares")
        else:
            reply = get_bares_reply()
        return finalize_and_log(guest, text_raw, "bares", reply, remembered, intent_for_session="bares")

    if intent == "shopping":
        return finalize_and_log(guest, text_raw, "shopping", get_shopping_reply(), remembered, intent_for_session="shopping")

    if intent == "feira":
        return finalize_and_log(guest, text_raw, "feira", get_feira_reply(), remembered, intent_for_session="feira")

    if intent == "tempo":
        return finalize_and_log(guest, text_raw, "tempo", get_tempo_reply(), remembered, intent_for_session="tempo")

    if intent == "passeio":
        return finalize_and_log(guest, text_raw, "passeio", get_acqua_mundo_reply(), remembered, intent_for_session="passeio")

    if intent == "eventos":
        return finalize_and_log(guest, text_raw, "eventos", get_eventos_reply(), remembered, intent_for_session="eventos")

    if intent == "surf":
        return finalize_and_log(guest, text_raw, "surf", get_surf_reply(), remembered, intent_for_session="surf")

    if intent == "zelador":
        return finalize_and_log(guest, text_raw, "zelador", get_zelador_reply(), remembered, intent_for_session="zelador")

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


# =========================
# START
# =========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
