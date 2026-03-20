from flask import Flask, request, jsonify, send_from_directory
import os
import json
import random
import unicodedata
import requests
from pathlib import Path
from datetime import datetime

app = Flask(__name__, static_folder="static", static_url_path="/static")

BASE_DIR = Path(__file__).parent
KNOWLEDGE_FILE = BASE_DIR / "knowledge_base.json"
GUEST_FILE = BASE_DIR / "current_guest.json"
MEMORY_FILE = BASE_DIR / "conversation_memory.json"
INCIDENTS_FILE = BASE_DIR / "incidents.json"
SESSION_FILE = BASE_DIR / "session_state.json"

ADMIN_PIN = "2710"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()


def load_knowledge():
    if KNOWLEDGE_FILE.exists():
        with open(KNOWLEDGE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def default_guest():
    return {
        "nome": "",
        "grupo": "",
        "checkout": "",
        "idioma": "pt",
        "observacoes": ""
    }


def load_guest():
    if GUEST_FILE.exists():
        with open(GUEST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    data = default_guest()
    save_guest(data)
    return data


def save_guest(data):
    with open(GUEST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def default_memory():
    return {"messages": []}


def load_memory():
    if MEMORY_FILE.exists():
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    data = default_memory()
    save_memory(data)
    return data


def save_memory(data):
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def append_memory(role, text, topic="", intent="", filters=None, emotion=""):
    memory = load_memory()
    memory["messages"].append({
        "role": role,
        "text": text,
        "topic": topic,
        "intent": intent,
        "filters": filters or {},
        "emotion": emotion,
        "timestamp": datetime.now().isoformat(timespec="seconds")
    })
    memory["messages"] = memory["messages"][-30:]
    save_memory(memory)


def reset_memory():
    save_memory(default_memory())


def load_incidents():
    if INCIDENTS_FILE.exists():
        with open(INCIDENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_incidents(data):
    with open(INCIDENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def append_incident(payload):
    data = load_incidents()
    data.append(payload)
    save_incidents(data)


def default_session():
    return {
        "current_intent": "",
        "last_recommendation": "",
        "conversation_stage": "",
        "urgency": "",
        "last_suggestion_type": "",
        "last_topic": "",
        "updated_at": ""
    }


def load_session():
    if SESSION_FILE.exists():
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    data = default_session()
    save_session(data)
    return data


def save_session(data):
    with open(SESSION_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def reset_session():
    save_session(default_session())


KNOWLEDGE = load_knowledge()
ADMIN_UNLOCKED = False


def normalize_text(text: str) -> str:
    text = (text or "").lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text


def has_any(text: str, terms) -> bool:
    base = normalize_text(text)
    return any(normalize_text(term) in base for term in terms)


def get_recent_messages(limit=8):
    return load_memory().get("messages", [])[-limit:]


def get_last_topic():
    for item in reversed(get_recent_messages(15)):
        topic = item.get("topic", "")
        if topic and topic not in ["admin", "fallback", "saudacao"]:
            return topic
    return ""


def get_last_user_text():
    for item in reversed(get_recent_messages(15)):
        if item.get("role") == "user":
            return normalize_text(item.get("text", ""))
    return ""


def current_time_label():
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "manhÃ£"
    if 12 <= hour < 18:
        return "tarde"
    return "noite"


def guest_group_label(guest):
    grupo = (guest.get("grupo") or "").strip().lower()
    if grupo == "familia":
        return "famÃ­lia"
    if grupo == "amigos":
        return "amigos"
    if grupo == "casal":
        return "casal"
    return ""


def saudacao_personalizada(guest):
    nome = (guest.get("nome") or "").strip()
    grupo = (guest.get("grupo") or "").strip().lower()

    if not nome:
        return "OlÃ¡ ð"

    if grupo == "familia":
        return f"OlÃ¡ {nome} e famÃ­lia ð"
    if grupo == "amigos":
        return f"OlÃ¡ {nome} e amigos ð"
    if grupo == "casal":
        return f"OlÃ¡ {nome} ð"

    return f"OlÃ¡ {nome} ð"


def mensagem_boas_vindas():
    guest = load_guest()
    inicio = saudacao_personalizada(guest)

    return (
        f"{inicio}\n\n"
        "ð´ Bem-vindo Ã  praia da Enseada!\n\n"
        "Ã uma honra ter vocÃª hospedado aqui ð Espero que tenha feito uma Ã³tima viagem!\n\n"
        "Eu sou o **Gepetto**, seu concierge pessoal durante a estadia.\n\n"
        "Posso te ajudar com:\n"
        "â¢ **Guia do apartamento e do condomÃ­nio**\n"
        "â¢ **RecomendaÃ§Ãµes de restaurantes**\n"
        "â¢ **Mercados e conveniÃªncias**\n"
        "â¢ **Praia, passeios e dicas locais**\n\n"
        "Fique Ã  vontade para me chamar a qualquer momento ð"
    )


def proactive_prompt(guest):
    grupo = guest_group_label(guest)

    if grupo == "famÃ­lia":
        return (
            "Se quiser, posso te indicar agora:\n"
            "â¢ uma boa opÃ§Ã£o de **restaurante** ð½ï¸\n"
            "â¢ um **mercado prÃ³ximo** ð\n"
            "â¢ ou te explicar rapidinho como funciona a **praia** ðï¸"
        )

    if grupo == "amigos":
        return (
            "Se quiser, posso te indicar agora:\n"
            "â¢ um lugar bom pra **comer ou jantar** ð½ï¸\n"
            "â¢ uma opÃ§Ã£o rÃ¡pida de **mercado** ð\n"
            "â¢ ou jÃ¡ te passar como funciona a **praia** ðï¸"
        )

    if grupo == "casal":
        return (
            "Se quiser, posso te indicar agora:\n"
            "â¢ um restaurante mais **especial** â¨\n"
            "â¢ uma opÃ§Ã£o rÃ¡pida de **mercado** ð\n"
            "â¢ ou te orientar sobre a **praia** ðï¸"
        )

    opcoes = [
        "Se quiser, posso te indicar agora um **restaurante**, um **mercado** ou te explicar como funciona a **praia** ð",
        "Posso te ajudar agora com **praia**, **mercado**, **restaurantes** ou qualquer dÃºvida do apartamento ð",
        "Se preferir, jÃ¡ posso comeÃ§ar te orientando sobre **praia**, **comida** ou **compras rÃ¡pidas** ð"
    ]
    return random.choice(opcoes)


def should_send_telegram():
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram_message(message):
    if not should_send_telegram():
        return False, "Telegram nÃ£o configurado"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    try:
        resp = requests.post(url, json=payload, timeout=8)
        return resp.ok, resp.text
    except Exception as e:
        return False, str(e)


def classify_incident(text):
    text_n = normalize_text(text)

    high = [
        "vazamento", "sem energia", "porta nao abre", "nao entra",
        "curto", "fogo", "incendio", "incÃªndio", "fumaca", "fumaÃ§a",
        "gas", "gÃ¡s", "explosao", "explosÃ£o", "cheiro de queimado",
        "queimando", "pegando fogo"
    ]

    medium = [
        "chuveiro", "ar nao funciona", "ar nÃ£o funciona", "tv nao liga", "tv nÃ£o liga",
        "wifi nao funciona", "wifi nÃ£o funciona", "internet nao funciona", "internet nÃ£o funciona",
        "parou de funcionar", "quebrou", "defeito", "nao funciona", "nÃ£o funciona",
        "queimou", "queimado", "esquentando demais", "muito quente", "travou", "bugou"
    ]

    if has_any(text_n, high):
        return "alta"
    if has_any(text_n, medium):
        return "media"
    return "baixa"


def classify_health(text):
    text_n = normalize_text(text)

    high = [
        "desmaiou", "desmaio", "nao consegue respirar", "nÃ£o consegue respirar",
        "falta de ar", "muita dor", "dor forte", "sangrando",
        "muito mal", "urgente", "emergencia", "emergÃªncia", "dor no peito"
    ]

    medium = [
        "dor", "doente", "febre", "passando mal", "mal estar", "mal-estar",
        "enjoo", "vomito", "vÃ´mito", "cansaco", "cansaÃ§o"
    ]

    if has_any(text_n, high):
        return "alta"
    if has_any(text_n, medium):
        return "media"
    return "baixa"


def detect_emotion(text):
    text_n = normalize_text(text)

    if has_any(text_n, ["urgente", "socorro", "me ajuda", "ajuda", "desespero", "medo"]):
        return "urgente"

    if has_any(text_n, ["que droga", "ruim", "chato", "nao gostei", "nÃ£o gostei", "pessimo", "pÃ©ssimo", "horrivel", "horrÃ­vel"]):
        return "frustrado"

    if has_any(text_n, ["dor", "doente", "febre", "passando mal", "mal estar", "mal-estar"]):
        return "saude"

    return ""


def maybe_notify_incident(raw_message, guest, kind="incidente", severity=None):
    sev = severity or classify_incident(raw_message)
    if sev not in ["alta", "media"]:
        return

    payload = {
        "tipo": kind,
        "gravidade": sev,
        "mensagem": raw_message,
        "hospede": guest.get("nome", ""),
        "grupo": guest.get("grupo", ""),
        "checkout": guest.get("checkout", ""),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "status": "aberto"
    }
    append_incident(payload)

    label_guest = guest.get("nome") or "HÃ³spede sem nome definido"
    emoji = "ð¨" if sev == "alta" else "â ï¸"
    tg_msg = (
        f"{emoji} {kind.upper()} NO APTO 14B\n\n"
        f"Gravidade: {sev.upper()}\n"
        f"HÃ³spede: {label_guest}\n"
        f"Mensagem: {raw_message}\n"
        f"HorÃ¡rio: {payload['timestamp']}"
    )
    send_telegram_message(tg_msg)


def infer_intent(text, last_topic=""):
    text_n = normalize_text(text)

    if len(text_n.split()) <= 4 and last_topic and has_any(
        text_n,
        [
            "mais barato", "mais perto", "mais especial", "mais completo",
            "mais rapido", "mais rÃ¡pido", "melhor", "qual melhor", "qual",
            "barato", "especial", "rapido", "rÃ¡pido", "agora", "hoje",
            "leve", "romantico", "romÃ¢ntico", "animado", "tranquilo"
        ]
    ):
        return last_topic

    if has_any(text_n, ["restaurante", "jantar", "almoco", "almoÃ§o", "comer", "comida", "lanche", "hamburguer", "hambÃºrguer", "sushi", "sobremesa", "doce", "chocolate", "fome"]):
        return "restaurantes"

    if has_any(text_n, ["mercado", "supermercado", "compras", "pao de acucar", "pÃ£o de aÃ§Ãºcar", "carrefour", "extra", "agua", "Ã¡gua", "bebida"]):
        return "mercado"

    if has_any(text_n, ["praia", "servico de praia", "serviÃ§o de praia", "guarda-sol", "guarda sol", "cadeira de praia"]):
        return "praia"

    if has_any(text_n, ["bar", "bares", "pub", "cerveja", "beber", "drink", "drinks", "noite"]):
        return "bares"

    if has_any(text_n, ["shopping", "la plage"]):
        return "shopping"

    if has_any(text_n, ["padaria", "cafe", "cafÃ©", "cafe da manha", "cafÃ© da manhÃ£"]):
        return "padaria"

    if has_any(text_n, ["farmacia", "farmÃ¡cia", "remedio", "remÃ©dio"]):
        return "farmacia"

    if has_any(text_n, ["hospital", "upa", "doente", "dor", "febre", "passando mal", "mal estar", "mal-estar", "medico", "mÃ©dico", "saude", "saÃºde"]):
        return "saude"

    if has_any(text_n, ["fogo", "incendio", "incÃªndio", "fumaca", "fumaÃ§a", "gas", "gÃ¡s", "curto", "vazamento", "queimado", "queimou", "cheiro de queimado"]):
        return "incidente"

    if has_any(text_n, ["roteiro", "o que fazer hoje", "plano pro dia", "sugestao de roteiro", "sugestÃ£o de roteiro"]):
        return "roteiro"

    if has_any(text_n, ["passeio", "passeios", "chuva", "crianca", "crianÃ§a", "aquario", "aquÃ¡rio", "acqua mundo"]):
        return "passeio"

    if has_any(text_n, ["evento", "eventos", "show", "shows", "festa"]):
        return "eventos"

    if has_any(text_n, ["surf", "ondas", "mar", "pico de surf"]):
        return "surf"

    if has_any(text_n, ["gepetto", "gepeto", "como voce chama", "qual seu nome", "quem e voce", "quem te criou", "quem te fez"]):
        return "identidade"

    if has_any(text_n, ["onde estamos", "qual o endereco", "qual o endereÃ§o", "me passa o endereco", "me passa o endereÃ§o"]):
        return "localizacao"

    return ""


def resolve_intent_with_session(text, inferred_intent):
    session = load_session()
    text_n = normalize_text(text)

    if inferred_intent:
        return inferred_intent

    if session.get("current_intent") and (
        len(text_n.split()) <= 5
        or has_any(text_n, [
            "mais barato", "mais perto", "mais especial", "mais completo",
            "mais rapido", "mais rÃ¡pido", "melhor", "qual", "animado",
            "tranquilo", "agora", "hoje", "leve"
        ])
    ):
        return session["current_intent"]

    return inferred_intent


def extract_filters(text):
    text_n = normalize_text(text)
    filters = {}

    if has_any(text_n, ["barato", "economico", "econÃ´mico", "em conta"]):
        filters["preco"] = "barato"

    if has_any(text_n, ["perto", "mais perto", "proximo", "prÃ³ximo"]):
        filters["distancia"] = "perto"

    if has_any(text_n, ["especial", "romantico", "romÃ¢ntico", "sofisticado", "premium"]):
        filters["estilo"] = "especial"

    if has_any(text_n, ["rapido", "rÃ¡pido", "pratico", "prÃ¡tico"]):
        filters["ritmo"] = "rapido"

    if has_any(text_n, ["leve"]):
        filters["tipo"] = "leve"

    if has_any(text_n, ["hoje", "agora", "pra hoje", "para hoje"]):
        filters["momento"] = "agora"

    if has_any(text_n, ["mais tarde", "a noite", "Ã  noite", "noite"]):
        filters["momento"] = "noite"

    if has_any(text_n, ["animado"]):
        filters["clima"] = "animado"

    if has_any(text_n, ["tranquilo"]):
        filters["clima"] = "tranquilo"

    return filters


def update_session(intent="", emotion="", topic="", suggestion_type=""):
    session = load_session()

    if intent:
        session["current_intent"] = intent
    if topic:
        session["last_topic"] = topic
    if emotion:
        session["urgency"] = emotion
    if suggestion_type:
        session["last_suggestion_type"] = suggestion_type

    session["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_session(session)


def add_proactive_layer(intent):
    if intent == "restaurantes":
        return "\n\nSe quiser, eu tambÃ©m posso te direcionar para algo mais rÃ¡pido, mais especial ou atÃ© mais em conta ð"

    if intent == "mercado":
        return "\n\nSe quiser, te digo qual compensa mais dependendo do tipo de compra ð"

    if intent == "praia":
        return "\n\nSe quiser, tambÃ©m posso te explicar rapidinho o melhor jeito de aproveitar a praia hoje ðï¸"

    if intent == "bares":
        return "\n\nSe quiser, eu tambÃ©m posso separar em algo mais animado ou mais tranquilo ð»"

    return ""


def adapt_response_by_urgency(response, emotion):
    if emotion == "urgente":
        return response.replace("ð", "").replace("ð", "").replace("ð", "")
    return response


def get_followup_reply(text, last_topic, guest):
    text_n = normalize_text(text)
    topic = infer_contextual_followup(text, last_topic)

    if topic == "restaurantes":
        if has_any(text_n, ["mais perto", "perto"]):
            return (
                "Se a prioridade for praticidade, eu focaria no que te faÃ§a sair e voltar com mais conforto.\n\n"
                "Se quiser, eu sigo numa linha mais **prÃ¡tica**, mais **especial** ou mais **tradicional** ð"
            )
        if has_any(text_n, ["mais barato", "barato", "economico", "econÃ´mico", "algo leve"]):
            return get_restaurantes_reply("barato")
        if has_any(text_n, ["mais especial", "especial", "romantico", "romÃ¢ntico", "sofisticado"]):
            return get_restaurantes_reply("especial")
        if has_any(text_n, ["qual melhor", "qual voce recomenda", "qual vc recomenda", "qual voce indica", "qual vc indica"]):
            return (
                "Se eu tivesse que te direcionar sem erro ð\n\n"
                "â¢ **Thai Lounge** â se vocÃª quiser algo mais especial\n"
                "â¢ **Restaurante Alcides** â se quiser algo clÃ¡ssico e tradicional\n"
                "â¢ **McDonald's** â se a ideia for praticidade\n"
                "â¢ **Sushi Katoshi** â se estiver com vontade de japonÃªs ð£"
            )

    if topic == "praia":
        if has_any(text_n, ["mais tarde", "agora", "hoje"]):
            return (
                "Se for ainda hoje, eu aproveitaria enquanto o serviÃ§o estÃ¡ funcionando e jÃ¡ deixaria o fim do dia mais leve ð"
            )
        if has_any(text_n, ["como funciona", "funciona como"]):
            return get_praia_reply()

    if topic == "mercado":
        if has_any(text_n, ["mais completo", "completo", "grande", "variedade"]):
            return get_mercado_reply("completo")
        if has_any(text_n, ["mais perto", "perto", "rapido", "rÃ¡pido"]):
            return get_mercado_reply("rapido")
        if has_any(text_n, ["qual melhor", "qual voce recomenda", "qual vc recomenda"]):
            return (
                "Depende do que vocÃª precisa ð\n\n"
                "â¢ **Dia** â se quiser algo rÃ¡pido\n"
                "â¢ **PÃ£o de AÃ§Ãºcar** â se quiser algo mais organizado e confortÃ¡vel\n"
                "â¢ **Extra / Carrefour** â se a ideia for compra mais completa"
            )

    if topic == "saude":
        return get_health_reply(text)

    if topic == "incidente":
        return get_problem_reply(text)

    if topic == "bares":
        if has_any(text_n, ["animado"]):
            return "Se vocÃªs quiserem algo mais animado, eu buscaria uma saÃ­da com mais movimento e clima de noite ð»"
        if has_any(text_n, ["tranquilo"]):
            return "Se a ideia for algo mais tranquilo, eu priorizaria uma saÃ­da leve, sem muita agitaÃ§Ã£o, sÃ³ pra curtir com calma ð"

    return ""


def infer_contextual_followup(text_raw, last_topic):
    text_n = normalize_text(text_raw)

    generic_followups = [
        "mais perto", "perto", "mais barato", "barato",
        "mais especial", "especial", "mais completo", "completo",
        "mais rapido", "mais rÃ¡pido", "agora", "pra hoje", "pra hoje a noite",
        "hoje", "algo leve", "algo melhor", "qual melhor", "qual voce indica",
        "qual voce recomenda", "qual vc recomenda", "qual vc indica", "animado", "tranquilo"
    ]

    if has_any(text_n, generic_followups):
        return last_topic

    if len(text_n.split()) <= 4 and last_topic:
        return last_topic

    return ""


def finalize_response(response, inferred_intent, emotion, topic=None):
    topic_to_store = topic or inferred_intent or ""
    update_session(
        intent=inferred_intent or "",
        emotion=emotion or "",
        topic=topic_to_store,
        suggestion_type=inferred_intent or ""
    )
    response = adapt_response_by_urgency(response, emotion)
    response += add_proactive_layer(inferred_intent or "")
    return response


def gepetto_responde(msg):
    guest = load_guest()
    text_raw = msg or ""
    text = normalize_text(text_raw)
    emotion = detect_emotion(text_raw)
    last_topic = get_last_topic()
    filters = extract_filters(text_raw)

    if text_raw.startswith("/"):
        admin_reply = handle_admin_command(text_raw)
        if admin_reply is not None:
            append_memory("user", text_raw, "admin", "admin", filters, emotion)
            append_memory("assistant", admin_reply, "admin", "admin")
            return admin_reply

    followup = get_followup_reply(text_raw, last_topic, guest)
    if followup:
        followup = finalize_response(followup, last_topic, emotion, last_topic)
        append_memory("user", text_raw, last_topic, last_topic, filters, emotion)
        append_memory("assistant", followup, last_topic, last_topic)
        return followup

    inferred_intent = infer_intent(text_raw, last_topic)
    inferred_intent = resolve_intent_with_session(text_raw, inferred_intent)

    if has_any(text, ["oi", "ola", "olÃ¡", "cheguei", "chegamos", "boa tarde", "bom dia", "boa noite"]):
        reply = (
            f"{saudacao_personalizada(guest)}\n\n"
            "Que bom que vocÃª chegou!\n\n"
            f"{proactive_prompt(guest)}"
        )
        append_memory("user", text_raw, "saudacao", "saudacao", filters, emotion)
        append_memory("assistant", reply, "saudacao", "saudacao")
        return reply

    if inferred_intent == "identidade":
        reply = finalize_response(get_identidade_reply(text_raw), inferred_intent, emotion)
        append_memory("user", text_raw, "identidade", "identidade", filters, emotion)
        append_memory("assistant", reply, "identidade", "identidade")
        return reply

    if inferred_intent == "localizacao":
        reply = finalize_response(get_localizacao_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "localizacao", "localizacao", filters, emotion)
        append_memory("assistant", reply, "localizacao", "localizacao")
        return reply

    if inferred_intent == "saude":
        reply = get_health_reply(text_raw)
        sev = classify_health(text_raw)
        if sev in ["alta", "media"]:
            maybe_notify_incident(text_raw, guest, kind="saude", severity=sev)
        reply = finalize_response(reply, inferred_intent, emotion or "saude")
        append_memory("user", text_raw, "saude", "saude", filters, emotion or "saude")
        append_memory("assistant", reply, "saude", "saude")
        return reply

    if inferred_intent == "incidente":
        reply = get_problem_reply(text_raw)
        maybe_notify_incident(text_raw, guest, kind="incidente")
        reply = finalize_response(reply, inferred_intent, emotion or "urgente")
        append_memory("user", text_raw, "incidente", "incidente", filters, emotion or "urgente")
        append_memory("assistant", reply, "incidente", "incidente")
        return reply

    if has_any(text, ["wifi", "wi-fi", "internet"]):
        reply = finalize_response(get_wifi_reply(), "wifi", emotion)
        append_memory("user", text_raw, "wifi", "wifi", filters, emotion)
        append_memory("assistant", reply, "wifi", "wifi")
        return reply

    if has_any(text, ["regra", "regras", "casa", "condominio", "silencio", "lixo", "areia"]):
        reply = finalize_response(get_regras_reply(), "regras", emotion)
        append_memory("user", text_raw, "regras", "regras", filters, emotion)
        append_memory("assistant", reply, "regras", "regras")
        return reply

    if "onde fica" in text and "praia" in text:
        reply = finalize_response(get_servico_praia_localizacao_reply(), "praia", emotion, "praia")
        append_memory("user", text_raw, "praia", "praia", filters, emotion)
        append_memory("assistant", reply, "praia", "praia")
        return reply

    if inferred_intent == "praia":
        reply = finalize_response(get_praia_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "praia", "praia", filters, emotion)
        append_memory("assistant", reply, "praia", "praia")
        return reply

    if inferred_intent == "roteiro":
        reply = finalize_response(get_roteiro_reply(guest), inferred_intent, emotion)
        append_memory("user", text_raw, "roteiro", "roteiro", filters, emotion)
        append_memory("assistant", reply, "roteiro", "roteiro")
        return reply

    if inferred_intent == "restaurantes":
        reply = finalize_response(get_restaurantes_reply(text_raw), inferred_intent, emotion)
        append_memory("user", text_raw, "restaurantes", "restaurantes", filters, emotion)
        append_memory("assistant", reply, "restaurantes", "restaurantes")
        return reply

    if inferred_intent == "mercado":
        reply = finalize_response(get_mercado_reply(text_raw), inferred_intent, emotion)
        append_memory("user", text_raw, "mercado", "mercado", filters, emotion)
        append_memory("assistant", reply, "mercado", "mercado")
        return reply

    if inferred_intent == "padaria":
        reply = finalize_response(get_padaria_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "padaria", "padaria", filters, emotion)
        append_memory("assistant", reply, "padaria", "padaria")
        return reply

    if inferred_intent == "farmacia":
        reply = finalize_response(get_farmacia_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "farmacia", "farmacia", filters, emotion)
        append_memory("assistant", reply, "farmacia", "farmacia")
        return reply

    if has_any(text, ["garagem", "vaga", "estacionar", "estacionamento"]):
        reply = finalize_response(get_garagem_reply(), "garagem", emotion)
        append_memory("user", text_raw, "garagem", "garagem", filters, emotion)
        append_memory("assistant", reply, "garagem", "garagem")
        return reply

    if has_any(text, ["checkout", "check-out", "check out"]):
        reply = finalize_response(get_checkout_reply(guest), "checkout", emotion)
        append_memory("user", text_raw, "checkout", "checkout", filters, emotion)
        append_memory("assistant", reply, "checkout", "checkout")
        return reply

    if has_any(text, ["bruno", "anfitriao", "anfitriÃ£o", "host"]):
        reply = finalize_response(get_bruno_reply(), "bruno", emotion)
        append_memory("user", text_raw, "bruno", "bruno", filters, emotion)
        append_memory("assistant", reply, "bruno", "bruno")
        return reply

    if inferred_intent == "bares":
        reply = finalize_response(get_bares_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "bares", "bares", filters, emotion)
        append_memory("assistant", reply, "bares", "bares")
        return reply

    if inferred_intent == "shopping":
        reply = finalize_response(get_shopping_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "shopping", "shopping", filters, emotion)
        append_memory("assistant", reply, "shopping", "shopping")
        return reply

    if has_any(text, ["feira", "artesanato", "feirinha"]):
        reply = finalize_response(get_feira_reply(), "feira", emotion)
        append_memory("user", text_raw, "feira", "feira", filters, emotion)
        append_memory("assistant", reply, "feira", "feira")
        return reply

    if has_any(text, ["tempo", "clima", "previsao", "previsÃ£o", "meteorologia", "vai chover"]):
        reply = finalize_response(get_tempo_reply(), "tempo", emotion)
        append_memory("user", text_raw, "tempo", "tempo", filters, emotion)
        append_memory("assistant", reply, "tempo", "tempo")
        return reply

    if inferred_intent == "passeio":
        reply = finalize_response(get_acqua_mundo_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "passeio", "passeio", filters, emotion)
        append_memory("assistant", reply, "passeio", "passeio")
        return reply

    if inferred_intent == "eventos":
        reply = finalize_response(get_eventos_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "eventos", "eventos", filters, emotion)
        append_memory("assistant", reply, "eventos", "eventos")
        return reply

    if inferred_intent == "surf":
        reply = finalize_response(get_surf_reply(), inferred_intent, emotion)
        append_memory("user", text_raw, "surf", "surf", filters, emotion)
        append_memory("assistant", reply, "surf", "surf")
        return reply

    reply = finalize_response(get_fallback_reply(guest), "", emotion, "fallback")
    append_memory("user", text_raw, "fallback", "fallback", filters, emotion)
    append_memory("assistant", reply, "fallback", "fallback")
    return reply


@app.route("/")
def home():
    return send_from_directory("static", "index.html")


@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json() or {}
    msg = data.get("message", "")
    resposta = gepetto_responde(msg)
    return jsonify({"reply": resposta})


@app.route("/welcome", methods=["GET"])
def welcome():
    return jsonify({"message": mensagem_boas_vindas()})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
