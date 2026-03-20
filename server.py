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

ADMIN_PIN = "2710"

# Telegram: configurar no Railway
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# =========================
# ARQUIVOS / ESTADO
# =========================

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

KNOWLEDGE = load_knowledge()
ADMIN_UNLOCKED = False

# =========================
# HELPERS
# =========================

def normalize_text(text: str) -> str:
    text = (text or "").lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text

def has_any(text: str, terms) -> bool:
    return any(normalize_text(term) in normalize_text(text) for term in terms)

def get_recent_messages(limit=8):
    return load_memory().get("messages", [])[-limit:]

def get_last_topic():
    for item in reversed(get_recent_messages(15)):
        topic = item.get("topic", "")
        if topic and topic not in ["admin", "fallback", "saudacao"]:
            return topic
    return ""

def get_last_filters():
    for item in reversed(get_recent_messages(15)):
        filters = item.get("filters", {})
        if filters:
            return filters
    return {}

def get_last_user_text():
    for item in reversed(get_recent_messages(15)):
        if item.get("role") == "user":
            return normalize_text(item.get("text", ""))
    return ""

def current_time_label():
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "manhã"
    if 12 <= hour < 18:
        return "tarde"
    return "noite"

def guest_group_label(guest):
    grupo = (guest.get("grupo") or "").strip().lower()
    if grupo == "familia":
        return "família"
    if grupo == "amigos":
        return "amigos"
    if grupo == "casal":
        return "casal"
    return ""

def saudacao_personalizada(guest):
    nome = (guest.get("nome") or "").strip()
    grupo = (guest.get("grupo") or "").strip().lower()

    if not nome:
        return "Olá 😊"

    if grupo == "familia":
        return f"Olá {nome} e família 😊"
    if grupo == "amigos":
        return f"Olá {nome} e amigos 😄"
    if grupo == "casal":
        return f"Olá {nome} 😊"

    return f"Olá {nome} 😊"

def mensagem_boas_vindas():
    guest = load_guest()
    inicio = saudacao_personalizada(guest)

    return (
        f"{inicio}\n\n"
        "🌴 Bem-vindo à praia da Enseada!\n\n"
        "É uma honra ter você hospedado aqui 😊 Espero que tenha feito uma ótima viagem!\n\n"
        "Eu sou o **Gepetto**, seu concierge pessoal durante a estadia.\n\n"
        "Posso te ajudar com:\n"
        "• **Guia do apartamento e do condomínio**\n"
        "• **Recomendações de restaurantes**\n"
        "• **Mercados e conveniências**\n"
        "• **Praia, passeios e dicas locais**\n\n"
        "Fique à vontade para me chamar a qualquer momento 😉"
    )

def proactive_prompt(guest):
    grupo = guest_group_label(guest)

    if grupo == "família":
        return (
            "Se quiser, posso te indicar agora:\n"
            "• uma boa opção de **restaurante** 🍽️\n"
            "• um **mercado próximo** 🛒\n"
            "• ou te explicar rapidinho como funciona a **praia** 🏖️"
        )

    if grupo == "amigos":
        return (
            "Se quiser, posso te indicar agora:\n"
            "• um lugar bom pra **comer ou jantar** 🍽️\n"
            "• uma opção rápida de **mercado** 🛒\n"
            "• ou já te passar como funciona a **praia** 🏖️"
        )

    if grupo == "casal":
        return (
            "Se quiser, posso te indicar agora:\n"
            "• um restaurante mais **especial** ✨\n"
            "• uma opção rápida de **mercado** 🛒\n"
            "• ou te orientar sobre a **praia** 🏖️"
        )

    opcoes = [
        "Se quiser, posso te indicar agora um **restaurante**, um **mercado** ou te explicar como funciona a **praia** 😉",
        "Posso te ajudar agora com **praia**, **mercado**, **restaurantes** ou qualquer dúvida do apartamento 😄",
        "Se preferir, já posso começar te orientando sobre **praia**, **comida** ou **compras rápidas** 👍"
    ]
    return random.choice(opcoes)

def should_send_telegram():
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

def send_telegram_message(message):
    if not should_send_telegram():
        return False, "Telegram não configurado"

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

# =========================
# CLASSIFICAÇÃO
# =========================

def classify_incident(text):
    text_n = normalize_text(text)

    high = [
        "vazamento", "sem energia", "porta nao abre", "nao entra",
        "curto", "fogo", "incendio", "incêndio", "fumaca", "fumaça",
        "gas", "gás", "explosao", "explosão", "cheiro de queimado",
        "queimando", "pegando fogo"
    ]

    medium = [
        "chuveiro", "ar nao funciona", "ar não funciona", "tv nao liga", "tv não liga",
        "wifi nao funciona", "wifi não funciona", "internet nao funciona", "internet não funciona",
        "parou de funcionar", "quebrou", "defeito", "nao funciona", "não funciona",
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
        "desmaiou", "desmaio", "nao consegue respirar", "não consegue respirar",
        "falta de ar", "muita dor", "dor forte", "sangrando",
        "muito mal", "urgente", "emergencia", "emergência", "dor no peito"
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

def detect_emotion(text):
    text_n = normalize_text(text)

    if has_any(text_n, ["urgente", "socorro", "me ajuda", "ajuda", "desespero", "medo"]):
        return "urgente"

    if has_any(text_n, ["que droga", "ruim", "chato", "nao gostei", "não gostei", "pessimo", "péssimo", "horrivel", "horrível"]):
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

    label_guest = guest.get("nome") or "Hóspede sem nome definido"
    emoji = "🚨" if sev == "alta" else "⚠️"
    tg_msg = (
        f"{emoji} {kind.upper()} NO APTO 14B\n\n"
        f"Gravidade: {sev.upper()}\n"
        f"Hóspede: {label_guest}\n"
        f"Mensagem: {raw_message}\n"
        f"Horário: {payload['timestamp']}"
    )
    send_telegram_message(tg_msg)

# =========================
# INTENÇÃO / FILTROS
# =========================

def infer_intent(text, last_topic=""):
    text_n = normalize_text(text)

    if len(text_n.split()) <= 4 and last_topic and has_any(
        text_n,
        [
            "mais barato", "mais perto", "mais especial", "mais completo",
            "mais rapido", "mais rápido", "melhor", "qual melhor", "qual",
            "barato", "especial", "rapido", "rápido", "agora", "hoje",
            "leve", "romantico", "romântico", "animado", "tranquilo"
        ]
    ):
        return last_topic

    if has_any(text_n, ["restaurante", "jantar", "almoco", "almoço", "comer", "comida", "lanche", "hamburguer", "hambúrguer", "sushi", "sobremesa", "doce", "chocolate", "fome"]):
        return "restaurantes"

    if has_any(text_n, ["mercado", "supermercado", "compras", "pao de acucar", "pão de açúcar", "carrefour", "extra", "agua", "água", "bebida"]):
        return "mercado"

    if has_any(text_n, ["praia", "servico de praia", "serviço de praia", "guarda-sol", "guarda sol", "cadeira de praia"]):
        return "praia"

    if has_any(text_n, ["bar", "bares", "pub", "cerveja", "beber", "drink", "drinks", "noite"]):
        return "bares"

    if has_any(text_n, ["shopping", "la plage"]):
        return "shopping"

    if has_any(text_n, ["padaria", "cafe", "café", "cafe da manha", "café da manhã"]):
        return "padaria"

    if has_any(text_n, ["farmacia", "farmácia", "remedio", "remédio"]):
        return "farmacia"

    if has_any(text_n, ["hospital", "upa", "doente", "dor", "febre", "passando mal", "mal estar", "mal-estar", "medico", "médico", "saude", "saúde"]):
        return "saude"

    if has_any(text_n, ["fogo", "incendio", "incêndio", "fumaca", "fumaça", "gas", "gás", "curto", "vazamento", "queimado", "queimou", "cheiro de queimado"]):
        return "incidente"

    if has_any(text_n, ["roteiro", "o que fazer hoje", "plano pro dia", "sugestao de roteiro", "sugestão de roteiro"]):
        return "roteiro"

    if has_any(text_n, ["passeio", "passeios", "chuva", "crianca", "criança", "aquario", "aquário", "acqua mundo"]):
        return "passeio"

    if has_any(text_n, ["evento", "eventos", "show", "shows", "festa"]):
        return "eventos"

    if has_any(text_n, ["surf", "ondas", "mar", "pico de surf"]):
        return "surf"

    if has_any(text_n, ["gepetto", "gepeto", "como voce chama", "qual seu nome", "quem e voce", "quem te criou", "quem te fez"]):
        return "identidade"

    if has_any(text_n, ["onde estamos", "qual o endereco", "qual o endereço", "me passa o endereco", "me passa o endereço"]):
        return "localizacao"

    return ""

def extract_filters(text):
    text_n = normalize_text(text)
    filters = {}

    if has_any(text_n, ["barato", "economico", "econômico", "em conta"]):
        filters["preco"] = "barato"

    if has_any(text_n, ["perto", "mais perto", "proximo", "próximo"]):
        filters["distancia"] = "perto"

    if has_any(text_n, ["especial", "romantico", "romântico", "sofisticado", "premium"]):
        filters["estilo"] = "especial"

    if has_any(text_n, ["rapido", "rápido", "pratico", "prático"]):
        filters["ritmo"] = "rapido"

    if has_any(text_n, ["leve"]):
        filters["tipo"] = "leve"

    if has_any(text_n, ["hoje", "agora", "pra hoje", "para hoje"]):
        filters["momento"] = "agora"

    if has_any(text_n, ["mais tarde", "a noite", "à noite", "noite"]):
        filters["momento"] = "noite"

    return filters

# =========================
# RESPOSTAS
# =========================

def get_wifi_reply():
    return (
        "Claro 😊\n\n"
        "📶 Usuário: **Volare Hal**\n"
        "🔑 Senha: **Guaruja123@**"
    )

def get_identidade_reply(text):
    text_n = normalize_text(text)

    if has_any(text_n, ["quem te fez", "quem te criou"]):
        return "O **Bruno** me criou para proporcionar a melhor experiência possível para vocês durante a hospedagem ✨"

    if has_any(text_n, ["como voce chama", "qual seu nome", "quem e voce"]):
        return "Eu sou o **Gepetto**, seu concierge particular 😊"

    return "Oi 😊 Eu sou o **Gepetto**. Em que posso ser útil?"

def get_localizacao_reply():
    return (
        "Estamos na deliciosa **praia da Enseada, no Guarujá** 😊\n\n"
        "📍 **Residencial Volare – apto 14B**\n"
        "**Avenida da Saudade, 335**\n"
        "**Jardim São Miguel**\n"
        "**CEP: 11440-180**\n\n"
        "Se quiser, posso te passar esse endereço para iFood, Uber ou entregas."
    )

def get_regras_reply():
    regras = KNOWLEDGE.get("regras", {})
    return (
        "Claro 😊\n\n"
        "Algumas informações importantes:\n"
        f"• Silêncio: {regras.get('silencio', '23h às 7h')}\n"
        f"• Areia: {regras.get('areia', 'usar lava-pés antes de entrar no elevador')}\n"
        f"• Lixo: {regras.get('lixo', 'há ponto de descarte no térreo')}"
    )

def get_praia_reply():
    praia = KNOWLEDGE.get("praia", {})
    servico = praia.get("servico_praia", {})
    return (
        "Boa escolha 😄\n\n"
        f"A praia fica a {praia.get('distancia', 'poucos minutos a pé')}.\n"
        f"O serviço de praia funciona das {servico.get('horario', '9h às 17h')}.\n"
        f"Ele fica {servico.get('localizacao', 'ao lado do Thai Lounge, em frente ao Casa Grande Hotel')}.\n\n"
        f"{servico.get('como_funciona', 'Os itens já ficam montados na areia.')}\n\n"
        "Se quiser, também posso te explicar rapidinho como aproveitar melhor esse primeiro dia de praia 😉"
    )

def get_servico_praia_localizacao_reply():
    praia = KNOWLEDGE.get("praia", {})
    servico = praia.get("servico_praia", {})
    return (
        "Claro 😊\n\n"
        f"O serviço de praia fica {servico.get('localizacao', 'em frente ao Casa Grande Hotel')}."
    )

def get_restaurantes_reply(text):
    text_n = normalize_text(text)

    if has_any(text_n, ["barato", "economico", "simples", "rapido"]):
        return (
            "Boa 😄\n\n"
            "Se a ideia for algo mais simples ou prático, eu começaria por algo rápido por perto e deixaria algo mais especial para a noite.\n\n"
            "Uma opção prática nesse estilo é o **McDonald's**, que fica a cerca de **5 minutos de carro**.\n\n"
            "Se quiser, também posso te sugerir algo mais tradicional sem pesar tanto 😉"
        )

    if has_any(text_n, ["especial", "romantico", "sofisticado", "premium"]):
        return (
            "Boa 😄\n\n"
            "Se quiser algo mais especial, o **Thai Lounge** costuma ser uma ótima pedida ✨\n\n"
            "Se preferir algo mais clássico e tradicional, o **Restaurante Alcides** também é uma excelente escolha."
        )

    if has_any(text_n, ["japones", "japonesa", "sushi"]):
        return (
            "Se a vontade for comida japonesa 🍣\n\n"
            "Uma boa referência é o **Sushi Katoshi**, que fica a cerca de **4 minutos de carro**."
        )

    if has_any(text_n, ["hamburguer", "hambúrguer", "lanche", "mcdonald", "mcdonald's"]):
        return (
            "Se quiser algo mais rápido 🍔\n\n"
            "O **McDonald's** fica a cerca de **5 minutos de carro**."
        )

    if has_any(text_n, ["doce", "sobremesa", "chocolate", "kopenhagen"]):
        return (
            "Se a ideia for um doce ou uma lembrança gostosa 🍫\n\n"
            "A **Kopenhagen** fica a cerca de **4 minutos de carro**."
        )

    if has_any(text_n, ["leve"]):
        return (
            "Se você quiser algo mais leve 😊\n\n"
            "Eu seguiria por uma linha mais tranquila, sem pesar tanto, e deixaria algo mais completo para outro momento.\n\n"
            "Se quiser, te direciono melhor entre **rápido**, **leve** ou **especial**."
        )

    return (
        "Boa 😄\n\n"
        "Aqui vão algumas boas referências por perto:\n\n"
        "• **Restaurante Alcides** → clássico, tradicional e muito lembrado no Guarujá 🦐\n"
        "• **Thai Lounge** → vibe mais especial e experiência mais sofisticada ✨\n"
        "• **Sushi Katoshi** → comida japonesa, a cerca de **4 minutos de carro** 🍣\n"
        "• **McDonald's** → opção prática, a cerca de **5 minutos de carro** 🍔\n"
        "• **Kopenhagen** → chocolateria, a cerca de **4 minutos de carro** 🍫\n\n"
        "Se quiser, também posso te sugerir uma opção mais **rápida**, mais **especial**, mais **tradicional** ou algo para **sobremesa** 😉"
    )

def get_mercado_reply(text):
    text_n = normalize_text(text)

    if has_any(text_n, ["rapido", "rápido", "perto", "urgente"]):
        return (
            "Pra algo rápido 🛒\n\n"
            "• **Supermercado Dia** → praticamente ao lado (menos de 1 min a pé)\n\n"
            "Perfeito pra água, bebida ou emergência 👍"
        )

    if has_any(text_n, ["completo", "grande", "variedade"]):
        return (
            "Se quiser um mercado mais completo:\n\n"
            "• **Pão de Açúcar** → cerca de **3 minutos de carro**\n"
            "• **Carrefour** → cerca de **5 minutos de carro**\n"
            "• **Extra** → cerca de **5 minutos de carro**\n\n"
            "Essas opções fazem mais sentido quando você quer mais variedade 🚗"
        )

    return (
        "Aqui vão boas opções próximas 😊\n\n"
        "• **Supermercado Dia** → ao lado (ultra prático)\n"
        "• **Pão de Açúcar** → cerca de **3 minutos de carro**\n"
        "• **Extra** → cerca de **5 minutos de carro**\n"
        "• **Carrefour** → cerca de **5 minutos de carro**\n\n"
        "Se quiser, te indico o melhor dependendo do que você precisa 😉"
    )

def get_padaria_reply():
    return (
        "Se você quiser padaria ou café da manhã 😊\n\n"
        "Uma referência prática é a **Padaria Pitangueiras**, a cerca de **300m do apartamento**.\n\n"
        "Se quiser algo mais rápido ou mais caprichado, também posso te orientar por estilo."
    )

def get_farmacia_reply():
    return (
        "Se você estiver precisando de farmácia, posso te orientar para uma opção próxima e prática.\n\n"
        "Se for algo urgente ou mais delicado, também posso te indicar atendimento rápido na região 👍"
    )

def get_garagem_reply():
    info = KNOWLEDGE.get("garagem", {}).get("info", "")
    return info or "A vaga não é fixa. Ao chegar, vale confirmar com o funcionário do prédio onde estacionar 😊"

def get_checkout_reply(guest):
    checkout = guest.get("checkout") or KNOWLEDGE.get("apartamento", {}).get("checkout", "CHECKOUT_HORARIO")
    return f"O check-out está configurado para: **{checkout}** 😊"

def get_bruno_reply():
    return (
        "Se preferir falar diretamente com o Bruno, posso avisá-lo rapidamente 😊\n\n"
        "Ele receberá uma notificação e poderá entrar em contato com você pelo Airbnb."
    )

def get_emergencia_reply():
    proximidades = KNOWLEDGE.get("proximidades", {})
    upa = proximidades.get("upa_enseada", {})
    hospital = proximidades.get("hospital_santo_amaro", {})

    return (
        "Se for algo de saúde ou urgência, posso te orientar assim:\n\n"
        f"• **UPA Enseada** → cerca de {upa.get('tempo_carro', '4 a 6 minutos de carro')}\n"
        f"• **Hospital Santo Amaro** → cerca de {hospital.get('tempo_carro', '10 a 15 minutos de carro')}\n\n"
        "Se for uma urgência real, priorize atendimento imediato."
    )

def get_health_reply(text):
    sev = classify_health(text)
    if sev == "alta":
        return (
            "Isso parece importante ⚠️\n\n"
            "Se for uma situação urgente, priorize atendimento imediato.\n\n"
            "Posso te orientar rapidamente para **UPA Enseada** ou **Hospital Santo Amaro**.\n\n"
            "Enquanto isso, já estou deixando isso sinalizado por aqui."
        )
    return (
        "Poxa, entendi 😕\n\n"
        "Se você não estiver se sentindo bem, posso te orientar para farmácia ou atendimento na região.\n\n"
        "Se quiser, já te digo qual caminho faz mais sentido."
    )

def get_acqua_mundo_reply():
    acqua = KNOWLEDGE.get("proximidades", {}).get("acqua_mundo", {})
    return (
        f"Uma boa opção por aqui é o **{acqua.get('nome', 'Acqua Mundo')}** 😊\n\n"
        f"{acqua.get('observacao', 'Costuma funcionar muito bem para famílias e também em dias de chuva.')}\n\n"
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
    return (
        "Se você curte surf, posso te ajudar com uma orientação geral sobre os picos mais lembrados por aqui 🌊\n\n"
        "Entre os mais conhecidos estão:\n"
        "• **Tombo**\n"
        "• **Enseada**\n"
        "• **Pitangueiras**\n"
        "• **Pernambuco / Mar Casado**\n\n"
        "Se quiser, posso sugerir qual parece fazer mais sentido para o seu perfil."
    )

def get_problem_reply(text):
    sev = classify_incident(text)
    if sev == "alta":
        return (
            "Isso parece importante ⚠️\n\n"
            "Se for seguro, se afaste do local ou desligue o equipamento, quando fizer sentido.\n\n"
            "Já estou deixando isso sinalizado como prioridade."
        )
    if sev == "media":
        return (
            "Entendi 👍\n\n"
            "Vou te ajudar com isso.\n\n"
            "Me conta só se aconteceu do nada ou se já estava assim antes."
        )
    return (
        "Poxa, que pena 😕\n\n"
        "Me conta exatamente o que aconteceu.\n\n"
        "Já estava assim antes ou aconteceu do nada?\n\n"
        "Enquanto isso, eu já deixo isso encaminhado por aqui 👍"
    )

def get_bares_reply():
    return (
        "Se a ideia for sair à noite 🍻\n\n"
        "Encontrei alguns estabelecimentos próximos ao apartamento:\n\n"
        "• **Dona Eva Bar & Chopperia**\n"
        "• **Bali Hai**\n"
        "• **Quiosques da Orla da Enseada**\n"
        "• **Boteco da Orla**\n\n"
        "São opções mais descontraídas para curtir a noite 😊\n\n"
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
    return (
        "A parte de meteorologia eu vou deixar para a próxima camada ☀️🌧️\n\n"
        "Quando você quiser, eu consigo preparar o Gepetto para previsão do tempo em tempo real."
    )

def get_roteiro_reply(guest):
    parte_do_dia = current_time_label()

    if parte_do_dia == "manhã":
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

# =========================
# CONTEXTO / FOLLOW-UP
# =========================

def infer_contextual_followup(text_raw, last_topic):
    text_n = normalize_text(text_raw)

    generic_followups = [
        "mais perto", "perto", "mais barato", "barato",
        "mais especial", "especial", "mais completo", "completo",
        "mais rapido", "mais rápido", "agora", "pra hoje", "pra hoje a noite",
        "hoje", "algo leve", "algo melhor", "qual melhor", "qual voce indica",
        "qual voce recomenda", "qual vc recomenda", "qual vc indica", "animado", "tranquilo"
    ]

    if has_any(text_n, generic_followups):
        return last_topic

    if len(text_n.split()) <= 4 and last_topic:
        return last_topic

    return ""

def get_followup_reply(text, last_topic, guest):
    text_n = normalize_text(text)
    topic = infer_contextual_followup(text, last_topic)

    if topic == "restaurantes":
        if has_any(text_n, ["mais perto", "perto"]):
            return (
                "Se a prioridade for praticidade, eu focaria no que te faça sair e voltar com mais conforto.\n\n"
                "Se quiser, eu sigo numa linha mais **prática**, mais **especial** ou mais **tradicional** 😉"
            )
        if has_any(text_n, ["mais barato", "barato", "economico", "econômico", "algo leve"]):
            return get_restaurantes_reply("barato")
        if has_any(text_n, ["mais especial", "especial", "romantico", "romântico", "sofisticado"]):
            return get_restaurantes_reply("especial")
        if has_any(text_n, ["qual melhor", "qual voce recomenda", "qual vc recomenda", "qual voce indica", "qual vc indica"]):
            return (
                "Se eu tivesse que te direcionar sem erro 😊\n\n"
                "• **Thai Lounge** → se você quiser algo mais especial\n"
                "• **Restaurante Alcides** → se quiser algo clássico e tradicional\n"
                "• **McDonald's** → se a ideia for praticidade\n"
                "• **Sushi Katoshi** → se estiver com vontade de japonês 🍣"
            )

    if topic == "praia":
        if has_any(text_n, ["mais tarde", "agora", "hoje"]):
            return (
                "Se for ainda hoje, eu aproveitaria enquanto o serviço está funcionando e já deixaria o fim do dia mais leve 😉"
            )
        if has_any(text_n, ["como funciona", "funciona como"]):
            return get_praia_reply()

    if topic == "mercado":
        if has_any(text_n, ["mais completo", "completo", "grande", "variedade"]):
            return get_mercado_reply("completo")
        if has_any(text_n, ["mais perto", "perto", "rapido", "rápido"]):
            return get_mercado_reply("rapido")
        if has_any(text_n, ["qual melhor", "qual voce recomenda", "qual vc recomenda"]):
            return (
                "Depende do que você precisa 😊\n\n"
                "• **Dia** → se quiser algo rápido\n"
                "• **Pão de Açúcar** → se quiser algo mais organizado e confortável\n"
                "• **Extra / Carrefour** → se a ideia for compra mais completa"
            )

    if topic == "bares":
        if has_any(text_n, ["animado"]):
            return "Se vocês quiserem algo mais animado, eu buscaria uma saída com mais movimento e clima de noite 🍻"
        if has_any(text_n, ["tranquilo"]):
            return "Se a ideia for algo mais tranquilo, eu priorizaria uma saída leve, sem muita agitação, só pra curtir com calma 😊"

    if topic == "saude":
        return get_health_reply(text)

    if topic == "incidente":
        return get_problem_reply(text)

    return ""

def get_guided_reply(text):
    text_n = normalize_text(text)

    if has_any(text_n, ["comer", "jantar", "fome", "lanche", "leve"]):
        return (
            "Claro 😄\n\n"
            "Pra eu te direcionar melhor, você quer algo:\n"
            "• mais **rápido**\n"
            "• mais **especial**\n"
            "• ou mais **tradicional**?"
        )

    if has_any(text_n, ["comprar", "mercado", "supermercado"]):
        return (
            "Posso te ajudar nisso 👍\n\n"
            "Você quer algo:\n"
            "• **rápido**\n"
            "• ou um mercado mais **completo**?"
        )

    if has_any(text_n, ["sair", "noite", "beber", "drink"]):
        return (
            "Boa 😄\n\n"
            "Você quer algo mais:\n"
            "• **animado**\n"
            "• ou mais **tranquilo**?"
        )

    return ""

def get_fallback_reply(guest):
    memory = load_memory()
    last_msgs = memory.get("messages", [])[-3:]
    fallback_count = sum(1 for m in last_msgs if m.get("topic") == "fallback")

    if fallback_count >= 2:
        return (
            "Peço desculpas 😅\n\n"
            "Ainda estou em fase de testes beta e infelizmente não compreendi sua questão.\n\n"
            "Se puder tentar escrever de outra maneira, eu agradeço 🙏"
        )

    guided = get_guided_reply(get_last_user_text())
    if guided:
        return guided

    base = [
        "Me diz o que você precisa que eu te ajudo 😄",
        "Bora resolver isso 👍 Me fala se você quer ajuda com o apartamento ou com alguma dica da região.",
        "Consigo te direcionar nisso 😊 Me diz se a ideia é comida, praia, compras ou algo do apartamento."
    ]

    extra = proactive_prompt(guest)
    return f"{random.choice(base)}\n\n{extra}"

# =========================
# MODO ADMIN
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
                "/set observacoes gosta de praia cedo\n"
                "/show\n"
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

    if cmd == "/reset":
        save_guest(default_guest())
        reset_memory()
        return "Dados do hóspede resetados ♻️"

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
        guest[field] = value
        save_guest(guest)

        return f"{field} atualizado para: {value} ✅"

    return None

# =========================
# RESPOSTAS DO GEPETTO
# =========================

def gepetto_responde(msg):
    guest = load_guest()
    text_raw = msg or ""
    text = normalize_text(text_raw)
    emotion = detect_emotion(text_raw)
    last_topic = get_last_topic()
    filters = extract_filters(text_raw)

    # Admin
    if text_raw.startswith("/"):
        admin_reply = handle_admin_command(text_raw)
        if admin_reply is not None:
            append_memory("user", text_raw, "admin", "admin", filters, emotion)
            append_memory("assistant", admin_reply, "admin", "admin")
            return admin_reply

    # Follow-up
    followup = get_followup_reply(text_raw, last_topic, guest)
    if followup:
        append_memory("user", text_raw, last_topic, last_topic, filters, emotion)
        append_memory("assistant", followup, last_topic, last_topic)
        return followup

    inferred_intent = infer_intent(text_raw, last_topic)

    # Saudações
    if has_any(text, ["oi", "ola", "cheguei", "chegamos", "boa tarde", "bom dia", "boa noite"]):
        reply = (
            f"{saudacao_personalizada(guest)}\n\n"
            "Que bom que você chegou!\n\n"
            f"{proactive_prompt(guest)}"
        )
        append_memory("user", text_raw, "saudacao", "saudacao", filters, emotion)
        append_memory("assistant", reply, "saudacao", "saudacao")
        return reply

    # Identidade
    if inferred_intent == "identidade":
        reply = get_identidade_reply(text_raw)
        append_memory("user", text_raw, "identidade", "identidade", filters, emotion)
        append_memory("assistant", reply, "identidade", "identidade")
        return reply

    # Localização
    if inferred_intent == "localizacao":
        reply = get_localizacao_reply()
        append_memory("user", text_raw, "localizacao", "localizacao", filters, emotion)
        append_memory("assistant", reply, "localizacao", "localizacao")
        return reply

    # Saúde
    if inferred_intent == "saude":
        reply = get_health_reply(text_raw)
        sev = classify_health(text_raw)
        if sev in ["alta", "media"]:
            maybe_notify_incident(text_raw, guest, kind="saude", severity=sev)
        append_memory("user", text_raw, "saude", "saude", filters, emotion or "saude")
        append_memory("assistant", reply, "saude", "saude")
        return reply

    # Incidentes
    if inferred_intent == "incidente":
        reply = get_problem_reply(text_raw)
        maybe_notify_incident(text_raw, guest, kind="incidente")
        append_memory("user", text_raw, "incidente", "incidente", filters, emotion or "urgente")
        append_memory("assistant", reply, "incidente", "incidente")
        return reply

    # Wi-Fi
    if has_any(text, ["wifi", "wi-fi", "internet"]):
        reply = get_wifi_reply()
        append_memory("user", text_raw, "wifi", "wifi", filters, emotion)
        append_memory("assistant", reply, "wifi", "wifi")
        return reply

    # Regras / condomínio / casa
    if has_any(text, ["regra", "regras", "casa", "condominio", "silencio", "lixo", "areia"]):
        reply = get_regras_reply()
        append_memory("user", text_raw, "regras", "regras", filters, emotion)
        append_memory("assistant", reply, "regras", "regras")
        return reply

    # Praia - localização específica antes
    if "onde fica" in text and "praia" in text:
        reply = get_servico_praia_localizacao_reply()
        append_memory("user", text_raw, "praia", "praia", filters, emotion)
        append_memory("assistant", reply, "praia", "praia")
        return reply

    # Praia
    if inferred_intent == "praia":
        reply = get_praia_reply()
        append_memory("user", text_raw, "praia", "praia", filters, emotion)
        append_memory("assistant", reply, "praia", "praia")
        return reply

    # Roteiro
    if inferred_intent == "roteiro":
        reply = get_roteiro_reply(guest)
        append_memory("user", text_raw, "roteiro", "roteiro", filters, emotion)
        append_memory("assistant", reply, "roteiro", "roteiro")
        return reply

    # Restaurantes
    if inferred_intent == "restaurantes":
        reply = get_restaurantes_reply(text_raw)
        append_memory("user", text_raw, "restaurantes", "restaurantes", filters, emotion)
        append_memory("assistant", reply, "restaurantes", "restaurantes")
        return reply

    # Mercado
    if inferred_intent == "mercado":
        reply = get_mercado_reply(text_raw)
        append_memory("user", text_raw, "mercado", "mercado", filters, emotion)
        append_memory("assistant", reply, "mercado", "mercado")
        return reply

    # Padaria
    if inferred_intent == "padaria":
        reply = get_padaria_reply()
        append_memory("user", text_raw, "padaria", "padaria", filters, emotion)
        append_memory("assistant", reply, "padaria", "padaria")
        return reply

    # Farmácia
    if inferred_intent == "farmacia":
        reply = get_farmacia_reply()
        append_memory("user", text_raw, "farmacia", "farmacia", filters, emotion)
        append_memory("assistant", reply, "farmacia", "farmacia")
        return reply

    # Garagem
    if has_any(text, ["garagem", "vaga", "estacionar", "estacionamento"]):
        reply = get_garagem_reply()
        append_memory("user", text_raw, "garagem", "garagem", filters, emotion)
        append_memory("assistant", reply, "garagem", "garagem")
        return reply

    # Checkout
    if has_any(text, ["checkout", "check-out", "check out"]):
        reply = get_checkout_reply(guest)
        append_memory("user", text_raw, "checkout", "checkout", filters, emotion)
        append_memory("assistant", reply, "checkout", "checkout")
        return reply

    # Bruno
    if has_any(text, ["bruno", "anfitriao", "anfitrião", "host"]):
        reply = get_bruno_reply()
        append_memory("user", text_raw, "bruno", "bruno", filters, emotion)
        append_memory("assistant", reply, "bruno", "bruno")
        return reply

    # Bares
    if inferred_intent == "bares":
        reply = get_bares_reply()
        append_memory("user", text_raw, "bares", "bares", filters, emotion)
        append_memory("assistant", reply, "bares", "bares")
        return reply

    # Shopping
    if inferred_intent == "shopping":
        reply = get_shopping_reply()
        append_memory("user", text_raw, "shopping", "shopping", filters, emotion)
        append_memory("assistant", reply, "shopping", "shopping")
        return reply

    # Feira
    if has_any(text, ["feira", "artesanato", "feirinha"]):
        reply = get_feira_reply()
        append_memory("user", text_raw, "feira", "feira", filters, emotion)
        append_memory("assistant", reply, "feira", "feira")
        return reply

    # Tempo
    if has_any(text, ["tempo", "clima", "previsao", "previsão", "meteorologia", "vai chover"]):
        reply = get_tempo_reply()
        append_memory("user", text_raw, "tempo", "tempo", filters, emotion)
        append_memory("assistant", reply, "tempo", "tempo")
        return reply

    # Passeios
    if inferred_intent == "passeio":
        reply = get_acqua_mundo_reply()
        append_memory("user", text_raw, "passeio", "passeio", filters, emotion)
        append_memory("assistant", reply, "passeio", "passeio")
        return reply

    # Eventos
    if inferred_intent == "eventos":
        reply = get_eventos_reply()
        append_memory("user", text_raw, "eventos", "eventos", filters, emotion)
        append_memory("assistant", reply, "eventos", "eventos")
        return reply

    # Surf
    if inferred_intent == "surf":
        reply = get_surf_reply()
        append_memory("user", text_raw, "surf", "surf", filters, emotion)
        append_memory("assistant", reply, "surf", "surf")
        return reply

    # Fallback
    reply = get_fallback_reply(guest)
    append_memory("user", text_raw, "fallback", "fallback", filters, emotion)
    append_memory("assistant", reply, "fallback", "fallback")
    return reply

# =========================
# ROTAS
# =========================

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

# =========================
# START
# =========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
