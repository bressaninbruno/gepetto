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

# Telegram: deixe vazio por enquanto.
# Depois você me passa BOT TOKEN e CHAT_ID e eu te digo como ativar.
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
    return {
        "messages": []
    }

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

def append_memory(role, text, topic=""):
    memory = load_memory()
    memory["messages"].append({
        "role": role,
        "text": text,
        "topic": topic,
        "timestamp": datetime.now().isoformat(timespec="seconds")
    })
    memory["messages"] = memory["messages"][-20:]
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
    text = text.lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text

def has_any(text: str, terms) -> bool:
    return any(term in text for term in terms)

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
        "Eu sou o Gepetto, seu concierge pessoal durante a estadia.\n\n"
        "Posso te ajudar com:\n"
        "• **Guia do apartamento e do condomínio**\n"
        "• **Recomendações de restaurantes**\n"
        "• **Mercados e conveniências**\n"
        "• **Praia, passeios e dicas locais**\n\n"
        "Fique à vontade para me chamar a qualquer momento 😉"
    )

def get_last_topic():
    memory = load_memory()
    for item in reversed(memory.get("messages", [])):
        topic = item.get("topic", "")
        if topic:
            return topic
    return ""

def current_time_label():
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "manhã"
    if 12 <= hour < 18:
        return "tarde"
    return "noite"

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

def classify_incident(text):
    text_n = normalize_text(text)

    high = [
        "vazamento", "sem energia", "porta nao abre", "porta não abre",
        "nao entra", "não entra", "curto", "fogo", "gas", "gás",
        "queimou", "queimado"
    ]
    medium = [
        "chuveiro", "ar nao funciona", "ar não funciona", "tv nao liga", "tv não liga",
        "wifi nao funciona", "wifi não funciona", "internet nao funciona", "internet não funciona",
        "parou de funcionar", "quebrou", "defeito", "nao funciona", "não funciona",
        "queimou", "queimado"
    ]

    if has_any(text_n, high):
        return "alta"
    if has_any(text_n, medium):
        return "media"
    return "baixa"

def maybe_notify_incident(raw_message, guest):
    severity = classify_incident(raw_message)
    if severity not in ["alta", "media"]:
        return

    payload = {
        "tipo": "incidente",
        "gravidade": severity,
        "mensagem": raw_message,
        "hospede": guest.get("nome", ""),
        "grupo": guest.get("grupo", ""),
        "checkout": guest.get("checkout", ""),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "status": "aberto"
    }
    append_incident(payload)

    label_guest = guest.get("nome") or "Hóspede sem nome definido"
    tg_msg = (
        f"🚨 INCIDENTE NO APTO 14B\n\n"
        f"Gravidade: {severity.upper()}\n"
        f"Hóspede: {label_guest}\n"
        f"Mensagem: {raw_message}\n"
        f"Horário: {payload['timestamp']}"
    )
    send_telegram_message(tg_msg)

def get_wifi_reply():
    return (
        "Claro 😊\n\n"
        "📶 Usuário: **Volare Hal**\n"
        "🔑 Senha: **Guaruja123@**"
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
            "Se quiser, posso te sugerir o que faz mais sentido conforme o momento do dia 😉"
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

def get_acqua_mundo_reply():
    acqua = KNOWLEDGE.get("proximidades", {}).get("acqua_mundo", {})
    return (
        f"Uma boa opção por aqui é o **{acqua.get('nome', 'Acqua Mundo')}** 😊\n\n"
        f"{acqua.get('observacao', 'Costuma funcionar muito bem para famílias e também em dias de chuva.')}\n\n"
        "Se quiser, posso te sugerir esse tipo de passeio quando o tempo não estiver tão bom."
    )

def get_eventos_reply():
    return (
        "O Guarujá costuma ter eventos, feiras e programações pontuais dependendo da época 😊\n\n"
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

def get_problem_reply():
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

def get_followup_reply(text, last_topic, guest):
    text_n = normalize_text(text)

    if last_topic == "restaurantes":
        if has_any(text_n, ["mais perto", "perto"]):
            return (
                "Se a prioridade for praticidade, eu focaria no que te faça sair e voltar com mais conforto.\n\n"
                "Se quiser, eu sigo numa linha mais **prática**, mais **especial** ou mais **tradicional** 😉"
            )
        if has_any(text_n, ["mais barato", "barato", "economico", "econômico"]):
            return get_restaurantes_reply("barato")
        if has_any(text_n, ["mais especial", "especial", "romantico", "romântico", "sofisticado"]):
            return get_restaurantes_reply("especial")

    if last_topic == "praia":
        if has_any(text_n, ["mais tarde", "agora", "hoje"]):
            return (
                "Se for ainda hoje, eu aproveitaria enquanto o serviço está funcionando e já deixaria o fim do dia mais leve 😉"
            )

    if last_topic == "mercado":
        if has_any(text_n, ["mais completo", "completo", "grande", "variedade"]):
            return get_mercado_reply("completo")
        if has_any(text_n, ["mais perto", "perto", "rapido", "rápido"]):
            return get_mercado_reply("rapido")

    return ""

def get_fallback_reply(guest):
    base = [
        "Posso te ajudar com **praia**, **restaurantes**, **mercado**, **regras da casa** ou qualquer dúvida do apartamento 😉",
        "Se quiser, posso te orientar agora sobre **praia**, **mercado**, **restaurantes** ou **garagem** 👍",
        "Me conta melhor o que você precisa 😄 Posso te ajudar com a parte do apartamento ou com dicas da região."
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

    # Admin
    if text_raw.startswith("/"):
        admin_reply = handle_admin_command(text_raw)
        if admin_reply is not None:
            append_memory("user", text_raw, "admin")
            append_memory("assistant", admin_reply, "admin")
            return admin_reply

    # Follow-up contextual
    last_topic = get_last_topic()
    followup = get_followup_reply(text_raw, last_topic, guest)
    if followup:
        append_memory("user", text_raw, last_topic)
        append_memory("assistant", followup, last_topic)
        return followup

    # Saudações
    if has_any(text, ["oi", "ola", "cheguei", "chegamos", "boa tarde", "bom dia", "boa noite"]):
        reply = (
            f"{saudacao_personalizada(guest)}\n\n"
            "Que bom que você chegou!\n\n"
            f"{proactive_prompt(guest)}"
        )
        append_memory("user", text_raw, "saudacao")
        append_memory("assistant", reply, "saudacao")
        return reply

    # Wi-Fi
    if has_any(text, ["wifi", "wi-fi", "internet"]):
        reply = get_wifi_reply()
        append_memory("user", text_raw, "wifi")
        append_memory("assistant", reply, "wifi")
        return reply

    # Regras / condomínio / casa
    if has_any(text, ["regra", "regras", "casa", "condominio", "silencio", "lixo", "areia"]):
        reply = get_regras_reply()
        append_memory("user", text_raw, "regras")
        append_memory("assistant", reply, "regras")
        return reply

    # Onde fica o serviço de praia (antes da resposta genérica)
    if "onde fica" in text and "praia" in text:
        reply = get_servico_praia_localizacao_reply()
        append_memory("user", text_raw, "praia")
        append_memory("assistant", reply, "praia")
        return reply

    # Praia
    if has_any(text, ["praia", "servico de praia", "serviço de praia", "guarda-sol", "guarda sol", "cadeira de praia"]):
        reply = get_praia_reply()
        append_memory("user", text_raw, "praia")
        append_memory("assistant", reply, "praia")
        return reply

    # Roteiro
    if has_any(text, ["roteiro", "o que fazer hoje", "o que fazer", "plano pro dia", "sugestao de roteiro", "sugestão de roteiro"]):
        reply = get_roteiro_reply(guest)
        append_memory("user", text_raw, "roteiro")
        append_memory("assistant", reply, "roteiro")
        return reply

    # Restaurantes / comida
    if has_any(text, ["restaurante", "jantar", "almoco", "almoço", "comer", "comida", "onde jantar", "onde comer", "sushi", "japones", "japonesa", "lanche", "hamburguer", "hambúrguer", "chocolate", "sobremesa", "kopenhagen", "mcdonald", "mcdonald's"]):
        reply = get_restaurantes_reply(text_raw)
        append_memory("user", text_raw, "restaurantes")
        append_memory("assistant", reply, "restaurantes")
        return reply

    # Mercado / supermercado / compras
    if has_any(text, ["mercado", "supermercado", "compras", "dia", "pao de acucar", "pão de açúcar", "carrefour", "extra"]):
        reply = get_mercado_reply(text_raw)
        append_memory("user", text_raw, "mercado")
        append_memory("assistant", reply, "mercado")
        return reply

    # Padaria / café
    if has_any(text, ["padaria", "cafe da manha", "café da manhã", "cafe"]):
        reply = get_padaria_reply()
        append_memory("user", text_raw, "padaria")
        append_memory("assistant", reply, "padaria")
        return reply

    # Farmácia / remédio
    if has_any(text, ["farmacia", "farmácia", "remedio", "remédio", "dor de cabeca", "dor de cabeça"]):
        reply = get_farmacia_reply()
        append_memory("user", text_raw, "farmacia")
        append_memory("assistant", reply, "farmacia")
        return reply

    # Garagem / vaga
    if has_any(text, ["garagem", "vaga", "estacionar", "estacionamento"]):
        reply = get_garagem_reply()
        append_memory("user", text_raw, "garagem")
        append_memory("assistant", reply, "garagem")
        return reply

    # Checkout
    if has_any(text, ["checkout", "check-out", "check out"]):
        reply = get_checkout_reply(guest)
        append_memory("user", text_raw, "checkout")
        append_memory("assistant", reply, "checkout")
        return reply

    # Bruno / anfitrião
    if has_any(text, ["bruno", "anfitriao", "anfitrião", "host"]):
        reply = get_bruno_reply()
        append_memory("user", text_raw, "bruno")
        append_memory("assistant", reply, "bruno")
        return reply

    # Bares / pubs / noite
    if has_any(text, ["bar", "bares", "pub", "cerveja", "noite", "beber", "drink", "drinks"]):
        reply = get_bares_reply()
        append_memory("user", text_raw, "bares")
        append_memory("assistant", reply, "bares")
        return reply

    # Shopping
    if has_any(text, ["shopping", "la plage"]):
        reply = get_shopping_reply()
        append_memory("user", text_raw, "shopping")
        append_memory("assistant", reply, "shopping")
        return reply

    # Feira / artesanato
    if has_any(text, ["feira", "artesanato", "feirinha"]):
        reply = get_feira_reply()
        append_memory("user", text_raw, "feira")
        append_memory("assistant", reply, "feira")
        return reply

    # Tempo / previsão
    if has_any(text, ["tempo", "clima", "previsao", "previsão", "meteorologia", "vai chover"]):
        reply = get_tempo_reply()
        append_memory("user", text_raw, "tempo")
        append_memory("assistant", reply, "tempo")
        return reply

    # Problemas
    if has_any(text, [
        "nao funciona", "não funciona", "nao esta funcionando", "não está funcionando",
        "quebrou", "problema", "defeito", "parou de funcionar", "chuveiro", "tv nao liga",
        "tv não liga", "ar nao funciona", "ar não funciona", "wifi nao funciona",
        "wifi não funciona", "internet nao funciona", "internet não funciona",
        "vazamento", "sem energia", "porta nao abre", "porta não abre",
        "nao entra", "não entra", "queimou", "queimado"
    ]):
        reply = get_problem_reply()
        maybe_notify_incident(text_raw, guest)
        append_memory("user", text_raw, "incidente")
        append_memory("assistant", reply, "incidente")
        return reply

    # Saúde / emergência
    if has_any(text, ["hospital", "upa", "emergencia", "emergência", "medico", "médico", "saude", "saúde"]):
        reply = get_emergencia_reply()
        append_memory("user", text_raw, "emergencia")
        append_memory("assistant", reply, "emergencia")
        return reply

    # Passeios / chuva / crianças
    if has_any(text, ["crianca", "criança", "chuva", "o que fazer", "passeio", "passeios", "aquario", "aquário", "acqua mundo"]):
        reply = get_acqua_mundo_reply()
        append_memory("user", text_raw, "passeio")
        append_memory("assistant", reply, "passeio")
        return reply

    # Eventos
    if has_any(text, ["evento", "eventos", "show", "shows", "festa", "feira"]):
        reply = get_eventos_reply()
        append_memory("user", text_raw, "eventos")
        append_memory("assistant", reply, "eventos")
        return reply

    # Surf
    if has_any(text, ["surf", "ondas", "mar", "pico de surf"]):
        reply = get_surf_reply()
        append_memory("user", text_raw, "surf")
        append_memory("assistant", reply, "surf")
        return reply

    # Fallback
    reply = get_fallback_reply(guest)
    append_memory("user", text_raw, "fallback")
    append_memory("assistant", reply, "fallback")
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
