from flask import Flask, request, jsonify, send_from_directory
import os
import json
import random
import unicodedata
from pathlib import Path

app = Flask(__name__, static_folder="static", static_url_path="/static")

BASE_DIR = Path(__file__).parent
KNOWLEDGE_FILE = BASE_DIR / "knowledge_base.json"
GUEST_FILE = BASE_DIR / "current_guest.json"

ADMIN_PIN = "2710"

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
            "Se a ideia for algo mais simples ou prático, eu começaria por uma opção rápida por aqui e deixaria algo mais especial para a noite.\n\n"
            "Se quiser, também posso te sugerir o que faz mais sentido conforme o momento do dia 😉"
        )

    if has_any(text_n, ["especial", "romantico", "sofisticado", "premium"]):
        return (
            "Boa 😄\n\n"
            "Se quiser algo mais especial, o **Thai Lounge** costuma ser uma ótima pedida ✨\n\n"
            "Se preferir algo mais clássico e tradicional, o **Restaurante Alcides** também é uma excelente escolha."
        )

    return (
        "Boa 😄\n\n"
        "Aqui vão algumas boas referências por perto:\n\n"
        "• **Restaurante Alcides** → clássico, tradicional e muito lembrado no Guarujá 🦐\n"
        "• **Thai Lounge** → vibe mais especial e experiência mais sofisticada ✨\n"
        "• **Casa Grande Hotel** → boa referência para opções mais estruturadas na região\n\n"
        "Se quiser, eu também posso te sugerir uma opção mais **rápida**, mais **especial** ou mais **tradicional** 😉"
    )

def get_mercado_reply(text):
    proximidades = KNOWLEDGE.get("proximidades", {})
    mercado = proximidades.get("mercado_dia", {})

    return (
        "Perfeito 😊\n\n"
        "Pra compra rápida, o **Supermercado Dia** fica praticamente ao lado.\n"
        f"Dá para ir a pé em {mercado.get('tempo_a_pe', 'menos de 1 minuto')}.\n\n"
        "Ele é ótimo para:\n"
        "• água e bebidas\n"
        "• itens do dia a dia\n"
        "• compras rápidas sem precisar pegar carro\n\n"
        "Se quiser, também posso te sugerir o que vale mais a pena comprar logo na chegada 👍"
    )

def get_padaria_reply():
    return (
        "Posso te orientar para padarias e opções de café da manhã próximas 😊\n\n"
        "Se quiser, me diga se você quer algo mais rápido, mais caprichado ou mais perto do apartamento."
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
            return admin_reply

    # Saudações
    if has_any(text, ["oi", "ola", "cheguei", "chegamos", "boa tarde", "bom dia", "boa noite"]):
        return (
            f"{saudacao_personalizada(guest)}\n\n"
            "Que bom que você chegou!\n\n"
            f"{proactive_prompt(guest)}"
        )

    # Wi-Fi
    if has_any(text, ["wifi", "wi-fi", "internet"]):
        return get_wifi_reply()

    # Regras / condomínio / casa
    if has_any(text, ["regra", "regras", "casa", "condominio", "condominio", "silencio", "lixo", "areia"]):
        return get_regras_reply()

    # Praia / serviço de praia
    if has_any(text, ["praia", "servico de praia", "serviço de praia", "guarda-sol", "guarda sol", "cadeira de praia"]):
        return get_praia_reply()

    # Onde fica o serviço
    if "onde fica" in text and "praia" in text:
        return get_servico_praia_localizacao_reply()

    # Restaurantes / comida
    if has_any(text, ["restaurante", "jantar", "almoco", "almoço", "comer", "comida", "onde jantar", "onde comer"]):
        return get_restaurantes_reply(text_raw)

    # Mercado / supermercado / compras
    if has_any(text, ["mercado", "supermercado", "compras", "dia"]):
        return get_mercado_reply(text_raw)

    # Padaria / café
    if has_any(text, ["padaria", "cafe da manha", "café da manhã", "cafe"]):
        return get_padaria_reply()

    # Farmácia / remédio
    if has_any(text, ["farmacia", "farmácia", "remedio", "remédio", "dor de cabeca", "dor de cabeça"]):
        return get_farmacia_reply()

    # Garagem / vaga
    if has_any(text, ["garagem", "vaga", "estacionar", "estacionamento"]):
        return get_garagem_reply()

    # Checkout
    if has_any(text, ["checkout", "check-out", "check out"]):
        return get_checkout_reply(guest)

    # Bruno / anfitrião
    if has_any(text, ["bruno", "anfitriao", "anfitrião", "host"]):
        return get_bruno_reply()

    # Problemas
    if has_any(text, [
        "nao funciona", "não funciona", "nao esta funcionando", "não está funcionando",
        "quebrou", "problema", "defeito", "parou de funcionar", "chuveiro", "tv nao liga",
        "tv não liga", "ar nao funciona", "ar não funciona"
    ]):
        return get_problem_reply()

    # Saúde / emergência
    if has_any(text, ["hospital", "upa", "emergencia", "emergência", "medico", "médico", "saude", "saúde"]):
        return get_emergencia_reply()

    # Passeios / criança / chuva
    if has_any(text, ["crianca", "criança", "chuva", "o que fazer", "passeio", "passeios", "aquario", "aquário", "acqua mundo"]):
        return get_acqua_mundo_reply()

    # Eventos
    if has_any(text, ["evento", "eventos", "show", "shows", "festa", "feira"]):
        return get_eventos_reply()

    # Surf
    if has_any(text, ["surf", "ondas", "mar", "pico de surf"]):
        return get_surf_reply()

    # Fallback
    return get_fallback_reply(guest)

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
