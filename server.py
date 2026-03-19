from flask import Flask, request, jsonify, send_from_directory
import os
import json
from pathlib import Path

app = Flask(__name__, static_folder="static", static_url_path="/static")

BASE_DIR = Path(__file__).parent
KNOWLEDGE_FILE = BASE_DIR / "knowledge_base.json"
GUEST_FILE = BASE_DIR / "current_guest.json"

ADMIN_PIN = "1234"  # <-- TROQUE AQUI PELO SEU PIN

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
# FUNÇÕES DE SAUDAÇÃO
# =========================

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
        "Poderei te ajudar em qualquer ponto da sua viagem, como:\n"
        "• Guia do apartamento e do condomínio\n"
        "• Recomendações de restaurantes\n"
        "• Supermercados e conveniências\n"
        "• Praia, passeios e dicas locais\n\n"
        "Fique à vontade para me chamar a qualquer momento 😉"
    )

# =========================
# MODO ADMIN
# =========================

def handle_admin_command(message):
    global ADMIN_UNLOCKED

    parts = message.strip().split(" ", 2)
    cmd = parts[0].lower()

    # /admin 1234
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
    text = msg.lower()
    guest = load_guest()

    # Admin commands
    if text.startswith("/"):
        admin_reply = handle_admin_command(msg)
        if admin_reply is not None:
            return admin_reply

    # Saudações
    if "oi" in text or "olá" in text or "ola" in text:
        return (
            f"{saudacao_personalizada(guest)}\n\n"
            "Que bom que você chegou!\n\n"
            "Se quiser, posso te ajudar a organizar seu dia — praia, restaurante, mercado ou qualquer dúvida do apartamento 😄"
        )

    # Wi-Fi
    if "wifi" in text or "wi-fi" in text or "internet" in text:
        wifi = KNOWLEDGE.get("apartamento", {}).get("wifi", {})
        return (
            "Claro 😊\n\n"
            f"Rede: {wifi.get('nome', 'WIFI_NAME')}\n"
            f"Senha: {wifi.get('senha', 'WIFI_PASSWORD')}"
        )

    # Regras da casa
    if "regra" in text or "regras" in text or "casa" in text or "condomínio" in text or "condominio" in text:
        regras = KNOWLEDGE.get("regras", {})
        return (
            "Claro 😊\n\n"
            "Algumas informações importantes:\n"
            f"• Silêncio: {regras.get('silencio', '23h às 7h')}\n"
            f"• Areia: {regras.get('areia', 'usar lava-pés antes de entrar no elevador')}\n"
            f"• Lixo: {regras.get('lixo', 'há ponto de descarte no térreo')}"
        )

    # Praia
    if "praia" in text or "serviço de praia" in text or "servico de praia" in text:
        praia = KNOWLEDGE.get("praia", {})
        servico = praia.get("servico_praia", {})
        return (
            "Boa escolha 😄\n\n"
            f"A praia fica a {praia.get('distancia', 'poucos minutos a pé')}.\n"
            f"O serviço de praia funciona das {servico.get('horario', '9h às 17h')}.\n"
            f"Ele fica {servico.get('localizacao', 'ao lado do Casa Grande Hotel')}.\n\n"
            f"{servico.get('como_funciona', 'Os itens já ficam montados na areia.')}"
        )

    # Onde fica o serviço de praia
    if "onde fica" in text and "praia" in text:
        praia = KNOWLEDGE.get("praia", {})
        servico = praia.get("servico_praia", {})
        return (
            "Claro 😊\n\n"
            f"O serviço de praia fica {servico.get('localizacao', 'ao lado do Casa Grande Hotel')}."
        )

    # Restaurantes
    if "restaurante" in text or "jantar" in text or "almoço" in text or "almoco" in text:
        return (
            "Boa 😄\n\n"
            "Se quiser algo tradicional, recomendo o <strong>Restaurante Alcides</strong>.\n\n"
            "Se a ideia for algo mais especial à noite, o <strong>Thai Lounge</strong> tem uma vibe incrível ✨\n\n"
            "Se quiser, eu posso te sugerir o melhor conforme o estilo do passeio."
        )

    # Mercado
    if "mercado" in text or "supermercado" in text:
        mercado = KNOWLEDGE.get("proximidades", {}).get("mercado_dia", {})
        return (
            "Tem um mercado praticamente ao lado 😊\n\n"
            f"Dá para ir a pé em {mercado.get('tempo_a_pe', 'menos de 1 minuto')}.\n"
            "É ótimo para compras rápidas."
        )

    # Garagem
    if "garagem" in text or "vaga" in text or "estacionar" in text:
        info = KNOWLEDGE.get("garagem", {}).get("info", "")
        return f"{info}"

    # Checkout
    if "checkout" in text or "check-out" in text:
        checkout = guest.get("checkout") or KNOWLEDGE.get("apartamento", {}).get("checkout", "CHECKOUT_HORARIO")
        return f"O check-out está configurado para: {checkout} 😊"

    # Bruno
    if "bruno" in text or "anfitrião" in text or "anfitriao" in text:
        return (
            "Se preferir falar diretamente com o Bruno, posso avisá-lo rapidamente 😊\n\n"
            "Ele receberá uma notificação e poderá entrar em contato com você pelo Airbnb."
        )

    # Fallback
    return (
        "Perfeito 😄\n\n"
        "Posso te ajudar com:\n"
        "• Praia\n"
        "• Restaurantes\n"
        "• Mercados\n"
        "• Regras da casa\n"
        "• Dúvidas do apartamento\n\n"
        "O que você quer agora?"
    )

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
