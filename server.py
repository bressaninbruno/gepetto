# 🔥 IMPORTANTE: adicionado Response
from flask import Flask, request, jsonify, send_from_directory, Response
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

# =========================
# FUNÇÕES UTIL
# =========================

def normalize_text(text: str) -> str:
    text = (text or "").lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text

def has_any(text: str, terms) -> bool:
    base = normalize_text(text)
    return any(normalize_text(term) in base for term in terms)

# =========================
# TEXTOS CORRIGIDOS
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
    return (
        "🌴 Bem-vindo à praia da Enseada!\n\n"
        "É uma honra ter você hospedado aqui 😊\n\n"
        "Eu sou o Gepetto, seu concierge pessoal.\n\n"
        "Posso te ajudar com praia, restaurantes, mercado e muito mais 😉"
    )

# =========================
# CORE (mantido simples para estabilidade)
# =========================

def gepetto_responde(msg):
    text = normalize_text(msg)

    if has_any(text, ["oi", "ola", "olá"]):
        return "Olá 😊 Como posso te ajudar?"

    if "wifi" in text:
        return "📶 Usuário: Volare Hal\n🔑 Senha: Guaruja123@"

    if "praia" in text:
        return "A praia fica a poucos minutos a pé 🏖️"

    if "mercado" in text:
        return "• Dia (ao lado)\n• Pão de Açúcar (3 min)\n• Carrefour e Extra (5 min)"

    if "restaurante" in text:
        return "• Alcides\n• Thai Lounge\n• Sushi Katoshi\n• McDonald's"

    return "Posso te ajudar com praia, mercado, restaurantes ou dúvidas do apartamento 😉"

# =========================
# ROTAS (CORRIGIDAS UTF-8)
# =========================

@app.route("/")
def home():
    return send_from_directory("static", "index.html")

@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json() or {}
    msg = data.get("message", "")
    resposta = gepetto_responde(msg)

    return Response(
        json.dumps({"reply": resposta}, ensure_ascii=False),
        content_type="application/json; charset=utf-8"
    )

@app.route("/welcome", methods=["GET"])
def welcome():
    return Response(
        json.dumps({"message": mensagem_boas_vindas()}, ensure_ascii=False),
        content_type="application/json; charset=utf-8"
    )

# =========================
# START
# =========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
