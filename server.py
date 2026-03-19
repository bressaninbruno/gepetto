import os
import json
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory
from openai import OpenAI

APP_DIR = Path(__file__).parent
app = Flask(__name__, static_folder="static", static_url_path="/static")

KB_PATH = APP_DIR / "knowledge_base.json"
PROMPT_PATH = APP_DIR / "system_prompt.txt"

def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")

def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

KNOWLEDGE = load_json(KB_PATH)
SYSTEM_PROMPT = load_text(PROMPT_PATH)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

def build_system_prompt() -> str:
    kb_text = json.dumps(KNOWLEDGE, ensure_ascii=False, indent=2)
    return f"{SYSTEM_PROMPT}\n\nBASE DE CONHECIMENTO:\n{kb_text}"

def format_fallback(user_message: str) -> str:
    lower = user_message.lower()

    if any(k in lower for k in ["wifi", "wi-fi", "internet"]):
        wifi = KNOWLEDGE["apartamento"]["wifi"]
        return f"Claro 😊\nRede: {wifi['nome']}\nSenha: {wifi['senha']}"

    if any(k in lower for k in ["praia", "beach", "guarda-sol", "cadeira"]):
        praia = KNOWLEDGE["praia"]
        return (
            "Claro 😊\n"
            f"A praia fica a cerca de {praia['distancia']} do apartamento.\n"
            f"O serviço de praia funciona das {praia['servico_praia']['horario']} e fica {praia['servico_praia']['localizacao']}.\n"
            f"{praia['servico_praia']['como_funciona']}"
        )

    if any(k in lower for k in ["garagem", "vaga", "parking"]):
        return KNOWLEDGE["garagem"]["info"]

    if any(k in lower for k in ["silêncio", "silencio", "quiet", "barulho", "noise"]):
        return f"O condomínio pede silêncio das {KNOWLEDGE['regras']['silencio']} 😊"

    if any(k in lower for k in ["mercado", "supermercado", "dia"]):
        mercado = KNOWLEDGE["proximidades"]["mercado_dia"]
        return (
            f"Tem um supermercado Dia praticamente ao lado 😊\n"
            f"Dá para ir a pé em {mercado['tempo_a_pe']}.\n"
            "Ótimo para compras rápidas do dia a dia."
        )

    if any(k in lower for k in ["bruno", "host", "anfitrião", "anfitriao"]):
        return (
            "Se preferir falar diretamente com o Bruno, posso acioná-lo rapidamente 😊\n"
            "Ele será notificado e entrará em contato com você pelo Airbnb assim que possível."
        )

    return (
        "Olá! Eu sou o Gepetto 😊\n"
        "Ainda estou na versão alfa, mas já posso ajudar com Wi‑Fi, praia, garagem, regras, "
        "mercados, restaurantes e dúvidas gerais sobre a estadia.\n"
        "Se quiser, me diga o que você precisa 👍"
    )

@app.route("/")
def index():
    return send_from_directory(APP_DIR / "static", "index.html")

@app.route("/health")
def health():
    return {"status": "ok", "api_configured": bool(OPENAI_API_KEY)}

@app.route("/chat", methods=["POST"])
def chat():
    payload = request.get_json(force=True) or {}
    message = (payload.get("message") or "").strip()
    history = payload.get("history") or []

    if not message:
        return jsonify({"reply": "Pode me mandar sua dúvida 😊"}), 400

    if not OPENAI_API_KEY:
        return jsonify({"reply": format_fallback(message)})

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)

        messages = [{"role": "system", "content": build_system_prompt()}]

        # Últimas 6 mensagens para manter contexto curto
        for item in history[-6:]:
            role = item.get("role")
            content = item.get("content", "")
            if role in {"user", "assistant"} and content:
                messages.append({"role": role, "content": content})

        messages.append({"role": "user", "content": message})

        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=messages,
            temperature=0.5,
        )

        reply = response.choices[0].message.content.strip()
        return jsonify({"reply": reply})

    except Exception as e:
        return jsonify({
            "reply": (
                "Tive uma instabilidade rápida por aqui 😕\n"
                "Se quiser, tente novamente em alguns segundos ou peça para eu acionar o Bruno."
            ),
            "debug_error": str(e)
        }), 200

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
