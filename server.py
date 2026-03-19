from flask import Flask, request, jsonify, send_from_directory
import os

app = Flask(__name__, static_folder="static", static_url_path="/static")

def gepetto_responde(msg):
    msg = msg.lower()

    if "oi" in msg or "olá" in msg:
        return """Olá 😊 que bom que você chegou!

Fez uma boa viagem até o Guarujá?

Se quiser, posso te ajudar a organizar o seu dia — praia, restaurante ou algo mais tranquilo 😄"""

    if "wifi" in msg:
        return """Claro 😊

Rede: WIFI_NAME  
Senha: WIFI_PASSWORD"""

    if "praia" in msg:
        return """Boa escolha 😄

A praia fica a poucos minutos a pé.

O serviço de praia funciona das 9h às 17h e já deixa tudo montado pra você — guarda-sol e cadeiras ⛱️

Se quiser, te indico o melhor horário pra pegar a praia mais vazia 😉"""

    if "restaurante" in msg or "jantar" in msg:
        return """Boa 😄

Se quiser algo mais descontraído, o Alcides é bem tradicional e perto.

Agora, se quiser uma experiência mais especial, o Thai Lounge tem uma vibe incrível à noite ✨

Quer que eu te diga o melhor horário pra ir sem fila?"""

    if "mercado" in msg:
        return """Tem um mercado praticamente ao lado 😊

Menos de 1 minuto andando — perfeito pra qualquer coisa rápida."""

    return """Perfeito 😄

Posso te ajudar com:
🏖️ praia  
🍽️ restaurantes  
🛒 mercados  
🏢 dúvidas do apartamento  

O que você quer fazer agora?"""

@app.route("/")
def home():
    return send_from_directory("static", "index.html")

@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json()
    msg = data.get("message", "")
    resposta = gepetto_responde(msg)
    return jsonify({"reply": resposta})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
