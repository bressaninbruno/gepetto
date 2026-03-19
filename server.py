from flask import Flask, request, jsonify, send_from_directory
import os, json

app = Flask(__name__, static_folder="static", static_url_path="/static")

with open("knowledge_base.json", encoding="utf-8") as f:
    KNOWLEDGE = json.load(f)

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json()
    message = data.get("message","").lower()

    if "wifi" in message:
        return jsonify({"reply": f"Rede: {KNOWLEDGE['apartamento']['wifi']['nome']} | Senha: {KNOWLEDGE['apartamento']['wifi']['senha']}"})

    return jsonify({"reply": "Sou o Gepetto 😄 Me pergunte algo!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
