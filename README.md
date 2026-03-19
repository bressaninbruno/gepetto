# Gepetto MVP

Versão alfa funcional do Gepetto, concierge/assistente de estadia.

## O que vem pronto
- Chat web simples
- Backend Flask
- Integração com OpenAI
- Base de conhecimento em JSON
- Fallback local se a API key ainda não estiver configurada

## Como rodar localmente
1. Instale Python 3.10+
2. No terminal, entre nesta pasta
3. Instale dependências:
   ```bash
   pip install -r requirements.txt
   ```
4. Defina a chave da OpenAI:
   macOS/Linux:
   ```bash
   export OPENAI_API_KEY="sua_chave_aqui"
   ```
   Windows PowerShell:
   ```powershell
   setx OPENAI_API_KEY "sua_chave_aqui"
   ```
5. Rode:
   ```bash
   python server.py
   ```
6. Abra:
   http://localhost:5000

## Como subir no Render
- Build command:
  `pip install -r requirements.txt`
- Start command:
  `python server.py`
- Environment variable:
  `OPENAI_API_KEY=sua_chave`

## Onde editar
- `knowledge_base.json` -> informações do apê e região
- `system_prompt.txt` -> personalidade e regras do Gepetto
- `static/index.html` -> visual da interface

## Observações
- O plano gratuito do Render pode "dormir" após inatividade.
- Para QR code, depois de publicar, gere um QR com a URL pública.
