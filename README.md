# BTC Zones

Outil d'inférence des zones d'entrée probables des institutionnels sur BTCUSD.
Flux : Binance (spot+perp), Coinbase, Bybit. Zones scorées par confluence de 6 familles de signaux.

## Déploiement
1. Push ce dossier sur GitHub.
2. Render > New > Blueprint > choisis le repo (render.yaml détecté). Région Frankfurt.
3. Optionnel : variables TG_TOKEN et TG_CHAT pour les alertes Telegram.

Lancer en local : `pip install -r requirements.txt && uvicorn app.main:app --reload`
Il faut ~60 min de flux pour que absorption/profil soient fiables. Les zones n'ont pas encore été backtestées.
