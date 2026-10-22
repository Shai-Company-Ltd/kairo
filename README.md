<div align="center">

# 🤖 Trading v4 — Agent IA de trading autonome

**Un moteur de trading crypto qui tourne 24/7 sur un VPS, exécute ses propres trades sur Bitunix et rend compte à une équipe d'agents IA.**

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Bitunix](https://img.shields.io/badge/Exchange-Bitunix%20Futures-F0B90B)
![Statut](https://img.shields.io/badge/statut-LIVE-brightgreen)
![Version](https://img.shields.io/badge/version-v4.1.1-blue)
![Licence](https://img.shields.io/badge/licence-propri%C3%A9taire-red)

</div>

---

## ✨ Le pitch

Trading v4 est un **agent de trading autonome** : un moteur Python unique analyse BTCUSDT sur six unités de temps, détecte des setups, ouvre et protège ses positions (SL et TP par trade), se répare tout seul quand l'exchange annule un ordre, et coupe tout si le compte passe sous un seuil critique.

Il ne bavarde pas : il envoie à une **équipe d'agents IA** (Scalpeur, Intraday, Swing, Risk, Journal) des récapitulatifs groupés toutes les 3 heures, ce qui garde la supervision intelligente tout en économisant les tokens. En parallèle, **Kronos**, un modèle de fondation pour séries financières, est observé en silence pour mesurer s'il mérite un jour de filtrer les entrées.

## 🏗️ Architecture

```mermaid
flowchart LR
    subgraph VPS["🖥️ VPS Linux (systemd)"]
        E["⚙️ Moteur Python<br/>engine.py"]
        DB[("🗄️ journal.db<br/>SQLite")]
        K["🔮 Kronos-mini<br/>observation (timer 15 min)"]
        E --> DB
    end
    B["🏦 API Bitunix Futures<br/>BTCUSDT x20 isolé"]
    E <-->|"klines · funding · ordres · TP/SL"| B
    E -->|"webhook : récap 3 h,<br/>urgences, digest"| T
    subgraph T["🧠 Équipe d'agents IA « bot trading »"]
        S["⚡ Scalpeur<br/>5m–15m"]
        I["📈 Intraday<br/>1h–4h"]
        W["🌊 Swing<br/>1d"]
        R["🛡️ Risk"]
        J["📓 Journal"]
    end
    K -. "prédictions (pas de trading)" .-> DB
```

| Composant | Rôle |
|---|---|
| `server/engine.py` | Boucle principale : données, indicateurs, signaux, exécution, protection, alertes |
| `server/trading-v4.service` | Unit systemd (redémarrage automatique) |
| `kronos/` | `observe.py` (prédictions), `stats.py` (taux de réussite), unit + timer systemd |
| `journal.db` | Signaux, trades, événements, alertes (non versionné) |

## 📐 Stratégie

- **Indicateurs** : EMA 20 / 50 / 200, RSI, ATR, VWAP, volume relatif, funding rate, ADX (v4.1).
- **Setups** :
  - `breakout` — cassure de niveau confirmée par le volume et l'ADX ;
  - `pullback_ema20` — retour sur l'EMA20 dans le sens de la tendance (EMA50/200) ;
  - `rsi_reversal` — retournement sur RSI extrême.
- **Timeframes** : 5m, 15m, 1h, 4h, 1d pour le trading ; le 1m reste en contexte.
- **Confiance** : chaque signal a un score ; trade au-dessus de `MIN_CONF_TRADE`, alerte au-dessus de `MIN_CONF_ALERT`.
- **Exécution** : entrée limite post-only au meilleur prix, annulée si non remplie en 20 s (jamais chassée) ; TP en limite, SL au marché.

## 🛡️ Gestion du risque

- Levier **x20 en marge isolée**.
- **1/4 du compte maximum** engagé en marge par trade (`MAX_MARGIN_PCT=25`), SL à `ATR × 1,5` (minimum 0,41 %), RR 2.
- **Kill switch** : equity ≤ `KILL_EQUITY` → fermeture de tout et arrêt (HALT).
- **SL et TP par trade** même sur position fusionnée, avec **réparation automatique** : TP/SL annulé par Bitunix → re-posé ; SL impossible à re-poser → fermeture au marché de la seule part du trade ; alerte urgente.
- Frais plafonnés en part du risque (`MAX_FEE_SHARE`).

## 💸 Économie de tokens

L'équipe d'agents n'est pas réveillée à chaque événement :
- **récap groupé toutes les 3 h** (ouvertures, clôtures, setups) ;
- urgences regroupées à la minute ; kill switch toujours immédiat ;
- **un digest par jour** (14:00 UTC+8) ;
- plafonds journaliers d'alertes et cooldowns.

## 🏷️ Versions

| Tag | Date (UTC+8) | Résumé |
|---|---|---|
| `v4.0` · `v4.0-paper` | 21/10/2026 | Premier moteur, paper par défaut |
| `v4.0-live` · `v4.0.1` | 21/10/2026 | Passage en réel, marge de sécurité frais |
| `v4.0.2` | 21/10/2026 | Fermeture partielle sur position fusionnée |
| `v4.0.3` | 22/10/2026 | Suivi des TP/SL en attente |
| `v4.0.4` | 22/10/2026 | Webhook fiable (retries, événements jamais plafonnés) |
| `v4.0-stable` | 22/10/2026 | TP/SL par trade + réparation — version de référence |
| `v4.1` | 23/10/2026 | Récaps 3 h, filtre ADX, entrées post-only, 1m retiré |
| `v4.1.1` | 23/10/2026 | Réparation automatique du SL — **en production** |

Changelog détaillé : [CHANGELOG.md](CHANGELOG.md).

## 📊 Résultats réels

Chiffres lus dans `journal.db` (mode LIVE, du 21/10/2026 03:00 au 23/10/2026 00:55 UTC+8), sans extrapolation :

| Timeframe | Trades clôturés | Gagnants | Perdants | PnL net (USDT) | Frais (USDT) |
|---|---:|---:|---:|---:|---:|
| 1m | 31 | 11 | 20 | −2,38 | 5,05 |
| 5m | 13 | 9 | 4 | +3,56 | 2,26 |
| 15m | 7 | 6 | 1 | +4,67 | 1,55 |
| 1h | 1 | 1 | 0 | +1,51 | 0,15 |
| **Total** | **52** | **27** | **25** | **+7,36** | **9,02** |

Par setup : `breakout` 36 trades (+3,75 USDT), `pullback_ema20` 18 trades (+3,61 USDT) (2 trades encore ouverts inclus dans ces comptes).
**Equity : 26,62 USDT au départ → environ 34 USDT** (34,79 au dernier heartbeat).

> Échantillon très court (≈ 2 jours) : ces chiffres ne préjugent en rien des performances futures. Le 1m, seul timeframe perdant, a été retiré du trading en v4.1.

## 🚀 Installation

```bash
sudo useradd -r -m -d /opt/trading-v4 trader
sudo -u trader git clone https://github.com/Dontbyshai/trading-v4.git /opt/trading-v4
cd /opt/trading-v4/server
sudo -u trader python3 -m venv venv && sudo -u trader venv/bin/pip install -r requirements.txt
sudo -u trader cp ../.env.example .env && sudo chmod 600 .env   # puis éditer
sudo cp trading-v4.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now trading-v4
journalctl -u trading-v4 -f
```

Kronos (optionnel, observation seule) : copier `kronos/` dans `/opt/kronos`, créer le venv, puis installer `kronos/systemd/kronos-observe.{service,timer}` et `systemctl enable --now kronos-observe.timer`.

## ⚙️ Configuration

Tout passe par `.env` (modèle : [`.env.example`](.env.example)). Points clés :

| Variable | Rôle |
|---|---|
| `DRY_RUN` / `LIVE` | Ordres réels seulement si `LIVE=1` **et** `DRY_RUN=0` |
| `LEVERAGE`, `MAX_MARGIN_PCT`, `KILL_EQUITY` | Levier, marge max par trade, seuil du kill switch |
| `TRADE_TIMEFRAMES` | Timeframes autorisés à trader |
| `BITUNIX_API_KEY/SECRET` | Clés **sans droit de retrait**, IP whitelistée |
| `WEBHOOK_URL/KEY` | Point d'entrée de l'équipe d'agents |
| `WEBHOOK_BATCH`, `BATCH_HOURS` | Récaps groupés |
| `ADX_FILTER`, `ENTRY_POST_ONLY`, `TP_LIMIT` | Nouveautés v4.1 (0 = désactivé) |

> ⚠️ Ne jamais committer `.env` ni `journal.db` (déjà dans `.gitignore`).

## ⏪ Retour arrière

```bash
cd /opt/trading-v4 && git fetch --tags
git checkout v4.0-stable -- server/engine.py    # ou n'importe quel tag
sudo systemctl restart trading-v4
```

Les nouveautés v4.1 se désactivent aussi une par une dans `.env`, sans changer de code.

## 🗺️ Roadmap

- [ ] **Copy trading Bitunix** (devenir lead trader)
- [ ] **Multi-paires** (ETH, SOL…)
- [ ] **Backtest** sur l'historique des klines avec les mêmes règles
- [ ] **Filtre Kronos** sur les entrées, si son taux de réussite observé le justifie

## ⚠️ Avertissement

Ce projet est expérimental. Le trading de contrats à terme avec effet de levier (x20) comporte un **risque élevé de perte totale du capital**. Rien ici n'est un conseil en investissement. Les résultats passés, sur un échantillon court, ne garantissent pas les résultats futurs. N'engagez que des sommes que vous pouvez perdre.

## 📄 Licence

© 2026 Dontbyshai — **Tous droits réservés.** Code propriétaire : toute copie, modification ou redistribution est interdite sans autorisation écrite.
