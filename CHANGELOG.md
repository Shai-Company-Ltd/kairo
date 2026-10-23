# Changelog — Trading v4

Format inspiré de [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/). Dates en UTC+8.

## [v4.1.2] — 2026-10-23
### Ajouté
- `review.py` : revue de performance 3 jours sans tokens (lecture seule), rapport Markdown FR avec verdicts par setup×TF et recommandations.
- Timer systemd `trading-v4-review` tous les 3 jours à 05:30 UTC (13:30 UTC+8), Nice=10, MemoryMax=200M.

## [v4.1.1] — 2026-10-23
### Corrigé
- **Réparation automatique du SL** : `fix_missing_sl` détecte un trade sans stop-loss, le re-pose, et ferme au marché la seule quantité du trade si le SL n'est plus posable (prix au-delà du SL ou 2 échecs) ; alerte urgente `no_sl`.
- `repose_tp` : re-pose du TP manquant ; suppression des TP orphelins qui agiraient sur la quantité des autres trades.
- `_tpsl_canceled` / `_own_sl_filled` : distinction fiable entre un TP/SL annulé par Bitunix (`SYSTEM_CANCEL`) et un TP/SL exécuté ; en cas d'historique illisible, on ne déclare jamais un trade fermé.

## [v4.1] — 2026-10-23
### Ajouté
- **Récaps groupés toutes les 3 h** (`WEBHOOK_BATCH`, `BATCH_HOURS=3`) : ouvertures, clôtures et setups mis en tampon → un seul message à l'équipe d'agents (économie de tokens).
- Urgences coalescées à la minute (`URGENT_COALESCE`), kill switch toujours immédiat ; alerte de drawdown du jour (`URGENT_DD_PCT`).
- Digest quotidien unique en texte formaté (`DIGEST_TEXT`, 06:00 UTC = 14:00 UTC+8).
- **Filtre ADX** (`ADX_FILTER`, `ADX_MIN=20`) pour breakout et pullback.
- Entrée limite post-only au meilleur prix (`ENTRY_POST_ONLY`, `ENTRY_TIMEOUT_SEC=20`, jamais chassée) ; TP en limite, SL toujours au marché (`TP_LIMIT`).
### Modifié
- Le 1m est retiré du trading (`TRADE_TIMEFRAMES=5m,15m,1h,4h,1d`), il reste en contexte.

## [v4.0-stable] — 2026-10-22
### Ajouté
- `tpsl_place`, `tpsl_history`, `repair_tpsl`, `replace_tpsl` : TP/SL **par trade** sur positions fusionnées ; annulation de ses propres TP/SL avant re-pose pour éviter que Bitunix n'annule ceux des autres trades.
- Fermeture de la seule part d'un trade si ses TP/SL ne sont plus re-posables.
- Version de référence restaurable via `restore.sh`.
- Dossier `kronos/` : observation du modèle Kronos-mini (aucun trading).

## [v4.0.4] — 2026-10-22
### Corrigé
- Webhook : `trade_open`, `trade_close` et `kill_switch` ne sont jamais plafonnés ; 1 essai + 2 retries sur erreur réseau / HTTP 5xx.

## [v4.0.3] — 2026-10-22
### Ajouté
- Suivi des TP/SL en attente (`tpsl_pending`, `tpsl_cancel`, `own_tpsl`) : un trade dont les TP/SL ont disparu sur une position fusionnée est reconnu comme exécuté.

## [v4.0.2] — 2026-10-21
### Corrigé
- `close_qty` / `close_live` : fermeture partielle d'une position fusionnée (hedge) ; prix de sortie calculé par delta et non par moyenne globale.

## [v4.0.1] / [v4.0-live] — 2026-10-21
### Corrigé
- Marge de sécurité de 5 % sur la taille pour les frais d'ouverture (rejets d'ordres en live).

## [v4.0] / [v4.0-paper] — 2026-10-21
### Ajouté
- Premier moteur : BTCUSDT x20 isolé, indicateurs EMA20/50/200, RSI, ATR, VWAP, volume, funding ; setups `breakout`, `pullback_ema20`, `rsi_reversal` ; journal SQLite ; webhook vers l'équipe d'agents ; kill switch. Paper par défaut (`DRY_RUN=1`).
