# Changelog — Trading v4

Format inspiré de [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/). Dates en UTC+8.

## [v4.2.3] — 2026-10-25
### Corrigé (incident du 24/10 : `ERROR pose tp #67: 'list' object has no attribute 'get'`, 33 TP orphelins 0,0012 BTC, boucle `sl_reposed`/`tpsl_covered`, #67 et #68 fermés au marché « SL absent »)
- Réponse de `tpsl/place_order` lue qu'elle soit un **dict ou une liste** (`_resp_oid` : `{"orderId"}`, `[{"orderId"}]`, `["id"]`, `{"data": …}`) ; l'orderId est enregistré dans `sl_order_id`/`tp_order_id`.
- **Aucun doublon possible** : avant chaque pose, un ordre existant du même type, même position, même prix et non revendiqué est **adopté** au lieu d'en poser un autre ; après la pose (y compris en cas d'erreur réseau/réponse ambiguë), relecture des ordres TP/SL en attente (`limit=100`) et adoption de l'ordre réellement créé. Seul un refus explicite Bitunix (`BitunixError`) est traité comme « aucun ordre créé ».
- **Nettoyage automatique des orphelins** (`cleanup_orphans`, au plus 1×/min) : les ordres TP/SL d'une position n'appartenant à aucun `sl_order_id`/`tp_order_id` d'un trade ouvert sont annulés (événement `tpsl_orphan_cancelled`). Garde-fous : ordre vu orphelin à 2 passages successifs, position ignorée si un trade a < 120 s, une partielle en cours (`partial_guard`) ou une re-pose en attente (`need_tpsl`), et un ordre au prix d'un trade sans id pour ce type est adopté, jamais annulé.
- **Fin de la boucle de re-pose SL** : `fix_missing_sl` relit les ordres et ne fait rien si le SL propre du trade est déjà en place ; `sl_reposed` n'est journalisé que si un nouveau SL propre a réellement été posé.
- Aucun changement de stratégie, de taille ni de risque.
### Ajouté
- `tests/sim_hedge_v423.py` : réponses en liste / liste de scalaires / pose ambiguë (ordre créé puis exception), partielle puis re-pose du TP (une seule pose, 4 ordres au total), 33 TP + 1 SL orphelins annulés, garde-fous, absence de boucle SL. Reproduit l'erreur exacte sur v4.2.2.

## [v4.2.2] — 2026-10-24
### Corrigé (incident du 23/10 22:49–22:51 UTC : TP exécutés deux fois, 30038, SYSTEM_CANCELED, 30004, #56 fermé au marché)
- Compte Bitunix en mode **HEDGE** : tous les trades d'un même sens partagent un seul `positionId`. Chaque trade (6 max) possède désormais **ses propres ordres** SL et TP, identifiés par **orderId** stocké en base (`trades.sl_order_id`, `trades.tp_order_id`, migration douce). Plus aucun rapprochement par prix (`own_tpsl`, modification, annulation, détection d'exécution via l'historique par id). Les ordres posés avec l'entrée (sans id renvoyé) et les trades ouverts avant v4.2.2 sont **adoptés une seule fois** (même position, même prix, id non revendiqué par un autre trade).
- Chaque SL/TP est dimensionné à la **qty restante du trade** (après partielle) : ex. A 0,20 + B 0,10 = 0,30 short ; le SL de A ferme 0,20 à son niveau, celui de B 0,10 au sien.
- Avant toute (re)pose : relecture position + ordres TP/SL en attente (`limit=100`, au lieu de 10 par défaut) ; qty plafonnée à la **qty libre** = taille de position − ordres du même type des autres trades ; un ordre encore vivant n'est **jamais doublé** ; un TP limite déclenché encore en carnet compte comme vivant ; l'ancien TP doit être **confirmé annulé** avant la pose d'un nouveau.
- `30004` (position inexistante) ou taille 0 = trade clos : réconciliation depuis l'historique (SL/TP propre exécuté sinon prix courant), pas d'alerte `no_sl`. `30038` : si les ordres existants couvrent déjà la position, pas d'alerte ni de fermeture au marché.
- Ordres `SYSTEM_CANCELED` (après la partielle ou la sortie d'un autre trade) : re-pose des ordres du trade concerné à sa qty, sans doublon. À la clôture d'un trade, ses ordres restants (ex. TP après SL) sont annulés.
### Ajouté
- `tests/sim_hedge_v422.py` : simulation hors-ligne de l'incident (bourse factice hedge avec 30038/30004/SYSTEM_CANCELED).

## [v4.2.1] — 2026-10-23
### Corrigé (sécurité, demandé par Risk)
- Après la partielle à 1R, la position n'est **jamais sans protection** : le SL (break-even + `BE_FEE_PCT`) et le TP de la qty restante sont appliqués **d'abord**, par modification en place (`/api/v1/futures/tpsl/modify_order`, même orderId) ; si la modification échoue, le nouvel ordre est posé **puis** l'ancien annulé ; si la pose échoue, l'ancien ordre reste actif.
- Même règle pour la remise à la qty remplie après une entrée partielle (`verify_entry_tpsl`) ; les réparations des autres trades de la position fusionnée ne font que poser (aucune annulation préalable).
- Repli conservé : après 3 échecs de pose du SL BE, la qty restante est fermée au marché (alerte urgente `no_sl`).

## [v4.2] — 2026-10-23
### Ajouté
- **Sorties** : TP à 1,5R (`RR=1.5`) ; clôture partielle de 50 % à 1R (`PARTIAL_TP`, `PARTIAL_R`, `PARTIAL_FRAC`) par ordre limite posé à l'ouverture (`PARTIAL_LIMIT`, repli au marché) ; ensuite SL au break-even + frais (`BE_AFTER_PARTIAL`, `BE_FEE_PCT=0.10`). Positions fusionnées (hedge) : partielle par qty sur le `positionId`, puis remplacement des TP/SL du trade pour la qty restante et réparation de ceux des autres trades.
- **Filtre ATR %** par TF (`ATR_FILTER`, `ATR_MIN_PCT=5m:0.090,15m:0.165,1h:0.37,4h:0.90,1d:2.6`), calibré sur l'historique récent (22e centile des bougies).
- **Breakout 5m strict** (`BREAKOUT5M_STRICT`, `BREAKOUT5M_VOL_MULT=2.0`) : volume ≥ 2× la moyenne 20 + clôture au-delà du niveau dans le sens de la bougie.
- **VWAP/OBV** (`VWAP_OBV=boost`) : +0,03 de confiance si VWAP de session et pente OBV confirment, −0,03 si les deux contredisent (le filtre dur retirait 36 % des signaux au backtest, > 40 % en 15m/4h, et dégradait le 5m).
- **Kronos 1h** (`KRONOS_FILTER`) : lecture seule de `predictions.db`, actif seulement si précision 1h ≥ 55 % sur ≥ 50 prédictions vérifiées ; toute erreur est ignorée.
- **Auto-kill setup×TF** (`AUTO_KILL`) : 3 jours, n ≥ 8, PnL < 0 et WR < 40 % → paper (table `shadow_trades`), réactivation si le paper est positif sur 3 jours ; événement `autokill` dans le récap groupé.
- Les rejets ADX / ATR / volume / Kronos sont journalisés dans `signals.skip_reason` (`filtre: …`).
- `MAX_PER_TF` (2 en production) : positions simultanées par timeframe.
### Modifié
- `RISK_PCT` 33 → 12 (le plafond `MAX_MARGIN_PCT=25` reste).
- `MAX_FEE_SHARE` 0,30 → 0,20 ; frais estimés maker à l'entrée post-only (`MAKER_FEE_RATE=0.0002`) + taker à la sortie (`FEE_MODEL_MAKER`).
- Migration douce de `trades` (colonnes `init_sl`, `partial_*`) ; les trades ouverts avant v4.2 gardent leur gestion (pas de partielle).

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
