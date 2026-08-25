# Финальный cleanup: downscale kc → hc → dc до исходных 3 контроллеров (2026-08-25)

**Результат: PASS — кластер возвращён к исходному состоянию**

- Три последовательных `DELETE …/hosts/controllers?dc={kc,hc,dc}` → все три workflow COMPLETED
  (28cec15d…, c0fe6c92…, 94766fa7…), включая leader migration на каждом шаге.
- Итог: host_state = 3 контроллера (1 на ДЦ), PMS quorum = 3 исходных voter'а
  (10001@dc, 11001@hc, 12001@kc) — ровно baseline.
- Кластер жив: лидер 1.controller…dc (ACC=1), hc/kc followers.
- Заодно подтверждён downscale-флоу локально после vault/truststore-фиксов (3 прогона подряд).

Серия T1–T6 завершена. Все сценарии PASS.
