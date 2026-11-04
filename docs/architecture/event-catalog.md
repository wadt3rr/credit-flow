# Credit Flow Microservices — каталог событий Kafka

**Статус:** Draft v1 · **Версия:** 1.1  
**Брокер:** Apache Kafka · **Контракты:** Protobuf.

## 1. Принципы
- Событие сообщает о свершившемся факте; команда просит выполнить действие.
- События версионируются и проходят проверку совместимости в CI.
- `event_id` сохраняется при повторной публикации одного события из Outbox.
- Kafka key для событий заявки — `application_id`.
- Payload содержит минимально необходимые данные и не включает пароли, токены или полные документы.
- Consumers считают сообщения потенциально повторными.
- Нельзя полагаться на глобальный порядок между топиками/партициями.
- Технический retry/resume не должен менять публичный статус заявки без отдельного валидного бизнес-перехода.

## 2. Логический envelope

| Поле | Назначение |
|---|---|
| `event_id` | Уникальный ID логического события |
| `event_type` | Тип события |
| `event_version` | Версия схемы |
| `occurred_at` | Время события |
| `producer` | Имя producer service |
| `correlation_id` | ID сквозного процесса |
| `causation_id` | Причинное событие/команда, если есть |
| `aggregate_id` | Обычно application ID |
| typed payload | Типизированная Protobuf message |

Для попыток обработки заявок дополнительно передавать `process_version` и, когда применимо, `attempt_id`/`hold_id`. В Protobuf payload должен быть типизированным сообщением/`oneof`, а не произвольной JSON map без обоснования.

## 3. Топики MVP
- `creditflow.applications.v1`
- `creditflow.verifications.v1`
- `creditflow.decisions.v1`
- `creditflow.companies.v1`
- `creditflow.products.v1`

Retry/DLQ topics определяются отдельно. Физическое разбиение уточняется после определения нагрузки, retention, ACL и требований к ошибкам.

## 4. Каталог событий

### Application Service
- `ApplicationSubmitted`: `application_id`, `company_id`, `product_id`, `product_version`, `requested_amount_minor`, `currency`, `requested_term_months`, `process_version`. Consumers: Verification, Notification.
- `ApplicationReadyForDecision`: `application_id`, `company_id`, `product_id`, `product_version`, `process_version`, идентификаторы результатов проверок. Consumer: Decision.
- `ApplicationStatusChanged`: `application_id`, `previous_status`, `new_status`, `status_version`, `reason_code`, `process_version`. Consumer: Notification.
- `ApplicationCancelled`: `application_id`, `process_version`, `cancelled_at`, `reason_code`. Consumers: Verification, Decision, Notification.
- `ApplicationProcessingHeld`: `application_id`, `hold_id`, `hold_type` (`TECHNICAL`/`DATA_ISSUE`/`MANUAL_REVIEW`), `stage` (`VERIFICATION`/`DECISION`), `reason_code`, `retry_count`, `process_version`, `created_at`, `actor_type`. Публикуется при фиксации внутренней приостановки. Consumers: observability/operations; Notification — только если продуктовая политика предусматривает уведомление.
- `ApplicationProcessingResumed`: `application_id`, `hold_id`, `stage`, `attempt_id`, `process_version`, `resumed_at`, `actor_type`, `reason_code`. Публикуется после авторизованного resume и фиксации команды на повтор через Outbox. Consumers: соответствующий процессный orchestrator/operations.

`ApplicationProcessingHeld` и `ApplicationProcessingResumed` описывают внутреннее состояние процесса; они не являются `ApplicationStatusChanged` и не должны отображаться клиенту как новые публичные статусы. `actor_type` — ограниченный enum (`SYSTEM`, `ADMIN`, `USER` при разрешённой операции); не включать произвольные персональные данные.

### Verification Service
- `VerificationStarted`: `application_id`, `verification_id`, `process_version`, `attempt_id`, `check_types`.
- `VerificationCompleted`: `application_id`, `verification_id`, `process_version`, `attempt_id`, `result`, `reason_codes`, `rules_version`. `result`: ограниченный enum `PASSED`/`FAILED_BUSINESS`.
- `VerificationFailedTechnically`: `application_id`, `verification_id`, `process_version`, `attempt_id`, `error_code`, `retryable`. Не включать stack trace и секреты.

### Decision Service
- `DecisionCompleted`: `decision_id`, `application_id`, `process_version`, `attempt_id`, `rule_set_version`, `outcome` (`APPROVED`/`REJECTED`), `reason_codes`, `evaluated_at`.

### Company Service
- `CompanyCreated`, `CompanyUpdated`: `company_id`, `version`, `occurred_at`, при необходимости минимальные безопасные атрибуты для проекций.

### Product Service
- `ProductPublished`: `product_id`, `product_version`, `published_at`, минимальные параметры.
- `ProductRetired`: `product_id`, `product_version`, `retired_at`.

## 5. Команды возобновления и границы ответственности
`ResumeApplication` — команда Application Service, а не Kafka event. Она должна проверять авторизацию, текущее публичное состояние, наличие активного hold, этап, `process_version` и идемпотентный `command_id`. После успешной локальной транзакции Application Service снимает hold, записывает аудит и помещает соответствующее сообщение в Outbox.

Команда на повторную обработку должна маршрутизироваться только к сервису-владельцу этапа. Получатель проверяет `application_id`, `process_version`, `attempt_id` и уникальность активного запуска. Повторная доставка команды не должна запускать дублирующий этап. Если заявка терминальная или версия устарела, команда отклоняется/игнорируется с безопасным операционным логом; статус заявки не меняется.

## 6. Transactional Outbox
1. Бизнес-изменение и запись Outbox фиксируются в одной локальной транзакции.
2. Publisher безопасно захватывает ожидающие записи и публикует в Kafka.
3. После подтверждения брокера запись помечается опубликованной.
4. Если процесс падает после публикации, но до отметки, событие может отправиться повторно; `event_id` остаётся прежним.
5. Метрики включают возраст старейшей записи, число ошибок и количество ожидающих событий.

## 7. Inbox и consumer
1. Валидировать schema/version/required fields.
2. Проверить `event_id` на дубликат.
3. В одной транзакции записать Inbox и выполнить локальное бизнес-изменение.
4. Зафиксировать транзакцию.
5. Только затем подтвердить обработку/продвинуть offset согласно выбранной Kafka client library.
6. При ошибке транзакции сообщение доступно для повтора.
7. Дедупликация имеет согласованную политику хранения; важные бизнес-ограничения защищаются уникальными ключами независимо от Inbox TTL.

## 8. Retries и DLQ
Различать временные инфраструктурные ошибки, постоянные ошибки схемы, бизнес-неуспех и устаревшие события. Временные ошибки повторять ограниченное число раз с exponential backoff и jitter. После исчерпания попыток — DLQ/retry workflow и alert. DLQ не заменяет расследование.

- Автоматические retries предназначены для временных технических ошибок; их лимит и задержки задаются конфигурацией и наблюдаемы.
- Исчерпание retries может создать `ApplicationProcessingHeld`; оно не означает `REJECTED`.
- Ручное возобновление — отдельная авторизованная команда, с аудитом и idempotency key/command ID.
- Retry topics могут нарушить порядок относительно исходной партиции. `process_version` и `attempt_id` защищают от устаревших результатов и повторного выполнения.
- DLQ replay выполняется только как контролируемая операционная процедура; повторное проигрывание не обходит Inbox, идемпотентность и проверку версии процесса.

## 9. Эволюция схем
- Не переиспользовать номера Protobuf fields; удалённые поля резервировать.
- Добавление полей должно быть совместимым по выбранной политике.
- Несовместимые изменения получают новую версию контракта/топика либо период миграции.
- CI проверяет совместимость.
- Consumers обрабатывают неизвестные enum значения и новые optional fields безопасно.
- Для каждого события документируются владелец, потребители и retention.

## 10. Наблюдаемость
Метрики: published/failed events, Outbox age, retries, DLQ size/growth, consumer lag, processing latency, deduplicated events, schema errors. Для recovery дополнительно измерять число активных hold, возраст старейшего hold, число resume-команд, отказы по устаревшей версии и повторные/дедуплицированные команды. Логи содержат event ID/type, aggregate ID, correlation ID, process version, attempt ID и consumer name, но не чувствительный payload.

## 11. Критерии приёмки
- Protobuf-контракты и владельцы событий определены.
- Producer использует Outbox; consumer — Inbox/идемпотентность.
- Повтор Outbox event сохраняет event ID.
- Проверены падения до публикации, после публикации и до подтверждения обработки.
- Нет бесконечных retry loops; есть lag/Outbox/DLQ метрики.
- Повторная доставка resume-команды не запускает второй процесс.
- Устаревшая команда/событие не возобновляет терминальную заявку.
- Совместимость схем проверяется в CI.
