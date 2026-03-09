# Итоговое ДЗ — модуль 3 (ETL)

Стек:
- Apache Airflow
- PostgreSQL
- MongoDB
- Python
- Docker Compose

## Что делает проект
1. Поднимает MongoDB, PostgreSQL и Airflow.
2. Генерирует тестовые данные в MongoDB.
3. Реплицирует данные из MongoDB в PostgreSQL с трансформацией и дедупликацией.
4. Строит 2 аналитические витрины в PostgreSQL.

## Источники данных в MongoDB
- user_sessions
- event_logs
- support_tickets

## Репликация в PostgreSQL
Схемы:
- `staging` — можно расширять под промежуточный слой
- `dds` — очищенные и пригодные для аналитики таблицы
- `mart` — аналитические витрины

## Витрины
1. `mart.user_activity_daily`
   - активность пользователей по дням
   - число сессий
   - суммарная и средняя длительность
   - число посещений страниц и действий

2. `mart.support_efficiency_daily`
   - число тикетов по статусам и типам
   - среднее время решения
   - число открытых тикетов

## Запуск
```bash
docker compose up --build airflow-init
docker compose up --build