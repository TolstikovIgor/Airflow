# Настройка потоков данных. Apache Airflow
![](logo.png)
> #### Факультет Data Engineering
___
### Урок 1. Планирование задач. Введение Apache AirFlow
* Планировщик задач cron
* Почему именно Airflow?
  * Открытый исходный код
  * Веб-интерфейс
  * На основе Python
  * Широкое использование
  * Удобен в применении
  * Легко масштабируем
  * Собственный репозиторий метаданных
  * Интеграция со множеством источников и сервисов
  * Расширяемый REST API
* Основные понятия в Airflow
  * Направленный ациклический граф - DAG (Directed Acyclic Graph)
  * Планировщик (Scheduler)
  * Операторы (Operators)
  * Задачи (Tasks)
  * Сенсор (Sensor)
  * Экзекьюторы (Executors)
* Принципы работы и архитектура
### Урок 2. Установка Airflow. Создание и основные параметры DAG
* Установка Airflow
  * Шаг 1. Окружение и airflow package
  * Шаг 2. БД и airflow.cfg
  * Шаг 3. Запуск webserver и scheduler
* Создание DAG и его параметры
  * Настройка среды
  * Создание и запуск DAG
  * WebUI
  * Summary
### Урок 3. Разработка потоков данных
* Больше возможностей
  * Переменные (Variables)
  * Пулы (Pools)
  * Хуки (Hooks)
  * Подключения (Connections)
  * Плагины (Plugins)
  * Еще больше возможностей
* Особенности execution_date
* Перезапуск DAG'а
  * Catchup
  * Backfill
* SubDAGs и TaskGroups
  * SubDAGs - разделяем особенности выполнения частей DAG'а
  * TaskGroups - визуально группируем таски, не изменяя выполнение
* Области видимости и контекст выполнения task’и
  * Видимость DAG'а
  * Контекст выполнения
* XComs - обмен данными между tasks и DAGs
* Алертинг
* Мониторинг
* Создание усложненного DAG'а
### Урок 4. Airflow в production. Примеры реальных задач
* Airflow + Kubernetes
  * KubernetesExecutor
  * KubernetesPodOperator
* Пример рабочей задачи в связке Airflow с Celery в k8s + Spark + GitLab
* Новое в Airflow 2.0
  * Новый scheduler с Low-Latency и High-Availability (HA)
  * Полноценный REST API
  * Улучшение Sensors
  * TaskFlow API для задания DAG
  * Task Groups
  * Независимые providers
  * Упрощены Kubernetes Executor и KubernetesPodOperator
  * Улучшения в UI/UX
  * DAG Serialization
* Cложности и ограничения Airflow
* Альтернативы Airflow
  * Luigi
  * Argo, KubeFlow, MLFlow

[сертификат](https://gb.ru/go/JySs1R)
