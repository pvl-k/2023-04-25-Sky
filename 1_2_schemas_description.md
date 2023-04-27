# Тестовое задание на позицию Data Engineer. Выполнил Павел Коротков, tlg: @pvl_ko

## Схема БД-источника (предположим, что это Postgres)

![Схема БД-источника](DB_schema.png)


# Выбор методологии хранилища
В рамках выполнения данного тестового задания я хотел бы остановить выбор на витрине данных, которые обладают следующими преимуществами:
+ легкий доступ к часто запрашиваемым данным, т.к. витрины данных просты в использовании и специально разработаны для нужд пользователей;
+ витрина данных очень проста в реализации и требует меньше времени на внедрение;
+ стоимость внедрения витрины данных заведомо ниже, чем полноценного хранилища данных;
+ витрины данных очень гибкие, в случае изменения модели данных витрина может быть перестроена существенно быстрее;

Поэтому предлагается построить аналитическое хранилище на базе одной денормализованной таблицы, в которую соберём все данные из 4 таблиц БД-источника. В качестве ещё одного достоинства можно отметить тот факт, что данное решение имеет высокий уровень детализации хранимыых данных, которые при необходимости можно будет агрегировать в запрошенный аналитиками вид.

# Выбор СУБД для хранилища
В качестве базы данных для хранилища выберем колоночно-ориентированную СУБД ClickHouse, т.к. это достаточно быстрая аналитическая база, использование которой доступно как в облаке (например, Yandex Cloud или [ClickHouse Cloud](https://clickhouse.cloud/) на основе AWS), так и On Premise - это добавляет универсальности к такому решению. 
А, учитывая, что в вашем стеке присутствует DataLens (указан в нескольких вакансиях), то использование ClickHouse в таком случае выглядит достаточно логичным для хранения исторических аналитических данных с целью BI и т.п.

# Создание VIEW в БД-источнике (предполагаем, что это PostgreSQL):
Данная вьюшка необходима, чтобы было проще обращаться к нужным денормализованным данным в процессе ETL. Она позволяет заранее предопределить столбцы из нескольких таблиц и не выносить JOIN'ы в код DAG'а.

```CREATE VIEW Lessons_View AS
SELECT 
    l.id lesson_id,
    l.title lesson_title,
    l.description lesson_description,
    l.start_at lesson_start_at,
    l.end_at lesson_end_at,
    l.homework_url lesson_homework_url,
    l.teacher_id lesson_teacher,
    l.online_lesson_join_url lesson_join_url,
    l.online_lesson_recording_url lesson_rec_url,
    m.id module_id,
    m.title module_title,
    m.description module_description,
    m.created_at module_created_at,
    m.updated_at module_updated_at,
    m.order_in_stream module_order_in_stream,
    s.id stream_id,
    s.name stream_name,
    s.description stream_description,
    s.start_at stream_start_at,
    s.end_at stream_end_at,
    s.created_at stream_created_at,
    s.updated_at stream_updated_at,
    c.id course_id,
    c.title course_title,
    c.description course_description,
    c.created_at course_created_at,
    c.updated_at course_updated_at
FROM 
    stream_module_lesson l
LEFT JOIN 
    stream_module m ON m.id = l.stream_module_id
LEFT JOIN 
    stream s ON s.id = m.stream_id
LEFT JOIN 
    course c ON c.id = s.course_id;
```

## NB! Нижеприведенное графическое отображение схемы не учитывает тот факт, что таблица Lessons создается в СУБД ClickHouse, поэтому просьба не обращать внимания на типы данных на схеме, правильные типы данных указаны в SQL-скрипте в отдельной файле - ответе на задание 4 ##

![Схема хранилища](DWH_schema.png)


