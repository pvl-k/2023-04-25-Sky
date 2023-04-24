/* 

####################################################################################
# Тестовое задание на позицию Data Engineer. Выполнил Павел Коротков, tlg: @pvl_ko #
####################################################################################

Тип данных UInt выбран из соображений того, что в данных столбцах нам не нужны отрицательные значения.
Размерность выбрана, исходя из предположений по количеству имеющихся курсов, модулей в них, количества преподавателей и сумарного количества уроков на платформе.
В ClickHouse тип данных String заменяет TEXT и аналогичные, не имеет ограничений на длину, поэтому все типы текстовых столбцов приведены к String.
Тип Date32 выбран исходя из предположения, что даты ранее 1970 года не используются.

*/

CREATE TABLE "lessons" (
  "lesson_id" UInt64,
  "lesson_title" Nullable(String),
  "lesson_description" Nullable(String),
  "lesson_start_at" Date32,
  "lesson_end_at" Date32,
  "lesson_homework_url" Nullable(String),
  "lesson_teacher" UInt32,
  "lesson_join_url" Nullable(String),
  "lesson_rec_url" Nullable(String),
  "module_id" UInt32,
  "module_title" Nullable(String),
  "module_description" Nullable(String),
  "module_created_at" Date32,
  "module_updated_at" Date32,
  "module_order_in_stream" UInt8,
  "stream_id" UInt32,
  "stream_name" Nullable(String),
  "stream_description" Nullable(String),
  "stream_start_at" Date32,
  "stream_end_at" Date32,
  "stream_created_at" Date32,
  "stream_updated_at" Date32,
  "course_id" UInt16,
  "course_title" Nullable(String),
  "course_description" Nullable(String),
  "course_created_at" Date32,
  "course_updated_at" Date32
) ENGINE = MergeTree() ORDER BY (lesson_id);
