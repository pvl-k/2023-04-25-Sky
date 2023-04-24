/* 
Тип данных UInt выбран из соображений того, что нам в данных столбцах не нужны отрицательные значения.
Размерность выбрана, исходя из предположений по количеству имеющихся курсов, модулей в них, количества преподавателей и сумарного количества уроков на платформе.
В ClickHouse тип данных String заменяет TEXT и аналогичные, не имеет ограничений на длину, поэтому все типы текстовых столбцов приведены к String.
Тип Date32 выбран исходя из предположения, что даты ранее 1970 года хранить нет необходимости.
*/

CREATE TABLE "lessons" (
  "lesson_id" UInt64,
  "lesson_title" String,
  "lesson_description" String,
  "lesson_start_at" Date32,
  "lesson_end_at" Date32,
  "lesson_homework_url" String,
  "lesson_teacher" UInt32,
  "lesson_join_url" String,
  "lesson_rec_url" String,
  "module_id" UInt32,
  "module_title" String,
  "module_description" String,
  "module_created_at" Date32,
  "module_updated_at" Date32,
  "module_order_in_stream" UInt8,
  "stream_id" UInt32,
  "stream_name" String,
  "stream_description" String,
  "stream_start_at" Date32,
  "stream_end_at" Date32,
  "stream_created_at" Date32,
  "stream_updated_at" Date32,
  "course_id" UInt16,
  "course_title" String,
  "course_description" String,
  "course_created_at" Date32,
  "course_updated_at" Date32
) ENGINE = MergeTree() ORDER BY (id);
