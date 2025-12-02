// config.js
export const MigrationConfig = {
  // Путь к MDB-файлу (относительно batch.js)
  mdbPath: "../../Berg/DB/bergauto.mdb",

  // Access хранит даты в локальном часовом поясе. Корректируем:
  mdbTimezoneOffset: 10 * 60 * 60 * 1000,

  // Подключение к PostgreSQL
  pg: {
    host: "localhost",
    user: "postgres",
    password: "741621",
    database: "bergauto",
    port: 5432,
  },

  // Схема в PostgreSQL
  schema: "berg",

  // Ограничение на количество строк (null = все)
  limit: null,

  // Пересоздавать схему при каждом запуске?
  dropSchema: true,

  // Желаемый размер батча (будет автоматически уменьшен при >60k параметров)
  batchSize: 2000,
};