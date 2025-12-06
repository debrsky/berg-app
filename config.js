// config.js
export const MigrationConfig = {
  // Путь к MDB-файлу (относительно batch.js)
  mdbPath: "../../Berg/DB/bergauto.mdb",

  // Access хранит даты в локальном часовом поясе. Корректируем:
  mdbTimezoneOffset: 10 * 60 * 60 * 1000,

  // Подключение к PostgreSQL
  pg: {
    host: process.env.PG_LOCAL_HOST,
    user: process.env.PG_LOCAL_USER,
    password: process.env.PG_LOCAL_PASSWORD,
    database: process.env.PG_LOCAL_DB,
    port: process.env.PG_LOCAL_PORT,
  },

  pg_dbName: "bergapp",

  // Схема в PostgreSQL
  schema: "bergauto",

  // Ограничение на количество строк (null = все)
  limit: null,

  // Пересоздавать схему при каждом запуске?
  dropSchema: true,

  // Желаемый размер батча (будет автоматически уменьшен при >60k параметров)
  batchSize: 2000,
};