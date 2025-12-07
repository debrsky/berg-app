#!/usr/bin/env node
import MDBReader from "mdb-reader";
import { Client } from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import { MigrationConfig } from "./config.js";
import { SchemaConfig, IndexesConfig, TablesOrder } from "./schema.config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ───── Цвета ─────
const green = t => `\x1b[32m${t}\x1b[0m`;
const yellow = t => `\x1b[33m${t}\x1b[0m`;
const cyan = t => `\x1b[36m${t}\x1b[0m`;
const gray = t => `\x1b[90m${t}\x1b[0m`;
const red = t => `\x1b[31m${t}\x1b[0m`;

// ───── Вспомогательные функции ─────
function formatTime(seconds) {
  if (seconds < 60) return `${seconds.toFixed(1)}с`;
  const m = Math.floor(seconds / 60);
  const s = (seconds % 60).toFixed(0).padStart(2, "0");
  return `${m}м ${s}с`;
}

function getOptimalBatchSize(columnCount, desired = MigrationConfig.batchSize) {
  const MAX_PARAMS = 60000;
  const calculated = Math.floor(MAX_PARAMS / columnCount);
  return Math.max(50, Math.min(desired, calculated));
}

// ───── Основная функция ─────
async function main() {
  console.log(cyan("\nЗапуск миграции Access → PostgreSQL (индексы создаются ПОСЛЕ загрузки)\n"));

  // 1. Подключаемся к сервисной базе
  const adminClient = new Client(MigrationConfig.pg);

  await adminClient.connect();

  // 2. Проверяем, существует ли база
  const dbName = MigrationConfig.pg_dbName;
  const checkDbSql = `
        SELECT 1 FROM pg_database WHERE datname = $1
    `;
  const result = await adminClient.query(checkDbSql, [dbName]);

  if (result.rowCount === 0) {
    console.log(yellow(`Database "${dbName}" does not exist. Creating...`));
    await adminClient.query(`CREATE DATABASE ${dbName}`);
    console.log(yellow(`Database "${dbName}" created.`));
  } else {
    console.log(green(`Database "${dbName}" already exists.`));
  }

  await adminClient.end();

  const client = new Client({ ...MigrationConfig.pg, database: dbName });
  await client.connect();
  console.log(green(`PostgreSQL подключено к базе данных "${dbName}"\n`));

  const fullPath = path.resolve(__dirname, MigrationConfig.mdbPath);
  if (!fs.existsSync(fullPath)) {
    console.error(red(`Файл MDB не найден: ${fullPath}`));
    process.exit(1);
  }

  const buffer = fs.readFileSync(fullPath);
  const reader = new MDBReader(buffer);
  console.log(green(`MDB загружен: ${reader.getTableNames().length} таблиц найдено\n`));

  // ───── Пересоздание схемы ─────
  if (MigrationConfig.dropSchema) {
    await client.query(`DROP SCHEMA IF EXISTS ${MigrationConfig.schema} CASCADE`);
    await client.query(`CREATE SCHEMA ${MigrationConfig.schema}`);
    await client.query(`GRANT ALL ON SCHEMA ${MigrationConfig.schema} TO postgres`);
    console.log(yellow(`Схема "${MigrationConfig.schema}" пересоздана\n`));
  }

  // ───── 5. Создание таблицы операций ─────
  console.log(cyan("\nСоздание таблицы операций и заполнение её данными..."));

  // Проверяем наличие hstore
  const res = await client.query(
    `SELECT 1 FROM pg_extension WHERE extname = 'hstore';`
  );
  if (res.rowCount === 0) {
    console.log(yellow('Расширение hstore не найдено. Создаём...'));
    await client.query('CREATE EXTENSION hstore;');
    console.log(green('Расширение hstore успешно создано!\n'));
  } else {
    console.log(green('Расширение hstore уже установлено.\n'));
  }

  await client.query(`DROP SCHEMA IF EXISTS bergapp CASCADE`);
  await client.query(`CREATE SCHEMA bergapp`);
  await client.query(`GRANT ALL ON SCHEMA bergapp TO postgres`);

  console.log(yellow(`Схема "bergapp" пересоздана\n`));

  const scripts = [
    "SQL/operations/get-operations.sql",
    "SQL/operations/calculate-and-save-operations.sql"
  ];

  for (const script of scripts) {
    const filePath = script;

    if (!fs.existsSync(filePath)) {
      console.log(red(`Файл не найден: ${filePath}`));
      continue;
    }

    console.log(yellow(`\n → Выполняем: ${script}`));
    const sql = fs.readFileSync(filePath, "utf8");
    try {
      await client.query(sql);
    } catch (err) {
      console.log(red(`   ОШИБКА в ${script}: ${err.message}`));
      console.error(err);
    }
  }
  console.log(yellow(`Хранимые процедуры пересозданы\n`));


  // ───── 1. Создание таблиц ─────
  for (const tableName of TablesOrder) {
    const cols = SchemaConfig[tableName];
    if (!cols) continue;

    const lines = cols.map(col => {
      let line = `"${col.Name}" ${col.Type}`;
      if (col.PK) line += " PRIMARY KEY";
      if (col.NotNull) line += " NOT NULL";
      if (col.Default) {
        const def = col.Default === "NOW()" || col.Default.includes("INTERVAL")
          ? col.Default
          : `'${col.Default}'`;
        line += ` DEFAULT ${def}`;
      }
      return line;
    });

    const sql = `CREATE TABLE IF NOT EXISTS ${MigrationConfig.schema}."${tableName}" (${lines.join(", ")})`;
    await client.query(sql);
  }
  console.log(green("Все таблицы созданы (без индексов — для максимальной скорости вставки)\n"));

  let totalRows = 0;
  const totalStart = Date.now();

  // ───── 2. Перенос данных ─────
  for (const tableName of TablesOrder) {
    const table = reader.getTable(tableName);
    if (!table) {
      console.log(gray(`Таблица "${tableName}" отсутствует в MDB — пропуск`));
      continue;
    }

    const rowCount = table.rowCount ?? 0;
    const toInsert = MigrationConfig.limit ? Math.min(MigrationConfig.limit, rowCount) : rowCount;
    if (toInsert === 0) continue;

    const columns = SchemaConfig[tableName].map(c => c.Name);
    const columnList = columns.map(c => `"${c}"`).join(", ");
    const batchSize = getOptimalBatchSize(columns.length);

    console.log(cyan(`\n${tableName}`) +
      ` → перенос ${yellow(toInsert.toLocaleString())} строк` +
      gray(` (батч: ${batchSize})`));

    const start = Date.now();
    let inserted = 0;
    let batch = [];

    try {
      const dataIterator = table.getData({ limit: MigrationConfig.limit });

      for (const row of dataIterator) {
        if (inserted >= toInsert) break;

        const values = columns.map(col => {
          const val = row[col];
          if (val instanceof Date) {
            return new Date(val.getTime() - MigrationConfig.mdbTimezoneOffset);
          }
          return val === undefined || val === null ? null : val;
        });

        batch.push(values);
        inserted++;

        if (batch.length === batchSize || inserted === toInsert) {
          const flatValues = batch.flat();
          const placeholders = batch
            .map((_, i) =>
              columns
                .map((__, j) => `$${i * columns.length + j + 1}`)
                .join(", ")
            )
            .join("), (");

          const insertSql = `INSERT INTO ${MigrationConfig.schema}."${tableName}" (${columnList}) VALUES (${placeholders})`;
          await client.query(insertSql, flatValues);
          batch = [];

          if (inserted % (batchSize * 5) === 0 || inserted === toInsert) {
            const elapsed = (Date.now() - start) / 1000;
            const speed = elapsed > 0 ? inserted / elapsed : 0;
            const percent = ((inserted / toInsert) * 100).toFixed(1);
            const eta = speed > 0 ? (toInsert - inserted) / speed : 0;

            process.stdout.write(
              `\r  ${inserted.toLocaleString().padStart(String(toInsert).length, " ")}/${toInsert.toLocaleString()} ` +
              `| ${percent.padStart(5)}% | ${speed.toFixed(0).padStart(5)} стр/с | ETA ${formatTime(eta)}     `
            );
          }
        }
      }

      const timeSec = ((Date.now() - start) / 1000).toFixed(1);
      console.log(`\n${green("ГОТОВО")} за ${yellow(timeSec + "с")}`);
      totalRows += inserted;

      const { rows: [{ cnt }] } = await client.query(`SELECT COUNT(*)::int AS cnt FROM ${MigrationConfig.schema}."${tableName}"`);
      console.log(`В БД: ${cnt.toLocaleString()} строк → ${cnt >= inserted ? green("OK") : red("ОШИБКА")}\n`);

    } catch (err) {
      console.log(red(`\nОШИБКА в таблице "${tableName}"`));
      console.error(red(err.message));
      if (err.stack) console.error(gray(err.stack.split("\n").slice(1).join("\n")));
      continue;
    }
  }

  // ───── 3. Создание индексов ─────
  console.log(cyan("\nВсе данные загружены! Создаём индексы...\n"));

  await client.query(`SET search_path TO ${MigrationConfig.schema}`);

  const indexStart = Date.now();
  let createdIndexes = 0;

  for (const [tableName, indexList] of Object.entries(IndexesConfig)) {
    if (!indexList || indexList.length === 0) continue;

    console.log(yellow(`  ${tableName.padEnd(16)} → ${indexList.length} индекс(ов)`));

    for (const sql of indexList) {
      try {
        await client.query(sql);
        createdIndexes++;
        process.stdout.write(green("✓"));
      } catch (err) {
        process.stdout.write(red("✗"));
        console.warn(yellow(`\n    Ошибка: ${err.message}`));
      }
    }
    console.log("");
  }

  const indexTime = ((Date.now() - indexStart) / 1000).toFixed(1);
  console.log(green(`\nИндексы созданы: ${createdIndexes} за ${yellow(indexTime + "с")}\n`));

  // ───── 4. ANALYZE ─────
  console.log(cyan("Запуск ANALYZE для всех таблиц..."));
  for (const tableName of TablesOrder) {
    // await client.query(`VACUUM FULL ${MigrationConfig.schema}."${tableName}"`);
    await client.query(`ANALYZE ${MigrationConfig.schema}."${tableName}"`);
  }
  console.log(green("ANALYZE завершён\n"));

  // Пересоздаем подключение, без этого процедура не видит hstore
  await client.end();
  const opClient = new Client({ ...MigrationConfig.pg, database: dbName });
  await opClient.connect();

  // Заполняем данными таблицу операций

  console.log(cyan("\nЗаполняем данными таблицу операций..."));
  await opClient.query(`CALL bergapp.calculate_and_save_operations();`);
  const opCnt = await opClient.query(`SELECT COUNT(*) FROM bergapp.operations;`);
  console.log(opCnt.rows[0].count + " записей в bergapp.operations");
  console.log(green("Таблица операций заполнена\n"));

  // Создаем MATERIALIZED VIEW
  {
    console.log(yellow(`Создаем MATERIALIZED VIEW...`));
    const scripts = [
      "SQL/views/payers.sql",
      "SQL/views/sellers.sql"
    ];

    for (const script of scripts) {
      const filePath = script;

      if (!fs.existsSync(filePath)) {
        console.log(red(`Файл не найден: ${filePath}`));
        continue;
      }

      console.log(yellow(`\n → Выполняем: ${script}`));
      const sql = fs.readFileSync(filePath, "utf8");
      try {
        await opClient.query(sql);
      } catch (err) {
        console.log(red(`   ОШИБКА в ${script}: ${err.message}`));
        console.error(err);
      }
    }
    console.log(yellow(`MATERIALIZED VIEW созданы.\n`));
  }

  await opClient.end();

  // ───── Финал ─────

  const totalTime = ((Date.now() - totalStart) / 1000).toFixed(1);
  console.log(cyan("════════════════════════════════════════════════"));
  console.log(yellow("МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!"));
  console.log(`Перенесено строк: ${yellow(totalRows.toLocaleString())}`);
  console.log(`Общее время:      ${yellow(totalTime + " сек")}`);
  console.log(cyan("════════════════════════════════\n"));
}

main().catch(err => {
  console.error(red("\nФАТАЛЬНАЯ ОШИБКА:"));
  console.error(err);
  process.exit(1);
});