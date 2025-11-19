#!/usr/bin/env node
import MDBReader from "mdb-reader";
import { Client } from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { SchemaConfig, IndexesConfig, TablesOrder } from "./schema.config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const config = {
  mdbPath: "../../Berg/DB/bergauto.mdb",
  pg: {
    host: "localhost",
    user: "postgres",
    password: "741621",
    database: "bergauto",
    port: 5432,
  },
  schema: "berg",
  limit: null,          // null = все строки, например 10000 для теста
  dropSchema: true,
  batchSize: 2000,      // будет автоматически уменьшен при необходимости
};

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

function getOptimalBatchSize(columnCount, desired = config.batchSize) {
  const MAX_PARAMS = 60000;
  const calculated = Math.floor(MAX_PARAMS / columnCount);
  return Math.max(50, Math.min(desired, calculated));
}

// ───── Основная функция ─────
async function main() {
  console.log(cyan("\nЗапуск миграции Access → PostgreSQL (индексы создаются ПОСЛЕ загрузки)\n"));

  const client = new Client(config.pg);
  await client.connect();
  console.log(green("PostgreSQL подключено"));

  await client.query("SET synchronous_commit = off");
  await client.query("SET client_min_messages = warning");

  const fullPath = path.resolve(__dirname, config.mdbPath);
  if (!fs.existsSync(fullPath)) {
    console.error(red(`Файл MDB не найден: ${fullPath}`));
    process.exit(1);
  }

  const buffer = fs.readFileSync(fullPath);
  const reader = new MDBReader(buffer);
  console.log(green(`MDB загружен: ${reader.getTableNames().length} таблиц найдено\n`));

  // ───── Пересоздание схемы ─────
  if (config.dropSchema) {
    await client.query(`DROP SCHEMA IF EXISTS ${config.schema} CASCADE`);
    await client.query(`CREATE SCHEMA ${config.schema}`);
    await client.query(`GRANT ALL ON SCHEMA ${config.schema} TO postgres`);
    console.log(yellow(`Схема "${config.schema}" пересоздана\n`));
  }

  // ───── 1. Создание таблиц (только PK, без остальных индексов) ─────
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

    const sql = `CREATE TABLE IF NOT EXISTS ${config.schema}."${tableName}" (${lines.join(", ")})`;
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
    const toInsert = config.limit ? Math.min(config.limit, rowCount) : rowCount;
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
      const dataIterator = table.getData({ limit: config.limit });

      for (const row of dataIterator) {
        if (inserted >= toInsert) break;

        const values = columns.map(col => {
          const val = row[col];
          if (val instanceof Date) {
            return new Date(val.getTime() - 11 * 60 * 60 * 1000);
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

          const insertSql = `INSERT INTO ${config.schema}."${tableName}" (${columnList}) VALUES (${placeholders})`;
          await client.query(insertSql, flatValues);
          batch = [];

          if (inserted % (batchSize * 5) === 0 || inserted === toInsert) {
            const elapsed = (Date.now() - start) / 1000;
            const speed = elapsed > 0 ? inserted / elapsed : 0;
            const percent = ((inserted / toInsert) * 100).toFixed(1);
            const eta = speed > 0 ? (toInsert - inserted) / speed : 0;

            process.stdout.write(
              `\r  ${inserted.toLocaleString().padStart(String(toInsert).length, " ")}/${toInsert.toLocaleString()} ` +
              `| ${percent.padStart(5)}% | ${speed.toFixed(0).padStart(5)} стр/с | ETA ${formatTime(eta)}`
            );
          }
        }
      }

      const timeSec = ((Date.now() - start) / 1000).toFixed(1);
      console.log(`\n${green("ГОТОВО")} за ${yellow(timeSec + "с")}`);
      totalRows += inserted;

      const { rows: [{ cnt }] } = await client.query(`SELECT COUNT(*)::int AS cnt FROM ${config.schema}."${tableName}"`);
      console.log(`В БД: ${cnt.toLocaleString()} строк → ${cnt >= inserted ? green("OK") : red("ОШИБКА")}\n`);

    } catch (err) {
      console.log(red(`\nОШИБКА в таблице "${tableName}"`));
      console.error(red(err.message));
      if (err.stack) console.error(gray(err.stack.split("\n").slice(1).join("\n")));
      continue;
    }
  }

  // ───── 3. Создание индексов (ПОСЛЕ загрузки всех данных) ─────
  console.log(cyan("\nВсе данные загружены! Создаём индексы (это займёт время, но один раз)...\n"));

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
    await client.query(`ANALYZE ${config.schema}."${tableName}"`);
  }
  console.log(green("ANALYZE завершён — запросы будут летать!\n"));

  // ───── Финал ─────
  const totalTime = ((Date.now() - totalStart) / 1000).toFixed(1);
  console.log(cyan("════════════════════════════════════════════════"));
  console.log(yellow("МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!"));
  console.log(`Перенесено строк: ${yellow(totalRows.toLocaleString())}`);
  console.log(`Общее время:      ${yellow(totalTime + " сек")}`);
  console.log(cyan("════════════════════════════════\n"));

  await client.end();
}

main().catch(err => {
  console.error(red("\nФАТАЛЬНАЯ ОШИБКА:"));
  console.error(err);
  process.exit(1);
});