#!/usr/bin/env node
import MDBReader from "mdb-reader";
import { Client } from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { SchemaConfig, TablesOrder } from "./schema.config.js";

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
  limit: null,        // null = все строки, 100 = только 100 (тест)
  dropSchema: true,   // false = не пересоздавать схему (для допереноса)
};

// ───── Цвета ─────
const green = t => `\x1b[32m${t}\x1b[0m`;
const yellow = t => `\x1b[33m${t}\x1b[0m`;
const cyan = t => `\x1b[36m${t}\x1b[0m`;
const gray = t => `\x1b[90m${t}\x1b[0m`;
const red = t => `\x1b[31m${t}\x1b[0m`;

// ───── Вспомогательная функция форматирования времени ─────
function formatTime(seconds) {
  if (seconds < 60) return `${seconds.toFixed(1)}с`;
  const m = Math.floor(seconds / 60);
  const s = (seconds % 60).toFixed(0).padStart(2, "0");
  return `${m}м ${s}с`;
}

// ───── Основная функция ─────
async function main() {
  console.log(cyan("\nЗапуск миграции Access → PostgreSQL...\n"));

  const client = new Client(config.pg);
  await client.connect();
  console.log(green("PostgreSQL подключено"));

  const fullPath = path.resolve(__dirname, config.mdbPath);
  if (!fs.existsSync(fullPath)) {
    console.error(red(`Файл не найден: ${fullPath}`));
    process.exit(1);
  }

  const buffer = fs.readFileSync(fullPath);
  const reader = new MDBReader(buffer);
  console.log(green(`MDB загружен: ${reader.getTableNames().length} таблиц найдено\n`));

  if (config.dropSchema) {
    await client.query(`DROP SCHEMA IF EXISTS ${config.schema} CASCADE`);
    await client.query(`CREATE SCHEMA ${config.schema}`);
    await client.query(`GRANT ALL ON SCHEMA ${config.schema} TO postgres`);
    console.log(yellow(`Схема "${config.schema}" пересоздана\n`));
  } else {
    console.log(yellow(`Схема "${config.schema}" оставлена без изменений\n`));
  }

  // Создание таблиц
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
  console.log(green("Все таблицы созданы/обновлены\n"));

  let totalRows = 0;
  const totalStart = Date.now();

  for (const tableName of TablesOrder) {
    const table = reader.getTable(tableName);
    if (!table) {
      console.log(gray(`Таблица "${tableName}" отсутствует в MDB — пропуск`));
      continue;
    }

    const rowCount = table.rowCount ?? 0;
    const toInsert = config.limit ? Math.min(config.limit, rowCount) : rowCount;

    console.log(cyan(`\n${tableName}`) + ` → перенос ${yellow(toInsert.toLocaleString())} из ${rowCount.toLocaleString()} строк`);

    const columns = SchemaConfig[tableName].map(c => c.Name);
    const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");
    const insertSql = `INSERT INTO ${config.schema}."${tableName}" (${columns.map(c => `"${c}"`).join(", ")}) VALUES (${placeholders})`;

    await client.query("BEGIN");
    let inserted = 0;
    const start = Date.now();

    try {
      const dataIterator = table.getData();  // без limit!

      for (const row of dataIterator) {
        if (inserted >= toInsert) break;

        const values = columns.map(col => {
          const val = row[col];
          return val === undefined || val === null ? null : val;
        });

        await client.query(insertSql, values);
        inserted++;

        // ───── Прогресс каждые 500 строк или в конце ─────
        if (inserted % 500 === 0 || inserted === toInsert) {
          const elapsed = (Date.now() - start) / 1000;
          const speed = elapsed > 0 ? inserted / elapsed : 0;
          const percent = ((inserted / toInsert) * 100).toFixed(1);
          const etaSeconds = speed > 0 ? (toInsert - inserted) / speed : 0;

          process.stdout.write(
            `\r  ${inserted.toLocaleString().padStart(toInsert.toString().length, " ")}/${toInsert.toLocaleString()} ` +
            `| ${percent.padStart(5)}% | ${speed.toFixed(0).padStart(4)} стр/с | ETA: ${formatTime(etaSeconds)}     `
          );
        }
      }

      await client.query("COMMIT");
      const timeSec = ((Date.now() - start) / 1000).toFixed(1);
      console.log(`\n${green("ГОТОВО")} за ${yellow(timeSec + "с")}`);
      totalRows += inserted;

      const { rows: [{ cnt }] } = await client.query(`SELECT COUNT(*)::int AS cnt FROM ${config.schema}."${tableName}"`);
      const ok = config.limit ? (cnt >= toInsert) : (cnt === inserted);
      console.log(`Проверка: ${ok ? green("OK") : red("ОШИБКА")} (${cnt.toLocaleString()} строк в PostgreSQL)\n`);

    } catch (err) {
      await client.query("ROLLBACK");
      console.log(red(`\nОШИБКА в таблице "${tableName}"`));
      console.error(red(err.message));
      if (err.stack) console.error(gray(err.stack.split("\n").slice(1).join("\n")));
    }
  }

  const totalTime = ((Date.now() - totalStart) / 1000).toFixed(1);
  console.log(cyan("════════════════════════════════"));
  console.log(yellow("МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!"));
  console.log(`Перенесено строк: ${yellow(totalRows.toLocaleString())}`);
  console.log(`Общее время: ${yellow(totalTime + " сек")}`);
  console.log(cyan("════════════════════════════════\n"));

  await client.end();
}

main().catch(err => {
  console.error(red("\nФАТАЛЬНАЯ ОШИБКА:"));
  console.error(err);
  process.exit(1);
});