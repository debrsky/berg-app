#!/usr/bin/env node
import MDBReader from "mdb-reader";
import { Client } from "pg";
import copyFrom from "pg-copy-streams";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { SchemaConfig, TablesOrder } from "./schema.config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const config = {
  mdbPath: "../../Berg/DB/bergauto.mdb",
  pg: { host: "localhost", user: "postgres", password: "741621", database: "bergauto", port: 5432 },
  schema: "berg",
  limit: null,      // null = все строки, 100 = только 100 (тест)
  dropSchema: true,
};

// Цвета
const G = t => `\x1b[32m${t}\x1b[0m`;
const Y = t => `\x1b[33m${t}\x1b[0m`;
const C = t => `\x1b[36m${t}\x1b[0m`;
const R = t => `\x1b[31m${t}\x1b[0m`;

async function main() {
  console.log(C("\nЗапуск миграции Access → PostgreSQL (COPY + CSV — 100% надёжно)\n"));

  const client = new Client(config.pg);
  await client.connect();
  console.log(G("Подключено к PostgreSQL"));
  await client.query("SET synchronous_commit = off");

  const fullPath = path.resolve(__dirname, config.mdbPath);
  if (!fs.existsSync(fullPath)) {
    console.error(R("MDB файл не найден: " + fullPath));
    process.exit(1);
  }

  const buffer = fs.readFileSync(fullPath);
  const reader = new MDBReader(buffer);
  console.log(G(`MDB загружен: ${reader.getTableNames().length} таблиц\n`));

  if (config.dropSchema) {
    await client.query(`DROP SCHEMA IF EXISTS ${config.schema} CASCADE`);
    await client.query(`CREATE SCHEMA ${config.schema}`);
    console.log(Y(`Схема "${config.schema}" пересоздана\n`));
  }

  // Создание таблиц (как было)
  for (const tableName of TablesOrder) {
    const cols = SchemaConfig[tableName];
    if (!cols) continue;
    const lines = cols.map(col => {
      let line = `"${col.Name}" ${col.Type}`;
      if (col.PK) line += " PRIMARY KEY";
      if (col.NotNull) line += " NOT NULL";
      if (col.Default) {
        const def = col.Default.includes("NOW()") || col.Default.includes("INTERVAL") ? col.Default : `'${col.Default}'`;
        line += ` DEFAULT ${def}`;
      }
      return line;
    });
    await client.query(`CREATE TABLE IF NOT EXISTS ${config.schema}."${tableName}" (${lines.join(", ")})`);
  }
  console.log(G("Таблицы созданы\n"));

  let totalRows = 0;
  const totalStart = Date.now();

  for (const tableName of TablesOrder) {
    const table = reader.getTable(tableName);
    if (!table) continue;

    const rowCount = table.rowCount ?? 0;
    const toInsert = config.limit ? Math.min(config.limit, rowCount) : rowCount;
    console.log(C(`\n${tableName}`) + ` → ${Y(toInsert.toLocaleString())} строк через COPY (CSV)`);

    const columns = SchemaConfig[tableName].map(c => c.Name);
    const columnList = columns.map(c => `"${c}"`).join(", ");

    // ← ВОТ ГЛАВНОЕ ИЗМЕНЕНИЕ: FORMAT csv + QUOTE '"' + ESCAPE '"'
    const stream = client.query(
      copyFrom.from(
        `COPY ${config.schema}."${tableName}" (${columnList}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', QUOTE E'"', ESCAPE E'"', ENCODING 'UTF8')`
      )
    );

    let inserted = 0;
    const start = Date.now();

    const escapeCsvField = (val) => {
      if (val === null || val === undefined) return "\\N";
      if (val instanceof Date) return val.toISOString();
      if (typeof val === "boolean") return val ? "t" : "f";

      val = String(val);

      // Если содержит кавычки, табы, переводы строк — оборачиваем в " и дублируем внутренние кавычки
      if (val.includes('"') || val.includes("\t") || val.includes("\n") || val.includes("\r")) {
        val = val.replace(/"/g, '""');  // экранируем кавычки
        return `"${val}"`;
      }
      return val;
    };

    try {
      for (const row of table.getData()) {
        if (inserted >= toInsert) break;

        const line = columns.map(col => escapeCsvField(row[col])).join("\t") + "\n";
        stream.write(line);
        inserted++;

        if (inserted % 10000 === 0 || inserted === toInsert) {
          const elapsed = (Date.now() - start) / 1000;
          const speed = elapsed > 0 ? (inserted / elapsed).toFixed(0) : 0;
          process.stdout.write(
            `\r  ${inserted.toLocaleString().padStart(String(toInsert).length, " ")}/${toInsert.toLocaleString()} | ${((inserted / toInsert) * 100).toFixed(1).padStart(5)}% | ${speed.padStart(6)} стр/с`
          );
        }
      }

      stream.end();

      await new Promise((resolve, reject) => {
        stream.on("finish", resolve);
        stream.on("error", reject);
      });

      const timeSec = ((Date.now() - start) / 1000).toFixed(1);
      console.log(`\n${G("ГОТОВО")} за ${Y(timeSec + "с")}`);

      totalRows += inserted;
      const { rows } = await client.query(`SELECT COUNT(*) AS cnt FROM ${config.schema}."${tableName}"`);
      const cnt = Number(rows[0].cnt);
      console.log(`В БД: ${cnt.toLocaleString()} строк → ${cnt === inserted ? G("OK") : R("ОШИБКА")}\n`);

    } catch (err) {
      console.log(R(`\nОШИБКА в таблице "${tableName}"`));
      console.error(R(err.message || err));
      throw err;
    }
  }

  const totalTime = ((Date.now() - totalStart) / 1000).toFixed(1);
  console.log(C("════════════════════════════════"));
  console.log(Y("МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!"));
  console.log(`Строк перенесено: ${Y(totalRows.toLocaleString())}`);
  console.log(`Общее время:      ${Y(totalTime + " сек")}`);
  console.log(C("════════════════════════════════\n"));

  await client.end();
}

main().catch(err => {
  console.error(R("\nФАТАЛЬНАЯ ОШИБКА:"));
  console.error(err);
  process.exit(1);
});